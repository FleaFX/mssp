using FluentAssertions;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MSSP.Client;
using MSSP.Embedded;
using MSSP.Server;
using MsspGrpcClient = MSSP.Grpc.Mssp.MsspClient;

namespace MSSP.Grpc;

public class MsspGrpcServiceTests : IAsyncLifetime {
    readonly string _dataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    EmbeddedMsspClient? _embedded;
    WebApplication? _app;
    GrpcChannel? _channel;
    IMsspClient _client = null!;
    MsspGrpcClient _rawClient = null!;

    public async ValueTask InitializeAsync() {
        _embedded = await EmbeddedMsspClient.OpenAsync(_dataDir);

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseTestServer();
        builder.Services.AddSingleton<IMsspClient>(_embedded);
        builder.Services.AddGrpc();

        _app = builder.Build();
        _app.MapGrpcService<MsspGrpcService>();
        await _app.StartAsync();

        var testServer = _app.GetTestServer();
        _channel = GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions {
            HttpHandler = new Http2Handler(testServer.CreateHandler())
        });
        _client = new RemoteMsspClient(new MsspGrpcClient(_channel));
        _rawClient = new MsspGrpcClient(_channel);
    }

    public async ValueTask DisposeAsync() {
        _channel?.Dispose();
        if (_app is not null) await _app.DisposeAsync();
        _embedded?.Dispose();
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    static MSSP.EventData Event(string type, string payload) =>
        new(type, System.Text.Encoding.UTF8.GetBytes(payload));

    public class AppendAsync : MsspGrpcServiceTests {
        [Fact]
        public async Task NoStream_OnNewStream_Succeeds() {
            var act = async () => await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")]);

            await act.Should().NotThrowAsync();
        }

        [Fact]
        public async Task NoStream_OnExistingStream_ThrowsOptimisticConcurrencyException() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")]);

            var act = async () => await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")]);

            await act.Should().ThrowAsync<OptimisticConcurrencyException>();
        }

        [Fact]
        public async Task EmptyStreamId_ThrowsRpcException_WithInvalidArgument() {
            var act = async () => await _rawClient.AppendAsync(
                new AppendRequest { StreamId = "", ExpectedRevision = -1 });

            await act.Should().ThrowAsync<RpcException>()
                .Where(ex => ex.StatusCode == StatusCode.InvalidArgument);
        }
    }

    public class ReadAsync : MsspGrpcServiceTests {
        [Fact]
        public async Task EmptyStream_ReturnsNoEvents() {
            var events = await _client.ReadAsync("stream-a").ToListAsync();

            events.Should().BeEmpty();
        }

        [Fact]
        public async Task PreservesStreamIdEventTypePayloadAndRevision() {
            var payload = "hello world"u8.ToArray();
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [new MSSP.EventData("MyEvent", payload)]);

            var events = await _client.ReadAsync("stream-a").ToListAsync();

            events.Should().HaveCount(1);
            events[0].StreamId.Value.Should().Be("stream-a");
            events[0].Revision.Should().Be(0);
            events[0].EventType.Should().Be("MyEvent");
            events[0].Data.ToArray().Should().Equal(payload);
        }

        [Fact]
        public async Task ReturnsEventsInRevisionOrder() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second"),
                Event("Baz", "third")
            ]);

            var events = await _client.ReadAsync("stream-a").ToListAsync();

            events.Should().HaveCount(3);
            events[0].EventType.Should().Be("Foo");
            events[1].EventType.Should().Be("Bar");
            events[2].EventType.Should().Be("Baz");
        }

        [Fact]
        public async Task FromRevision_SkipsEarlierEvents() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second"),
                Event("Baz", "third")
            ]);

            var events = await _client.ReadAsync("stream-a", 1UL).ToListAsync();

            events.Should().HaveCount(2);
            events[0].EventType.Should().Be("Bar");
            events[1].EventType.Should().Be("Baz");
        }

        [Fact]
        public async Task TimestampPreservedOverWire() {
            var before = DateTimeOffset.UtcNow;
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")]);
            var after = DateTimeOffset.UtcNow;

            var events = await _client.ReadAsync("stream-a").ToListAsync();

            events[0].Timestamp.Should().BeCloseTo(before, TimeSpan.FromSeconds(1));
        }

        [Fact]
        public async Task EmptyStreamId_ThrowsRpcException_WithInvalidArgument() {
            var act = async () => {
                using var call = _rawClient.Read(new ReadRequest { StreamId = "" });
                await call.ResponseStream.MoveNext(CancellationToken.None);
            };

            await act.Should().ThrowAsync<RpcException>()
                .Where(ex => ex.StatusCode == StatusCode.InvalidArgument);
        }
    }

    sealed class Http2Handler(HttpMessageHandler inner) : DelegatingHandler(inner) {
        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken) {
            request.Version = new Version(2, 0);
            var response = await base.SendAsync(request, cancellationToken);
            response.Version = new Version(2, 0);
            return response;
        }
    }
}
