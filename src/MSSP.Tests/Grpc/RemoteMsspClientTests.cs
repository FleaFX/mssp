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

public class RemoteMsspClientTests : IAsyncLifetime {
    readonly string _dataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    EmbeddedMsspClient? _embedded;
    WebApplication? _app;
    GrpcChannel? _channel;
    RemoteMsspClient _client = null!;

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

    public class ReadAsync : RemoteMsspClientTests {
        [Fact]
        public async Task ReadBackwards_ReturnsEventsInReverseRevisionOrder() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second"),
                Event("Baz", "third")
            ]);

            var events = await _client.ReadAsync("stream-a", direction: MSSP.ReadDirection.Backwards).ToListAsync();

            events.Should().HaveCount(3);
            events[0].EventType.Should().Be("Baz");
            events[1].EventType.Should().Be("Bar");
            events[2].EventType.Should().Be("Foo");
        }

        [Fact]
        public async Task ReadBackwards_FromRevision_StartsFromSpecifiedRevision() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second"),
                Event("Baz", "third")
            ]);

            var events = await _client.ReadAsync("stream-a", 1UL, MSSP.ReadDirection.Backwards).ToListAsync();

            events.Should().HaveCount(2);
            events[0].EventType.Should().Be("Bar");
            events[1].EventType.Should().Be("Foo");
        }

        [Fact]
        public async Task MaxCount_LimitsNumberOfEvents() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second"),
                Event("Baz", "third")
            ]);

            var events = await _client.ReadAsync("stream-a", maxCount: 2).ToListAsync();

            events.Should().HaveCount(2);
            events[0].EventType.Should().Be("Foo");
            events[1].EventType.Should().Be("Bar");
        }

        [Fact]
        public async Task MaxCount_WithBackwards_LimitsNumberOfEvents() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second"),
                Event("Baz", "third")
            ]);

            var events = await _client.ReadAsync("stream-a", direction: MSSP.ReadDirection.Backwards, maxCount: 2).ToListAsync();

            events.Should().HaveCount(2);
            events[0].EventType.Should().Be("Baz");
            events[1].EventType.Should().Be("Bar");
        }

        [Fact]
        public async Task MaxCount_Zero_ReturnsNoEvents() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second")
            ]);

            var events = await _client.ReadAsync("stream-a", maxCount: 0).ToListAsync();

            events.Should().BeEmpty();
        }

        [Fact]
        public async Task ReadForwards_Explicitly_ReturnsEventsInRevisionOrder() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second")
            ]);

            var events = await _client.ReadAsync("stream-a", direction: MSSP.ReadDirection.Forwards).ToListAsync();

            events.Should().HaveCount(2);
            events[0].EventType.Should().Be("Foo");
            events[1].EventType.Should().Be("Bar");
        }

        [Fact]
        public async Task MaxCount_WithFromRevision_LimitsFromStartRevision() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second"),
                Event("Baz", "third"),
                Event("Qux", "fourth")
            ]);

            var events = await _client.ReadAsync("stream-a", from: 1UL, maxCount: 2).ToListAsync();

            events.Should().HaveCount(2);
            events[0].EventType.Should().Be("Bar");
            events[1].EventType.Should().Be("Baz");
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
