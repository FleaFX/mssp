using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MSSP.Engine;
using MSSP.Raft;
using MSSP.Server;
using MSSP.Storage;

namespace MSSP.Cluster;

public class ClusteredMsspClientForwardingTests : IAsyncLifetime {
    InMemoryCluster _cluster = null!;
    WebApplication _leaderServer = null!;
    ClusteredMsspClient _followerClient = null!;
    EmbeddedMsspClient _followerLocal = null!;
    string _followerDataDir = null!;
    InMemoryCluster.NodeHandle _leader = null!;

    public async ValueTask InitializeAsync() {
        _cluster = await InMemoryCluster.CreateAsync(nodeCount: 2);
        _leader = await _cluster.WaitForLeaderAsync();
        var follower = _cluster.Nodes.First(h => h.Node != _leader.Node);

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.Configure<KestrelServerOptions>(options =>
            options.Listen(System.Net.IPAddress.Loopback, 0, o => o.Protocols = HttpProtocols.Http2));
        builder.Services.AddSingleton<IMsspClient>(_leader.Client);
        builder.Services.AddGrpc();
        _leaderServer = builder.Build();
        _leaderServer.MapGrpcService<MsspGrpcService>();
        await _leaderServer.StartAsync();

        var address = _leaderServer.Services
            .GetRequiredService<IServer>()
            .Features.Get<IServerAddressesFeature>()!
            .Addresses.First();

        var peers = new[] { new RaftClusterMember(_leader.Node.NodeId, new Uri(address)) };
        _followerDataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_followerDataDir);
        var followerStateMachine = new RaftLogStateMachine();
        var followerRaftLog = new RaftLog(follower.Node, followerStateMachine);
        var followerLsmOptions = new LsmStoreOptions<EventKey>(_followerDataDir, 1024, _ => ValueTask.CompletedTask, BaseLevelSizeBytes: -1, LevelSizeMultiplier: 10);
        var followerStore = await LsmStore<EventKey>.OpenAsync(followerLsmOptions, AsyncEnumerable.Empty<ReadOnlyMemory<byte>>(), TestContext.Current.CancellationToken);
        var followerSubLog = SubscriptionLog.Open(_followerDataDir, SubscriptionLogFormat.FullPayload, 64 * 1024 * 1024);
        var followerPipeline = new SubscriptionPipeline(followerStore, followerSubLog);
        var followerLogDriven = LogDrivenStore<EventKey>.Create(followerRaftLog, followerPipeline, 1024);
        _followerLocal = new EmbeddedMsspClient(store: new GlobalPositionDecorator(followerLogDriven, followerPipeline), subscriptions: followerPipeline);
        _followerClient = new ClusteredMsspClient(follower.Node, _followerLocal, peers);
    }

    public async ValueTask DisposeAsync() {
        _followerClient?.Dispose();
        _followerLocal?.Dispose();
        if (_leaderServer is not null) await _leaderServer.DisposeAsync();
        await _cluster.DisposeAsync();
        if (_followerDataDir is not null && Directory.Exists(_followerDataDir))
            Directory.Delete(_followerDataDir, recursive: true);
    }

    static EventData Event(string type, string payload) =>
        new(type, System.Text.Encoding.UTF8.GetBytes(payload));

    [Fact]
    public async Task Follower_ForwardsAppend_ToLeader() {
        await _followerClient.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Forwarded", "payload")]);

        var events = await _leader.Client.ReadAsync("stream-a").ToListAsync();

        events.Should().HaveCount(1);
        events[0].EventType.Should().Be("Forwarded");
    }

    [Fact]
    public async Task Follower_ForwardsOccConflict_ThrowsOptimisticConcurrencyException() {
        await _followerClient.AppendAsync("stream-a", StreamRevision.NoStream, [Event("First", "data")]);

        var act = async () => await _followerClient.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Second", "data")]);

        await act.Should().ThrowAsync<OptimisticConcurrencyException>();
    }
}
