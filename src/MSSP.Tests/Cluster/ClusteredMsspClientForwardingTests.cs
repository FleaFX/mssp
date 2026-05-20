using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MSSP.Embedded;
using MSSP.Raft;
using MSSP.Server;

namespace MSSP.Cluster;

public class ClusteredMsspClientForwardingTests : IAsyncLifetime {
    InMemoryCluster _cluster = null!;
    WebApplication _leaderServer = null!;
    ClusteredMsspClient _followerClient = null!;
    InMemoryCluster.NodeHandle _leader = null!;

    public async Task InitializeAsync() {
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
        var followerDataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(followerDataDir);
        var subLog = SubscriptionLog.Open(followerDataDir, MSSP.Embedded.SubscriptionLogFormat.FullPayload, 64 * 1024 * 1024);
        _followerClient = new ClusteredMsspClient(follower.Node, follower.Store, peers, subLog, 0);
    }

    public async Task DisposeAsync() {
        _followerClient?.Dispose();
        if (_leaderServer is not null) await _leaderServer.DisposeAsync();
        await _cluster.DisposeAsync();
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
    public async Task Follower_ForwardsRead_ToLeader() {
        await _leader.Client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("SeedEvent", "data")]);

        var events = await _followerClient.ReadAsync("stream-a").ToListAsync();

        events.Should().HaveCount(1);
        events[0].EventType.Should().Be("SeedEvent");
    }

    [Fact]
    public async Task Follower_ForwardsOccConflict_ThrowsOptimisticConcurrencyException() {
        await _followerClient.AppendAsync("stream-a", StreamRevision.NoStream, [Event("First", "data")]);

        var act = async () => await _followerClient.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Second", "data")]);

        await act.Should().ThrowAsync<OptimisticConcurrencyException>();
    }
}
