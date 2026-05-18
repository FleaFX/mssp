using FluentAssertions;
using MSSP.Cluster;
using MSSP.Embedded;
using MSSP.Raft;

namespace MSSP.Cluster;

public class ClusteredMsspClientTests : IAsyncLifetime {
    InMemoryCluster _cluster = null!;

    public async Task InitializeAsync() => _cluster = await InMemoryCluster.CreateAsync();

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    static EventData Event(string type, string payload) =>
        new(type, System.Text.Encoding.UTF8.GetBytes(payload));

    [Fact]
    public async Task AppendAndRead_RoundTrip() {
        var leader = await _cluster.WaitForLeaderAsync();
        await leader.Client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "bar")]);

        var events = await leader.Client.ReadAsync("stream-a").ToListAsync();

        events.Should().HaveCount(1);
        events[0].EventType.Should().Be("Foo");
        System.Text.Encoding.UTF8.GetString(events[0].Data.Span).Should().Be("bar");
    }

    [Fact]
    public async Task Follower_ThrowsNotLeaderException() {
        var leader = await _cluster.WaitForLeaderAsync();
        var follower = _cluster.Nodes.First(h => h.Node != leader.Node);

        var act = async () => await follower.Client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "bar")]);

        await act.Should().ThrowAsync<NotLeaderException>()
            .Where(ex => ex.LeaderHint != null);
    }

    [Fact]
    public async Task OccConflict_ThrowsOptimisticConcurrencyException() {
        var leader = await _cluster.WaitForLeaderAsync();
        await leader.Client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "first")]);

        var act = async () => await leader.Client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "second")]);

        await act.Should().ThrowAsync<OptimisticConcurrencyException>();
    }

    [Fact]
    public async Task MultiEventAppend_CorrectRevisions() {
        var leader = await _cluster.WaitForLeaderAsync();
        await leader.Client.AppendAsync("stream-a", StreamRevision.NoStream, [
            Event("A", "1"),
            Event("B", "2"),
            Event("C", "3")
        ]);

        var events = await leader.Client.ReadAsync("stream-a").ToListAsync();

        events.Should().HaveCount(3);
        events[0].Revision.Should().Be(0);
        events[1].Revision.Should().Be(1);
        events[2].Revision.Should().Be(2);
        events[0].EventType.Should().Be("A");
        events[1].EventType.Should().Be("B");
        events[2].EventType.Should().Be("C");
    }

    [Fact]
    public async Task LeaderFailover_NewLeaderAcceptsWrites() {
        var leader = await _cluster.WaitForLeaderAsync();
        await leader.Client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Before", "failover")]);
        await leader.Node.StopAsync();

        var remaining = _cluster.Nodes.Where(h => h.Node != leader.Node).ToArray();
        InMemoryCluster.NodeHandle? newLeader = null;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (DateTime.UtcNow < deadline) {
            newLeader = remaining.FirstOrDefault(h => h.Node.IsLeader);
            if (newLeader is not null) break;
            await Task.Delay(50);
        }
        newLeader.Should().NotBeNull("a new leader should be elected after failover");

        var act = async () => await newLeader!.Client.AppendAsync("stream-b", StreamRevision.NoStream, [Event("After", "failover")]);
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task Recovery_EventsSurviveRestart() {
        var dataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(dataDir);

        // First run: use FileRaftLog so entries persist to disk
        {
            var log = await FileRaftLog.OpenAsync(dataDir);
            var stateStorage = new InMemoryRaftStateStorage();
            var transport = new InMemoryRaftTransport();
            var stateMachine = await MsspStateMachine.OpenAsync(dataDir, 1024 * 1024, 0);
            var config = new RaftNodeConfig("n1", [], 50, 100, 20);
            var node = new RaftNode(config, log, transport, stateMachine, stateStorage);
            transport.Register(node);
            await node.StartAsync();
            try {
                var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(3);
                while (DateTime.UtcNow < deadline && !node.IsLeader)
                    await Task.Delay(50);

                node.IsLeader.Should().BeTrue();
                var client = new ClusteredMsspClientAdapter(node, stateMachine);
                await client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")]);
            } finally {
                await node.StopAsync();
                log.Dispose();
                stateMachine.Dispose();
            }
        }

        // Second run: reopen FileRaftLog, replay entries from checkpoint+1
        {
            var checkpointIndex = await MsspStateMachine.ReadCheckpointIndexAsync(dataDir);
            var log2 = await FileRaftLog.OpenAsync(dataDir);
            var stateMachine2 = await MsspStateMachine.OpenAsync(dataDir, 1024 * 1024, checkpointIndex);
            try {
                for (var i = checkpointIndex + 1; i <= log2.LastIndex; i++) {
                    var entry = await log2.GetEntryAsync(i);
                    await stateMachine2.ApplyAsync(entry);
                }

                var scan = stateMachine2.ScanSnapshotFrom(new EventKey("stream-a", 0));
                var events = scan.Where(kvp => kvp.Key.StreamId == "stream-a").ToList();
                events.Should().HaveCount(1);
            } finally {
                log2.Dispose();
                stateMachine2.Dispose();
                if (Directory.Exists(dataDir)) Directory.Delete(dataDir, recursive: true);
            }
        }
    }
}
