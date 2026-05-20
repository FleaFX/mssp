using FluentAssertions;
using MSSP.Embedded;
using MSSP.Storage;
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
    public async Task Follower_ThrowsTimeoutException_WhenForwardingNotConfigured() {
        var leader = await _cluster.WaitForLeaderAsync();
        var follower = _cluster.Nodes.First(h => h.Node != leader.Node);

        var act = async () => await follower.Client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "bar")]);

        await act.Should().ThrowAsync<TimeoutException>();
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

        // First run: write an event using FileRaftLog so entries persist to disk
        {
            var fileRaftLog = await FileRaftLog.OpenAsync(dataDir);
            var stateMachine = new RaftLogStateMachine();
            var stateStorage = new InMemoryRaftStateStorage();
            var transport = new InMemoryRaftTransport();
            var config = new RaftNodeConfig("n1", [], 50, 100, 20);
            var node = new RaftNode(config, fileRaftLog, transport, stateMachine, stateStorage);
            transport.Register(node);
            var raftLog = new RaftLog(node, stateMachine);
            var options = new LsmStoreOptions<EventKey>(dataDir, 1024 * 1024, raftLog, _ => ValueTask.CompletedTask);
            var store = await LsmStore<EventKey>.OpenAsync(options, AsyncEnumerable.Empty<ReadOnlyMemory<byte>>(), default);
            await node.StartAsync();
            var subLog = SubscriptionLog.Open(dataDir, SubscriptionLogFormat.FullPayload, 64 * 1024 * 1024);
            var pipeline = new SubscriptionPipeline(store, subLog);
            var local = new EmbeddedMsspClient(pipeline, pipeline);
            var client = new ClusteredMsspClient(node, local, []);
            try {
                var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(3);
                while (DateTime.UtcNow < deadline && !node.IsLeader)
                    await Task.Delay(50);

                node.IsLeader.Should().BeTrue();
                await client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")]);
            } finally {
                await node.StopAsync();
                client.Dispose();
                local.Dispose();
                fileRaftLog.Dispose();
            }
        }

        // Second run: reopen FileRaftLog and replay entries from checkpoint+1
        {
            var checkpointIndex = await RaftLogStateMachine.ReadCheckpointIndexAsync(dataDir);
            var fileRaftLog2 = await FileRaftLog.OpenAsync(dataDir);
            var stateMachine2 = new RaftLogStateMachine();
            var node2 = new RaftNode(new RaftNodeConfig("n1", [], 50, 100, 20), fileRaftLog2, new InMemoryRaftTransport(), stateMachine2, new InMemoryRaftStateStorage());
            var raftLog2 = new RaftLog(node2, stateMachine2);
            var options2 = new LsmStoreOptions<EventKey>(dataDir, 1024 * 1024, raftLog2, _ => ValueTask.CompletedTask);
            var store2 = await LsmStore<EventKey>.OpenAsync(options2, AsyncEnumerable.Empty<ReadOnlyMemory<byte>>(), default);
            try {
                for (var i = checkpointIndex + 1; i <= fileRaftLog2.LastIndex; i++) {
                    var entry = await fileRaftLog2.GetEntryAsync(i);
                    await stateMachine2.ApplyAsync(entry);
                }

                // wait for apply loop to process the replayed records
                await Task.Delay(100);

                var scan = store2.ScanSnapshotFrom(new EventKey("stream-a", 0));
                var events = scan.Where(kvp => kvp.Key.StreamId == "stream-a").ToList();
                events.Should().HaveCount(1);
            } finally {
                store2.Dispose();
                fileRaftLog2.Dispose();
                if (Directory.Exists(dataDir)) Directory.Delete(dataDir, recursive: true);
            }
        }
    }
}
