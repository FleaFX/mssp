using MSSP.Cluster;
using MSSP.Embedded;
using MSSP.LsmTree;
using MSSP.Raft;

namespace MSSP.Cluster;

sealed class InMemoryCluster : IAsyncDisposable {
    public record NodeHandle(RaftNode Node, MsspStateMachine StateMachine, ClusteredMsspClientAdapter Client);

    readonly List<NodeHandle> _nodes = [];

    public IReadOnlyList<NodeHandle> Nodes => _nodes;

    public static async Task<InMemoryCluster> CreateAsync(int nodeCount = 3, int memTableCapacityBytes = 1024, CancellationToken ct = default) {
        var cluster = new InMemoryCluster();
        var transport = new InMemoryRaftTransport();

        var nodeIds = Enumerable.Range(1, nodeCount).Select(i => $"n{i}").ToArray();

        for (var i = 0; i < nodeCount; i++) {
            var nodeId = nodeIds[i];
            var peers = nodeIds.Where(id => id != nodeId).ToArray();
            var dataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(dataDir);

            var stateMachine = await MsspStateMachine.OpenAsync(dataDir, memTableCapacityBytes, 0, ct);
            var log = new InMemoryRaftLog();
            var stateStorage = new InMemoryRaftStateStorage();
            var config = new RaftNodeConfig(nodeId, peers, 50, 100, 20);
            var node = new RaftNode(config, log, transport, stateMachine, stateStorage);
            transport.Register(node);

            var client = new ClusteredMsspClientAdapter(node, stateMachine);
            cluster._nodes.Add(new NodeHandle(node, stateMachine, client));
        }

        foreach (var h in cluster._nodes)
            await h.Node.StartAsync(ct);

        return cluster;
    }

    public async Task<NodeHandle> WaitForLeaderAsync(TimeSpan? timeout = null) {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        while (DateTime.UtcNow < deadline) {
            var leader = _nodes.FirstOrDefault(h => h.Node.IsLeader);
            if (leader is not null) return leader;
            await Task.Delay(50);
        }
        throw new TimeoutException("No leader elected within timeout.");
    }

    public async ValueTask DisposeAsync() {
        foreach (var h in _nodes)
            await h.Node.StopAsync();
        foreach (var h in _nodes)
            h.StateMachine.Dispose();
    }
}

// Adapter: wraps RaftNode + MsspStateMachine as IMsspClient without RaftHostedService
sealed class ClusteredMsspClientAdapter(RaftNode node, MsspStateMachine stateMachine) : IMsspClient {
    public async ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken ct = default) {
        if (!node.IsLeader)
            throw new NotLeaderException(node.LeaderHint);
        var payload = AppendCommand.Serialize(streamId.Value, (long)expectedRevision, events);
        var result = await node.ProposeAsync(payload, ct);
        if (result.IsOccConflict)
            throw new OptimisticConcurrencyException(streamId, expectedRevision);
    }

    public async IAsyncEnumerable<RecordedEvent> ReadAsync(StreamId streamId, StreamRevision from = default, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default) {
        if (!node.IsLeader)
            throw new NotLeaderException(node.LeaderHint);
        var startKey = new EventKey(streamId.Value, 0UL);
        var scan = stateMachine.ScanSnapshotFrom(startKey);
        foreach (var (key, value) in scan) {
            if (ct.IsCancellationRequested) yield break;
            if (key.StreamId != streamId.Value) break;
            if (key.Revision < from || value is null) continue;
            yield return ((EventValue)value.Value).ToRecordedEvent(key);
        }
    }
}
