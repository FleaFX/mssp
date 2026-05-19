using MSSP.LsmTree;
using MSSP.Raft;

namespace MSSP.Cluster;

sealed class InMemoryCluster : IAsyncDisposable {
    public record NodeHandle(RaftNode Node, ClusteredMsspClient Client, LsmStore<EventKey> Store);

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

            var stateMachine = new RaftLogStateMachine();
            var log = new InMemoryRaftLog();
            var stateStorage = new InMemoryRaftStateStorage();
            var config = new RaftNodeConfig(nodeId, peers, 300, 600, 50);
            var node = new RaftNode(config, log, transport, stateMachine, stateStorage);
            transport.Register(node);

            var raftLog = new RaftLog(node, stateMachine);

            var options = new LsmStoreOptions<EventKey>(
                dataDir,
                memTableCapacityBytes,
                raftLog,
                _ => ValueTask.CompletedTask);

            var store = await LsmStore<EventKey>.OpenAsync(options, AsyncEnumerable.Empty<ReadOnlyMemory<byte>>(), ct);

            var client = new ClusteredMsspClient(node, store, []);
            cluster._nodes.Add(new NodeHandle(node, client, store));
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
        foreach (var h in _nodes) {
            h.Client.Dispose();
            h.Store.Dispose();
        }
    }
}
