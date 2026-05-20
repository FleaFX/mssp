using MSSP.Embedded;
using MSSP.Raft;
using MSSP.Storage;

namespace MSSP.Cluster;

sealed class InMemoryCluster : IAsyncDisposable {
    public record NodeHandle(RaftNode Node, ClusteredMsspClient Client, EmbeddedMsspClient Local, string DataDir);

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
            var config = new RaftNodeConfig(nodeId, peers, 1000, 2000, 100);
            var node = new RaftNode(config, log, transport, stateMachine, stateStorage);
            transport.Register(node);

            var raftLog = new RaftLog(node, stateMachine);

            var lsmOptions = new LsmStoreOptions<EventKey>(dataDir, memTableCapacityBytes, _ => ValueTask.CompletedTask);
            var store = await LsmStore<EventKey>.OpenAsync(lsmOptions, AsyncEnumerable.Empty<ReadOnlyMemory<byte>>(), ct);

            var subLog = SubscriptionLog.Open(dataDir, SubscriptionLogFormat.FullPayload, 64 * 1024 * 1024);
            var pipeline = new SubscriptionPipeline(store, subLog);
            var logDriven = LogDrivenStore<EventKey>.Create(raftLog, pipeline, memTableCapacityBytes);
            var local = new EmbeddedMsspClient(store: new GlobalPositionDecorator(logDriven, pipeline), subscriptions: pipeline);
            var client = new ClusteredMsspClient(node, local, []);
            cluster._nodes.Add(new NodeHandle(node, client, local, dataDir));
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
            h.Local.Dispose();
            if (Directory.Exists(h.DataDir))
                Directory.Delete(h.DataDir, recursive: true);
        }
    }
}
