using Microsoft.Extensions.Hosting;
using MSSP.Embedded;
using MSSP.LsmTree;
using MSSP.Raft;

namespace MSSP.Cluster;

/// <summary>
/// <see cref="IHostedService"/> that owns the lifetime of all Raft cluster resources:
/// <see cref="FileRaftLog"/>, <see cref="RaftLog"/>, <see cref="RaftLogStateMachine"/>,
/// <see cref="LsmStore{TKey}"/>, <see cref="RaftGrpcTransport"/>, and <see cref="RaftNode"/>.
/// </summary>
sealed class RaftHostedService(MsspOptions msspOptions, MsspClusterOptions clusterOptions) : IHostedService, IDisposable {
    FileRaftLog? _raftLog;
    RaftLog? _log;
    RaftLogStateMachine? _stateMachine;
    LsmStore<EventKey>? _store;
    ClusteredMsspClient? _client;
    RaftNode? _node;
    RaftGrpcTransport? _transport;
    FileRaftStateStorage? _stateStorage;
    bool _disposed;

    /// <summary>
    /// Gets the running <see cref="RaftNode"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if accessed before <see cref="StartAsync"/> completes.</exception>
    public RaftNode Node =>
        _node ?? throw new InvalidOperationException("Raft node is not available before the host has started.");

    /// <summary>
    /// Gets the <see cref="IMsspClient"/> for this cluster node.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if accessed before <see cref="StartAsync"/> completes.</exception>
    public ClusteredMsspClient Client =>
        _client ?? throw new InvalidOperationException("Client is not available before the host has started.");

    /// <inheritdoc/>
    public async Task StartAsync(CancellationToken cancellationToken) {
        var dataDir = msspOptions.DataDirectory;
        var checkpointIndex = await RaftLogStateMachine.ReadCheckpointIndexAsync(dataDir, cancellationToken);

        _raftLog = await FileRaftLog.OpenAsync(dataDir, cancellationToken);
        _stateMachine = new RaftLogStateMachine();
        _stateStorage = new FileRaftStateStorage(dataDir);
        _transport = new RaftGrpcTransport(clusterOptions.Peers);

        var config = new RaftNodeConfig(
            clusterOptions.NodeId,
            clusterOptions.Peers.Select(p => p.NodeId).ToArray(),
            clusterOptions.ElectionTimeoutMinMs,
            clusterOptions.ElectionTimeoutMaxMs,
            clusterOptions.HeartbeatIntervalMs);

        _node = new RaftNode(config, _raftLog, _transport, _stateMachine, _stateStorage);
        _log = new RaftLog(_node, _stateMachine);

        RaftLogStateMachine capturedStateMachine = _stateMachine;
        MemTableFlushedDelegate onFlushed = async ct =>
            await RaftLogStateMachine.WriteCheckpointAsync(dataDir, capturedStateMachine.LastAppliedIndex, ct);

        var options = new LsmStoreOptions<EventKey>(
            dataDir,
            msspOptions.MemTableCapacityBytes,
            _log,
            onFlushed);

        _store = await LsmStore<EventKey>.OpenAsync(options, AsyncEnumerable.Empty<ReadOnlyMemory<byte>>(), cancellationToken);

        _client = new ClusteredMsspClient(_node, _store, clusterOptions.Peers);

        // replay Raft log entries from checkpoint to current end
        for (var i = checkpointIndex + 1; i <= _raftLog.LastIndex; i++) {
            var entry = await _raftLog.GetEntryAsync(i, cancellationToken);
            await _stateMachine.ApplyAsync(entry, cancellationToken);
        }

        await _node.StartAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public async Task StopAsync(CancellationToken cancellationToken) {
        if (_node is not null)
            await _node.StopAsync(cancellationToken);
        Dispose();
    }

    /// <inheritdoc/>
    public void Dispose() {
        if (_disposed) return;
        _disposed = true;
        _client?.Dispose();
        _node?.Dispose();
        _transport?.Dispose();
        _store?.Dispose();
        _raftLog?.Dispose();
    }
}
