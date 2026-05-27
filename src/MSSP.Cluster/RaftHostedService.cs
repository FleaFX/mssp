using Microsoft.Extensions.Hosting;
using MSSP.Embedded;
using MSSP.Storage;
using MSSP.Raft;

namespace MSSP.Cluster;

/// <summary>
/// <see cref="IHostedService"/> that owns the lifetime of all Raft cluster resources:
/// <see cref="SegmentedRaftLog"/>, <see cref="RaftLog"/>, <see cref="RaftLogStateMachine"/>,
/// <see cref="LsmStore{TKey}"/>, <see cref="RaftGrpcTransport"/>, and <see cref="RaftNode"/>.
/// </summary>
sealed class RaftHostedService(MsspOptions msspOptions, MsspClusterOptions clusterOptions) : IHostedService, IDisposable {
    SegmentedRaftLog? _raftLog;
    RaftLog? _log;
    RaftLogStateMachine? _stateMachine;
    LsmStore<EventKey>? _lsmStore;
    EmbeddedMsspClient? _local;
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
    /// Gets the local <see cref="EmbeddedMsspClient"/> for this cluster node.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if accessed before <see cref="StartAsync"/> completes.</exception>
    public EmbeddedMsspClient Local =>
        _local ?? throw new InvalidOperationException("Local client is not available before the host has started.");

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

        _raftLog = await SegmentedRaftLog.OpenAsync(dataDir, clusterOptions.RaftLogSegmentSizeBytes, cancellationToken);
        _stateMachine = new RaftLogStateMachine();
        _stateStorage = new FileRaftStateStorage(dataDir);
        // RPC timeout: 5× the heartbeat interval. Long enough to absorb transient latency
        // spikes; short enough that a hanging call is detected well within the election timeout.
        _transport = new RaftGrpcTransport(clusterOptions.Peers,
            TimeSpan.FromMilliseconds(clusterOptions.HeartbeatIntervalMs * 5));

        var config = new RaftNodeConfig(
            clusterOptions.NodeId,
            clusterOptions.Peers.Select(p => p.NodeId).Where(id => id != clusterOptions.NodeId).ToArray(),
            clusterOptions.ElectionTimeoutMinMs,
            clusterOptions.ElectionTimeoutMaxMs,
            clusterOptions.HeartbeatIntervalMs);

        _node = new RaftNode(config, _raftLog, _transport, _stateMachine, _stateStorage);
        _log = new RaftLog(_node, _stateMachine);

        var capturedStateMachine = _stateMachine;
        var capturedRaftLog = _raftLog;

        _lsmStore = await LsmStore<EventKey>.OpenAsync(
            options: new LsmStoreOptions<EventKey>(dataDir, msspOptions.MemTableCapacityBytes, OnFlushed),
            walRecords: AsyncEnumerable.Empty<ReadOnlyMemory<byte>>(),
            cancellationToken: cancellationToken);
        var subscriptionLog = SubscriptionLog.Open(
            dataDir,
            msspOptions.SubscriptionLogFormat,
            msspOptions.SubscriptionLogSegmentSizeBytes);
        var pipeline = new SubscriptionPipeline(_lsmStore, subscriptionLog);
        var logDrivenStore = LogDrivenStore<EventKey>.Create(_log, pipeline, msspOptions.MemTableCapacityBytes);
        _local = new EmbeddedMsspClient(
            store: new GlobalPositionDecorator(logDrivenStore, pipeline),
            subscriptions: pipeline);

        _client = new ClusteredMsspClient(_node, _local, clusterOptions.Peers);

        // wire snapshot callbacks: leader serialises SST files; follower reloads them
        var capturedLsmStore = _lsmStore;
        _stateMachine.SnapshotProvider  = ct => ValueTask.FromResult(LsmSnapshot.Serialize(dataDir));
        _stateMachine.SnapshotInstaller = InstallSnapshotAsync;

        // Replay committed Raft log entries that were not yet reflected in the SST files.
        // Entries are applied directly via LogDrivenStore.ReplayAsync (bypassing the channel)
        // so that replay entries can never prematurely dequeue a pending TCS belonging to a
        // concurrent real-write that arrives just as the Raft node starts.
        for (var i = checkpointIndex + 1; i <= _raftLog.LastIndex; i++) {
            var entry = await _raftLog.GetEntryAsync(i, cancellationToken);
            if (entry.Type == RaftLogEntryType.Command)
                await logDrivenStore.ReplayAsync(entry.Payload, cancellationToken);
            _stateMachine.MarkApplied(entry.Index);
        }

        await _node.StartAsync(cancellationToken);
        return;

        async ValueTask OnFlushed(CancellationToken token) {
            var applied = capturedStateMachine.LastAppliedIndex;
            if (applied > 0 && applied > capturedRaftLog.LastIncludedIndex) {
                var term = await capturedRaftLog.GetTermAtAsync(applied, token);
                await capturedRaftLog.CompactToAsync(applied, term, token);
            }
            await RaftLogStateMachine.WriteCheckpointAsync(dataDir, applied, token);
        }

        async ValueTask InstallSnapshotAsync(ulong lastIncludedIndex, ulong lastIncludedTerm, ReadOnlyMemory<byte> data, CancellationToken token) {
            var stagingDir = Path.Combine(dataDir, "snapshot-staging");
            try {
                LsmSnapshot.Deserialize(data, stagingDir);
                await capturedLsmStore.ReloadAsync(stagingDir, token);
            } finally {
                if (Directory.Exists(stagingDir))
                    Directory.Delete(stagingDir, recursive: true);
            }
            await RaftLogStateMachine.WriteCheckpointAsync(dataDir, lastIncludedIndex, token);
        }
    }

    /// <inheritdoc/>
    public async Task StopAsync(CancellationToken cancellationToken) {
        if (_node is not null) {
            // Pass the host's shutdown token so the drain wait respects the shutdown deadline.
            // DisposeAsync is called afterwards to release _cts and _snapshotBuffer regardless.
            await _node.StopAsync(cancellationToken);
            await _node.DisposeAsync();
        }
        Dispose();
    }

    /// <inheritdoc/>
    public void Dispose() {
        if (_disposed) return;
        _disposed = true;
        _client?.Dispose();
        _local?.Dispose();
        // _node is IAsyncDisposable; disposed in StopAsync via DisposeAsync()
        _transport?.Dispose();
        _raftLog?.Dispose();
    }
}
