using System.Buffers.Binary;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Hosting;
using MSSP.Engine;
using MSSP.Engine.Storage;
using MSSP.Raft;

namespace MSSP.Cluster;

/// <summary>
/// <see cref="IHostedService"/> that owns the lifetime of all Raft cluster resources:
/// <see cref="SegmentedRaftLog"/>, <see cref="RaftLog"/>, <see cref="RaftLogStateMachine"/>,
/// <see cref="LsmStore{TKey}"/>, <see cref="RaftGrpcTransport"/>, and <see cref="RaftNode"/>.
/// </summary>
/// <remarks>
/// Under a host, the node starts in <see cref="StartingAsync"/>, before any hosted service starts,
/// and stops in <see cref="StoppedAsync"/>, after every hosted service has stopped.
/// </remarks>
sealed class RaftHostedService(
    MsspOptions msspOptions,
    MsspClusterOptions clusterOptions,
    IMeterFactory? meterFactory = null
) : IHostedLifecycleService, IAsyncDisposable {
    SegmentedRaftLog? _raftLog;
    RaftLog? _log;
    RaftLogStateMachine? _stateMachine;
    EmbeddedMsspClient? _local;
    ClusteredMsspClient? _client;
    RaftNode? _node;
    RaftGrpcTransport? _transport;
    FileRaftStateStorage? _stateStorage;
    RaftMetrics? _raftMetrics;
    LsmStoreMetrics? _lsmMetrics;
    CancellationTokenSource? _metricsCts;
    Task? _metricsTask;
    bool _disposed;
    bool _hostManaged;

    /// <summary>
    /// Gets the running <see cref="RaftNode"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown while the node is not running: before the host has started it, or after the host has stopped it.</exception>
    public RaftNode Node =>
        _node ?? throw new InvalidOperationException("Raft node is not available before the host has started.");

    /// <summary>
    /// Gets the local <see cref="EmbeddedMsspClient"/> for this cluster node.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown while the node is not running: before the host has started it, or after the host has stopped it.</exception>
    public EmbeddedMsspClient Local =>
        _local ?? throw new InvalidOperationException("Local client is not available before the host has started.");

    /// <summary>
    /// Gets the <see cref="IMsspClient"/> for this cluster node.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown while the node is not running: before the host has started it, or after the host has stopped it.</exception>
    public ClusteredMsspClient Client =>
        _client ?? throw new InvalidOperationException("Client is not available before the host has started.");

    /// <summary>
    /// Opens the stores and starts the Raft node, unless the node is already running.
    /// </summary>
    async Task OpenAsync(CancellationToken cancellationToken) {
        if (_node is not null) return;
        var dataDir = msspOptions.DataDirectory;
        var checkpointIndex = await RaftLogStateMachine.ReadCheckpointIndexAsync(dataDir, cancellationToken);

        if (meterFactory is not null) {
            _raftMetrics = new RaftMetrics(meterFactory);
            _lsmMetrics = new LsmStoreMetrics(meterFactory, msspOptions.MemTableCapacityBytes);
        }

        _raftLog = await SegmentedRaftLog.OpenAsync(dataDir, clusterOptions.RaftLogSegmentSizeBytes, cancellationToken);
        _stateMachine = new RaftLogStateMachine();
        _stateStorage = new FileRaftStateStorage(dataDir);
        // RPC timeout: 2× the heartbeat interval. Must be well below ElectionTimeoutMinMs so
        // a hanging call times out before any follower can start an election.
        _transport = new RaftGrpcTransport(clusterOptions.Peers,
            TimeSpan.FromMilliseconds(clusterOptions.HeartbeatIntervalMs * 2));

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

        var lsmStore = await LsmStore<EventKey>.OpenAsync(
            options: new LsmStoreOptions<EventKey>(dataDir, msspOptions.MemTableCapacityBytes, OnFlushed, BaseLevelSizeBytes: -1, LevelSizeMultiplier: 10, Metrics: _lsmMetrics),
            walRecords: AsyncEnumerable.Empty<ReadOnlyMemory<byte>>(),
            cancellationToken: cancellationToken);

        var subscriptionLog = SubscriptionLog.Open(
            dataDir,
            msspOptions.SubscriptionLogFormat,
            msspOptions.SubscriptionLogSegmentSizeBytes);
        var currentPosition = subscriptionLog.GetLastPosition().Value;

        // Replay committed Raft log entries not yet reflected in the SST files.
        for (var i = checkpointIndex + 1; i <= _raftLog.LastIndex; i++) {
            var entry = await _raftLog.GetEntryAsync(i, cancellationToken);
            if (entry.Type == RaftLogEntryType.Command)
                currentPosition = await ApplyRaftEntryAsync(lsmStore, subscriptionLog, currentPosition, entry.Payload, cancellationToken);
            _stateMachine.MarkApplied(entry.Index);
        }
        await subscriptionLog.FlushAsync(cancellationToken);

        _local = new EmbeddedMsspClient(_log!, lsmStore, subscriptionLog, dataDir, meterFactory);
        _client = new ClusteredMsspClient(_node!, _local, clusterOptions.Peers);

        // wire snapshot callbacks: leader serialises SST files; follower reloads them via engine
        _stateMachine.SnapshotProvider  = ct => ValueTask.FromResult(LsmSnapshot.Serialize(dataDir));
        _stateMachine.SnapshotInstaller = InstallSnapshotAsync;

        await _node.StartAsync(cancellationToken);

        if (_raftMetrics is not null) {
            _metricsCts = new CancellationTokenSource();
            _metricsTask = Task.Run(() => UpdateRaftMetricsLoopAsync(_metricsCts.Token), cancellationToken);
        }

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
                await _local!.ReloadSnapshotAsync(stagingDir, token);
                await RaftLogStateMachine.WriteCheckpointAsync(dataDir, lastIncludedIndex, token);
            } finally {
                if (Directory.Exists(stagingDir))
                    Directory.Delete(stagingDir, recursive: true);
            }
        }
    }

    static async ValueTask<ulong> ApplyRaftEntryAsync(
        LsmStore<EventKey> store,
        SubscriptionLog subscriptionLog,
        ulong currentPosition,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct) {

        var span = payload.Span;
        if (span.Length < 5) return currentPosition;
        var keyLen = BinaryPrimitives.ReadInt32LittleEndian(span[1..]);
        if (keyLen < 0 || 5 + keyLen > span.Length) return currentPosition;

        EventKey key = payload.Slice(5, keyLen);
        Memory<byte> value = payload[(5 + keyLen)..].ToArray();
        var pos = value.Length >= 8 ? BinaryPrimitives.ReadUInt64LittleEndian(value.Span[^8..]) : 0UL;

        await store.WriteAsync(key, value, ct);
        if (pos > currentPosition) {
            currentPosition = pos;
            await subscriptionLog.AppendAsync(new GlobalPosition(pos), key, value, ct);
        }
        return currentPosition;
    }

    /// <inheritdoc/>
    public Task StartingAsync(CancellationToken cancellationToken) {
        _hostManaged = true;
        return OpenAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public Task StartAsync(CancellationToken cancellationToken) =>
        OpenAsync(cancellationToken);

    /// <inheritdoc/>
    public Task StartedAsync(CancellationToken cancellationToken) =>
        Task.CompletedTask;

    /// <inheritdoc/>
    public Task StoppingAsync(CancellationToken cancellationToken) =>
        Task.CompletedTask;

    /// <inheritdoc/>
    public Task StopAsync(CancellationToken cancellationToken) =>
        // Under a host, StoppedAsync stops the node, after every other hosted service has stopped.
        _hostManaged ? Task.CompletedTask : CloseAsync(cancellationToken);

    /// <inheritdoc/>
    public Task StoppedAsync(CancellationToken cancellationToken) =>
        CloseAsync(cancellationToken);

    /// <summary>
    /// Stops the Raft node and disposes every resource, unless that already happened.
    /// </summary>
    async Task CloseAsync(CancellationToken cancellationToken) {
        if (_disposed) return;
        if (_node is not null)
            await _node.StopAsync(cancellationToken);
        await DisposeAsync();
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() {
        if (_disposed) return;
        _disposed = true;
        _metricsCts?.Cancel();
        if (_metricsTask is not null) await _metricsTask;
        _client?.Dispose();
        if (_local is not null) await _local.DisposeAsync();
        if (_node is not null) await _node.DisposeAsync();
        _transport?.Dispose();
        _raftLog?.Dispose();
        _raftMetrics?.Dispose();
        _lsmMetrics?.Dispose();
        _metricsCts?.Dispose();
    }

    async Task UpdateRaftMetricsLoopAsync(CancellationToken cancellationToken) {
        ulong? lastTerm = null;
        while (!cancellationToken.IsCancellationRequested) {
            try {
                await Task.Delay(100, cancellationToken); // Update every 100ms
                if (_node is not null && _raftMetrics is not null) {
                    var currentTerm = _node.CurrentTerm;
                    if (lastTerm is null || currentTerm > lastTerm) {
                        _raftMetrics.RecordElection();
                    }
                    lastTerm = currentTerm;

                    _raftMetrics.Update(
                        (long)currentTerm,
                        _node.IsLeader,
                        (long)_node.CommitIndex,
                        (long)_stateMachine!.LastAppliedIndex);
                }
            } catch (OperationCanceledException) {
                break;
            } catch {
                // Ignore errors in metrics collection
            }
        }
    }
}
