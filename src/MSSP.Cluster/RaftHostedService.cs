using Microsoft.Extensions.Hosting;
using MSSP.Embedded;
using MSSP.Raft;

namespace MSSP.Cluster;

/// <summary>
/// <see cref="IHostedService"/> that owns the lifetime of all Raft cluster resources:
/// <see cref="FileRaftLog"/>, <see cref="MsspStateMachine"/>, <see cref="RaftGrpcTransport"/>,
/// and <see cref="RaftNode"/>.
/// </summary>
/// <remarks>
/// On startup the service reads the SST checkpoint index, opens the log and state machine,
/// replays any log entries that postdate the checkpoint, then starts the Raft node.
/// </remarks>
sealed class RaftHostedService(MsspOptions msspOptions, MsspClusterOptions clusterOptions) : IHostedService, IDisposable {
    FileRaftLog? _log;
    MsspStateMachine? _stateMachine;
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
    /// Gets the <see cref="MsspStateMachine"/> that holds the current event-store state.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if accessed before <see cref="StartAsync"/> completes.</exception>
    public MsspStateMachine StateMachine =>
        _stateMachine ?? throw new InvalidOperationException("State machine is not available before the host has started.");

    /// <inheritdoc/>
    public async Task StartAsync(CancellationToken cancellationToken) {
        var dataDir = msspOptions.DataDirectory;

        var checkpointIndex = await MsspStateMachine.ReadCheckpointIndexAsync(dataDir, cancellationToken);

        _log = await FileRaftLog.OpenAsync(dataDir, cancellationToken);
        _stateMachine = await MsspStateMachine.OpenAsync(dataDir, msspOptions.MemTableCapacityBytes, checkpointIndex, cancellationToken);
        _stateStorage = new FileRaftStateStorage(dataDir);
        _transport = new RaftGrpcTransport(clusterOptions.Peers);

        var config = new RaftNodeConfig(
            clusterOptions.NodeId,
            clusterOptions.Peers.Select(p => p.NodeId).ToArray(),
            clusterOptions.ElectionTimeoutMinMs,
            clusterOptions.ElectionTimeoutMaxMs,
            clusterOptions.HeartbeatIntervalMs);

        _node = new RaftNode(config, _log, _transport, _stateMachine, _stateStorage);

        // replay Raft log entries from checkpoint to current end
        for (var i = checkpointIndex + 1; i <= _log.LastIndex; i++) {
            var entry = await _log.GetEntryAsync(i, cancellationToken);
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
        _node?.Dispose();
        _transport?.Dispose();
        _stateMachine?.Dispose();
        _log?.Dispose();
    }
}
