using System.Threading.Channels;

namespace MSSP.Raft;

/// <summary>
/// A single node in a Raft consensus cluster.
/// </summary>
/// <remarks>
/// All state mutations are serialized through an internal mailbox (a <see cref="Channel{T}"/>
/// single-consumer loop), so callers may invoke the public API concurrently without external locking.
/// <para>
/// A node starts as a follower. It transitions to candidate when its election timer fires,
/// and to leader once it wins a majority vote. A newly elected leader appends a no-op entry
/// before accepting client commands (Raft Figure 8).
/// </para>
/// </remarks>
/// <param name="config">Static configuration: node ID, peer IDs, and timeout settings.</param>
/// <param name="log">The durable replicated log.</param>
/// <param name="transport">The network layer used to contact peers.</param>
/// <param name="stateMachine">The application state machine that applies committed entries.</param>
/// <param name="stateStorage">Durable storage for the node's persistent Raft state.</param>
public sealed partial class RaftNode(RaftNodeConfig config, IRaftLog log, IRaftTransport transport, IRaftStateMachine stateMachine, IRaftStateStorage stateStorage) : IDisposable {
    enum RaftRole { Follower, Candidate, Leader }

    readonly Random _rng = new();

    RaftRole _role = RaftRole.Follower;
    ulong _currentTerm;
    string? _votedFor;
    string? _leaderId;
    ulong _commitIndex;
    bool _noOpCommitted;

    /// <summary>
    /// Gets the unique identifier of this node within the cluster.
    /// </summary>
    public string NodeId => config.NodeId;

    /// <summary>
    /// Gets a value indicating whether this node is the current Raft leader and has committed
    /// its initial no-op entry, meaning it is ready to accept client proposals.
    /// </summary>
    public bool IsLeader => _role == RaftRole.Leader && _noOpCommitted;

    /// <summary>
    /// Gets the node ID of the node this node believes to be the current leader,
    /// or <c>null</c> if the leader is unknown (e.g. during an election).
    /// </summary>
    public string? LeaderHint => _leaderId;

    /// <summary>
    /// Loads durable state, starts the mailbox consumer, and begins the election timer.
    /// </summary>
    /// <param name="ct">Token to cancel startup.</param>
    public async Task StartAsync(CancellationToken ct = default) {
        var state = await stateStorage.LoadAsync(ct);
        _currentTerm = state.CurrentTerm;
        _votedFor = state.VotedFor;

        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _mailboxTask = Task.Run(() => RunMailboxAsync(_cts.Token), _cts.Token);

        ResetElectionTimer();
    }

    /// <summary>
    /// Cancels the mailbox consumer, stops the heartbeat and election timers, and awaits
    /// any in-flight mailbox work.
    /// </summary>
    /// <param name="ct">Token to cancel the stop operation (not used for forced cancellation — that is handled internally).</param>
    public async Task StopAsync(CancellationToken ct = default) {
        if (_cts is not null) {
            await _cts.CancelAsync();
            if (_mailboxTask is not null)
                await _mailboxTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
        await (_electionTimer?.DisposeAsync() ?? ValueTask.CompletedTask);
        _heartbeatTimer?.Dispose();
        _heartbeatTask = null;
    }

    /// <inheritdoc/>
    public void Dispose() {
        _cts?.Dispose();
        _electionTimer?.Dispose();
        _heartbeatTimer?.Dispose();
    }

    /// <summary>
    /// Proposes a command for replication. The returned task completes once the entry has been
    /// committed by a quorum and applied to the state machine.
    /// </summary>
    /// <param name="command">The opaque command payload to replicate.</param>
    /// <param name="ct">Token to cancel the proposal; cancellation does not roll back an already-committed entry.</param>
    /// <exception cref="NotLeaderException">Thrown immediately if this node is not the current leader.</exception>
    public Task<RaftApplyResult> ProposeAsync(ReadOnlyMemory<byte> command, CancellationToken ct = default) {
        var tcs = new TaskCompletionSource<RaftApplyResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancelTcsOnStop(tcs);
        Post(async () => {
            if (_role != RaftRole.Leader || !_noOpCommitted) {
                tcs.TrySetException(new NotLeaderException(_leaderId));
                return;
            }
            var entry = new RaftLogEntry(_currentTerm, log.LastIndex + 1, RaftLogEntryType.Command, command);
            await log.AppendAsync([entry]);
            _pending[entry.Index] = tcs;
            await ReplicateToAllPeersAsync();
            await TryAdvanceCommitIndexAsync();
        });
        return tcs.Task;
    }

    /// <summary>
    /// Handles an inbound <see cref="VoteRequest"/> from a candidate peer.
    /// The response is enqueued via the mailbox and returned once processed.
    /// </summary>
    /// <param name="request">The vote request sent by the candidate.</param>
    /// <param name="ct">Token to cancel waiting for the response.</param>
    public ValueTask<VoteResponse> ReceiveVoteRequestAsync(VoteRequest request, CancellationToken ct = default) {
        var tcs = new TaskCompletionSource<VoteResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancelTcsOnStop(tcs);
        Post(async () => tcs.TrySetResult(await HandleVoteRequestAsync(request)));
        return new ValueTask<VoteResponse>(tcs.Task);
    }

    /// <summary>
    /// Handles an inbound <see cref="AppendEntriesRequest"/> from the leader (or a higher-term node).
    /// The response is enqueued via the mailbox and returned once processed.
    /// </summary>
    /// <param name="request">The append-entries request (or heartbeat) sent by the leader.</param>
    /// <param name="ct">Token to cancel waiting for the response.</param>
    public ValueTask<AppendEntriesResponse> ReceiveAppendEntriesAsync(AppendEntriesRequest request, CancellationToken ct = default) {
        var tcs = new TaskCompletionSource<AppendEntriesResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancelTcsOnStop(tcs);
        Post(async () => tcs.TrySetResult(await HandleAppendEntriesAsync(request)));
        return new ValueTask<AppendEntriesResponse>(tcs.Task);
    }

    void CancelTcsOnStop<T>(TaskCompletionSource<T> tcs) =>
        _cts?.Token.Register(() => tcs.TrySetCanceled());
}
