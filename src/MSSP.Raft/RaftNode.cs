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
public sealed partial class RaftNode(
    RaftNodeConfig config,
    IRaftLog log,
    IRaftTransport transport,
    IRaftStateMachine stateMachine,
    IRaftStateStorage stateStorage
) : IDisposable {

    readonly RaftNodeConfig _config = config;
    readonly IRaftLog _log = log;
    readonly IRaftTransport _transport = transport;
    readonly IRaftStateStorage _stateStorage = stateStorage;
    readonly Random _rng = new();

    ulong _currentTerm;
    string? _votedFor;
    string? _leaderId;
    ulong _commitIndex;
    RaftRole _role = null!;

    /// <summary>
    /// Gets the unique identifier of this node within the cluster.
    /// </summary>
    public string NodeId => _config.NodeId;

    /// <summary>
    /// Gets a value indicating whether this node is the current Raft leader and has committed
    /// its initial no-op entry, meaning it is ready to accept client proposals.
    /// </summary>
    public bool IsLeader => _role is LeaderRole { NoOpCommitted: true };

    /// <summary>
    /// Gets the node ID of the node this node believes to be the current leader,
    /// or <c>null</c> if the leader is unknown (e.g. during an election).
    /// </summary>
    public string? LeaderHint => _leaderId;

    /// <summary>
    /// Loads durable state, starts the mailbox consumer, and begins the election timer.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel startup.</param>
    public async Task StartAsync(CancellationToken cancellationToken = default) {
        var state = await _stateStorage.LoadAsync(cancellationToken);
        _currentTerm = state.CurrentTerm;
        _votedFor = state.VotedFor;

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _mailboxTask = Task.Run(() => RunMailboxAsync(_cts.Token), _cts.Token);

        _role = new FollowerRole(this);
    }

    /// <summary>
    /// Cancels the mailbox consumer, stops timers, and awaits any in-flight mailbox work.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the stop operation (not used for forced cancellation — that is handled internally).</param>
    public async Task StopAsync(CancellationToken cancellationToken = default) {
        if (_cts is not null) {
            await _cts.CancelAsync();
            if (_mailboxTask is not null)
                await _mailboxTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
        if (_role is LeaderRole leader)
            await leader.StopAsync();
        else
            _role?.Dispose();
    }

    /// <inheritdoc/>
    public void Dispose() {
        _cts?.Dispose();
        _role?.Dispose();
    }

    /// <summary>
    /// Proposes a command for replication. The returned task completes once the entry has been
    /// committed by a quorum and applied to the state machine.
    /// </summary>
    /// <param name="command">The opaque command payload to replicate.</param>
    /// <param name="cancellationToken">Token to cancel the proposal; cancellation does not roll back an already-committed entry.</param>
    /// <exception cref="NotLeaderException">Thrown immediately if this node is not the current leader.</exception>
    public Task<RaftApplyResult> ProposeAsync(ReadOnlyMemory<byte> command, CancellationToken cancellationToken = default) {
        var tcs = new TaskCompletionSource<RaftApplyResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancelTcsOnStop(tcs);
        Post(() => _role.ProposeAsync(command, tcs));
        return tcs.Task;
    }

    /// <summary>
    /// Handles an inbound <see cref="VoteRequest"/> from a candidate peer.
    /// The response is enqueued via the mailbox and returned once processed.
    /// </summary>
    /// <param name="request">The vote request sent by the candidate.</param>
    /// <param name="cancellationToken">Token to cancel waiting for the response.</param>
    public ValueTask<VoteResponse> ReceiveVoteRequestAsync(VoteRequest request, CancellationToken cancellationToken = default) {
        var tcs = new TaskCompletionSource<VoteResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancelTcsOnStop(tcs);
        Post(async () => tcs.TrySetResult(await _role.HandleVoteRequestAsync(request)));
        return new ValueTask<VoteResponse>(tcs.Task);
    }

    /// <summary>
    /// Handles an inbound <see cref="AppendEntriesRequest"/> from the leader (or a higher-term node).
    /// The response is enqueued via the mailbox and returned once processed.
    /// </summary>
    /// <param name="request">The append-entries request (or heartbeat) sent by the leader.</param>
    /// <param name="cancellationToken">Token to cancel waiting for the response.</param>
    public ValueTask<AppendEntriesResponse> ReceiveAppendEntriesAsync(AppendEntriesRequest request, CancellationToken cancellationToken = default) {
        var tcs = new TaskCompletionSource<AppendEntriesResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancelTcsOnStop(tcs);
        Post(async () => tcs.TrySetResult(await _role.HandleAppendEntriesAsync(request)));
        return new ValueTask<AppendEntriesResponse>(tcs.Task);
    }

    void CancelTcsOnStop<T>(TaskCompletionSource<T> tcs) =>
        _cts?.Token.Register(() => tcs.TrySetCanceled());

    async Task TransitionToFollowerAsync(ulong term) {
        if (term > _currentTerm) {
            _currentTerm = term;
            _votedFor = null;
            await _stateStorage.SaveAsync(new RaftPersistentState(_currentTerm, _votedFor));
        }
        if (_role is LeaderRole leader)
            await leader.StopAsync();
        else
            _role.Dispose();
        _role = new FollowerRole(this);
    }

    async Task TransitionToCandidateAsync() {
        _currentTerm++;
        _votedFor = _config.NodeId;
        _leaderId = null;
        await _stateStorage.SaveAsync(new RaftPersistentState(_currentTerm, _votedFor));
        _role.Dispose();
        _role = new CandidateRole(this);
    }

    async Task TransitionToLeaderAsync() {
        _leaderId = _config.NodeId;
        _role.Dispose();
        var leader = new LeaderRole(this);
        _role = leader;
        var noOp = new RaftLogEntry(_currentTerm, _log.LastIndex + 1, RaftLogEntryType.NoOp, ReadOnlyMemory<byte>.Empty);
        await _log.AppendAsync([noOp]);
        leader.StartHeartbeat();
        leader.ReplicateToAllPeers();
        await leader.TryAdvanceCommitIndexAsync();
    }

    async Task ApplyCommittedEntriesAsync() {
        while (stateMachine.LastAppliedIndex < _commitIndex) {
            var idx = stateMachine.LastAppliedIndex + 1;
            var entry = await _log.GetEntryAsync(idx);
            var success = await stateMachine.ApplyAsync(entry);
            _role.OnEntryApplied(idx, entry, success);
        }
    }
}
