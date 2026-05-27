using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace MSSP.Raft;

/// <summary>
/// Distinguishes the three roles a <see cref="RaftNode"/> may occupy at any given moment.
/// </summary>
internal enum NodeRole {
    /// <summary>
    /// Passively replicates entries from the leader. Default role at startup.
    /// </summary>
    Follower = 0,
    /// <summary>
    /// Solicits votes from peers in an attempt to win a leader election.
    /// </summary>
    Candidate = 1,
    /// <summary>
    /// Drives log replication and accepts client proposals.
    /// </summary>
    Leader = 2,
}

/// <summary>
/// A single node in a Raft consensus cluster.
/// </summary>
/// <remarks>
/// All state mutations are serialised through an internal actor channel — a
/// <see cref="Channel{T}"/> of typed <see cref="RaftMessage"/> values consumed by a single
/// background task. Callers may invoke the public API concurrently without external locking.
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
/// <param name="logger">Optional logger for diagnostic output.</param>
public sealed partial class RaftNode(
    RaftNodeConfig config,
    IRaftLog log,
    IRaftTransport transport,
    IRaftStateMachine stateMachine,
    IRaftStateStorage stateStorage,
    ILogger<RaftNode>? logger = null) : IAsyncDisposable {

    readonly Channel<RaftMessage> _channel = Channel.CreateUnbounded<RaftMessage>(
        new UnboundedChannelOptions { SingleReader = true });

    Task? _actorTask;
    CancellationTokenSource? _cts;

    readonly RaftNodeConfig _config = config;
    readonly IRaftLog _log = log;
    readonly IRaftTransport _transport = transport;
    readonly IRaftStateMachine _stateMachine = stateMachine;
    readonly IRaftStateStorage _stateStorage = stateStorage;
    readonly ILogger<RaftNode>? _logger = logger;

    // Persistent Raft state — persisted to storage on every change.
    internal ulong _currentTerm;   // internal: read by tests
    string? _votedFor;

    // Volatile state — reset on role transitions.
    // _role, _leaderHint and _noOpCommitted are read from external threads (e.g. ClusteredMsspClient
    // polling IsLeader/LeaderHint); volatile ensures memory visibility without a lock.
    internal volatile NodeRole _role;       // internal: read by tests
    volatile string? _leaderHint;
    ulong _commitIndex;

    // Timer generations — incremented on every (re)start; stale timer messages carry an old generation and are discarded.
    internal ulong _electionTimerGeneration;  // internal: read by tests
    ulong _heartbeatTimerGeneration;

    // Snapshot chunk reassembly — stored on the node (not on a role object) so chunks survive
    // role transitions within the same term. Cleared in BecomeFollowerAsync on term change.
    MemoryStream? _snapshotBuffer;
    ulong? _pendingSnapshotIndex;

    // Candidate state — valid only while _role == Candidate.
    int _votesGranted;

    // Leader state — allocated in BecomeLeaderAsync; null when not leader.
    Dictionary<string, ulong>? _nextIndex;
    Dictionary<string, ulong>? _matchIndex;
    Dictionary<ulong, TaskCompletionSource<RaftApplyResult>>? _pendingProposals;
    volatile bool _noOpCommitted;

    /// <summary>
    /// Gets the unique identifier of this node within the cluster.
    /// </summary>
    public string NodeId => _config.NodeId;

    /// <summary>
    /// Gets a value indicating whether this node is the current Raft leader and has committed its
    /// initial no-op entry, meaning it is ready to accept client proposals.
    /// </summary>
    public bool IsLeader => _role == NodeRole.Leader && _noOpCommitted;

    /// <summary>
    /// Gets the node ID of the node this node believes to be the current leader,
    /// or <c>null</c> if the leader is unknown (e.g. during an election).
    /// </summary>
    public string? LeaderHint => _leaderHint;

    /// <summary>
    /// Loads durable state, starts the actor loop, and arms the election timer.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel startup.</param>
    public async Task StartAsync(CancellationToken cancellationToken = default) {
        var state = await _stateStorage.LoadAsync(cancellationToken);
        _currentTerm = state.CurrentTerm;
        _votedFor = state.VotedFor;

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _actorTask = Task.Run(() => RunActorAsync(_cts.Token), cancellationToken);

        // Assign role before arming the timer so the first timer message is dispatched correctly.
        _role = NodeRole.Follower;
        RestartElectionTimer();
    }

    /// <summary>
    /// Cancels the actor loop, awaits shutdown, and fails any pending proposals.
    /// </summary>
    /// <param name="cancellationToken">
    /// Token to cancel waiting for the actor to drain (not used for forced cancellation —
    /// that is handled internally via the node's own <see cref="CancellationTokenSource"/>).
    /// </param>
    public async Task StopAsync(CancellationToken cancellationToken = default) {
        if (_cts is null) return;
        await _cts.CancelAsync();

        if (_actorTask is not null)
            await _actorTask.WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        FailPendingProposals();
        _channel.Writer.TryComplete();
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() {
        await StopAsync();
        _cts?.Dispose();
        _cts = null;            // null after dispose so _cts?.Token in public methods doesn't throw ObjectDisposedException
        _snapshotBuffer?.Dispose();
    }

    /// <summary>
    /// Proposes a command for replication. The returned task completes once the entry has been
    /// committed by a quorum and applied to the state machine.
    /// </summary>
    /// <param name="command">The opaque command payload to replicate.</param>
    /// <param name="cancellationToken">
    /// Token to cancel waiting for the result. Cancellation does not roll back an already-committed entry.
    /// </param>
    /// <exception cref="NotLeaderException">
    /// Thrown if this node is not the current leader, or if the initial no-op has not yet been committed.
    /// </exception>
    public Task<RaftApplyResult> ProposeAsync(ReadOnlyMemory<byte> command, CancellationToken cancellationToken = default) {
        var tcs = new TaskCompletionSource<RaftApplyResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (cancellationToken.CanBeCanceled)
            cancellationToken.UnsafeRegister(static s => ((TaskCompletionSource<RaftApplyResult>)s!).TrySetCanceled(), tcs);
        _channel.Writer.TryWrite(new ProposeReceived(command, tcs));

        // WaitAsync propagates node shutdown (stop-token) to the caller without leaking a registration
        // on _cts: the registration lives only for the duration of the WaitAsync, not until _cts is disposed.
        return tcs.Task.WaitAsync(_cts?.Token ?? CancellationToken.None);
    }

    /// <summary>
    /// Handles an inbound <see cref="VoteRequest"/> from a candidate peer.
    /// The response is processed by the actor and returned once resolved.
    /// </summary>
    /// <param name="request">The vote request sent by the candidate.</param>
    /// <param name="cancellationToken">Token to cancel waiting for the response.</param>
    public ValueTask<VoteResponse> ReceiveVoteRequestAsync(VoteRequest request, CancellationToken cancellationToken = default) {
        var tcs = new TaskCompletionSource<VoteResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (cancellationToken.CanBeCanceled)
            cancellationToken.UnsafeRegister(static s => ((TaskCompletionSource<VoteResponse>)s!).TrySetCanceled(), tcs);

        _channel.Writer.TryWrite(new VoteRequestReceived(request, tcs));
        return new ValueTask<VoteResponse>(tcs.Task.WaitAsync(_cts?.Token ?? CancellationToken.None));
    }

    /// <summary>
    /// Handles an inbound <see cref="AppendEntriesRequest"/> from the leader.
    /// The response is processed by the actor and returned once resolved.
    /// </summary>
    /// <param name="request">The append-entries request (or heartbeat) sent by the leader.</param>
    /// <param name="cancellationToken">Token to cancel waiting for the response.</param>
    public ValueTask<AppendEntriesResponse> ReceiveAppendEntriesAsync(AppendEntriesRequest request, CancellationToken cancellationToken = default) {
        var tcs = new TaskCompletionSource<AppendEntriesResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (cancellationToken.CanBeCanceled)
            cancellationToken.UnsafeRegister(static s => ((TaskCompletionSource<AppendEntriesResponse>)s!).TrySetCanceled(), tcs);

        _channel.Writer.TryWrite(new AppendEntriesReceived(request, tcs));
        return new ValueTask<AppendEntriesResponse>(tcs.Task.WaitAsync(_cts?.Token ?? CancellationToken.None));
    }

    /// <summary>
    /// Handles an inbound <see cref="InstallSnapshotRequest"/> from the leader.
    /// The response is processed by the actor and returned once resolved.
    /// </summary>
    /// <param name="request">The install-snapshot request sent by the leader.</param>
    /// <param name="cancellationToken">Token to cancel waiting for the response.</param>
    public ValueTask<InstallSnapshotResponse> ReceiveInstallSnapshotAsync(InstallSnapshotRequest request, CancellationToken cancellationToken = default) {
        var tcs = new TaskCompletionSource<InstallSnapshotResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (cancellationToken.CanBeCanceled)
            cancellationToken.UnsafeRegister(static s => ((TaskCompletionSource<InstallSnapshotResponse>)s!).TrySetCanceled(), tcs);

        _channel.Writer.TryWrite(new InstallSnapshotReceived(request, tcs));
        return new ValueTask<InstallSnapshotResponse>(tcs.Task.WaitAsync(_cts?.Token ?? CancellationToken.None));
    }

    async Task RunActorAsync(CancellationToken cancellationToken) {
        await foreach (var msg in _channel.Reader.ReadAllAsync(cancellationToken)) {
            try {
                await DispatchAsync(msg);
            } catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
                break;
            } catch (Exception ex) {
                _logger?.LogError(ex, "Unhandled error processing {MessageType}", msg.GetType().Name);
            }
        }
    }

    Task DispatchAsync(RaftMessage msg) => msg switch {
        ElectionTimerFired electionTimer => OnElectionTimerFiredAsync(electionTimer.Generation),
        HeartbeatTimerFired heartBeatTimer => OnHeartbeatTimerFiredAsync(heartBeatTimer.Generation),
        VoteRequestReceived voteRequest => OnVoteRequestReceivedAsync(voteRequest.Request, voteRequest.Reply),
        AppendEntriesReceived appendEntries => OnAppendEntriesReceivedAsync(appendEntries.Request, appendEntries.Reply),
        InstallSnapshotReceived installSnapshot => OnInstallSnapshotReceivedAsync(installSnapshot.Request, installSnapshot.Reply),
        ProposeReceived proposal => OnProposeReceivedAsync(proposal.Payload, proposal.Reply),
        VoteResponseReceived voteResponse => OnVoteResponseReceivedAsync(voteResponse.PeerId, voteResponse.Response, voteResponse.SentTerm),
        AppendEntriesResponseReceived appendEntries => OnAppendEntriesResponseReceivedAsync(appendEntries.PeerId, appendEntries.Response, appendEntries.SentTerm, appendEntries.SentUpToIndex),
        InstallSnapshotResponseReceived installSnapshot => OnInstallSnapshotResponseReceivedAsync(installSnapshot.PeerId, installSnapshot.Response, installSnapshot.SentTerm, installSnapshot.SentMatchIndex),
        DrainSentinel drain => DrainAsync(drain),
        _ => Task.CompletedTask,
    };

    static Task DrainAsync(DrainSentinel m) {
        m.Completion.TrySetResult();
        return Task.CompletedTask;
    }
}
