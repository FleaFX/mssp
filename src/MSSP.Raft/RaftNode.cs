using System.Threading.Channels;

namespace MSSP.Raft;

/// <summary>
/// A single node in a Raft consensus cluster.
/// </summary>
/// <remarks>
/// All state mutations are serialized through an internal mailbox (a <see cref="System.Threading.Channels.Channel{T}"/>
/// single-consumer loop), so callers may invoke the public API concurrently without external locking.
/// <para>
/// A node starts as a follower. It transitions to candidate when its election timer fires,
/// and to leader once it wins a majority vote. A newly elected leader appends a no-op entry
/// before accepting client commands (Raft Figure 8).
/// </para>
/// </remarks>
public sealed class RaftNode : IDisposable {
    enum RaftRole { Follower, Candidate, Leader }

    readonly RaftNodeConfig _config;
    readonly IRaftLog _log;
    readonly IRaftTransport _transport;
    readonly IRaftStateMachine _stateMachine;
    readonly IRaftStateStorage _stateStorage;
    readonly Random _rng = new();

    // mailbox
    readonly Channel<Func<Task>> _mailbox = Channel.CreateUnbounded<Func<Task>>(
        new UnboundedChannelOptions { SingleReader = true });
    Task? _mailboxTask;
    CancellationTokenSource? _cts;

    // volatile state (only mutated in mailbox consumer)
    RaftRole _role = RaftRole.Follower;
    ulong _currentTerm;
    string? _votedFor;
    string? _leaderId;
    ulong _commitIndex;
    bool _noOpCommitted;

    // leader-only state
    readonly Dictionary<string, ulong> _nextIndex = new();
    readonly Dictionary<string, ulong> _matchIndex = new();
    readonly Dictionary<ulong, TaskCompletionSource<RaftApplyResult>> _pending = new();
    PeriodicTimer? _heartbeatTimer;
    Task? _heartbeatTask;

    // election timer
    System.Threading.Timer? _electionTimer;

    /// <summary>
    /// Gets the unique identifier of this node within the cluster.
    /// </summary>
    public string NodeId => _config.NodeId;

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
    /// Initialises a new <see cref="RaftNode"/> with the given dependencies.
    /// Call <see cref="StartAsync"/> to begin participating in the cluster.
    /// </summary>
    /// <param name="config">Static configuration: node ID, peer IDs, and timeout settings.</param>
    /// <param name="log">The durable replicated log.</param>
    /// <param name="transport">The network layer used to contact peers.</param>
    /// <param name="stateMachine">The application state machine that applies committed entries.</param>
    /// <param name="stateStorage">Durable storage for the node's persistent Raft state.</param>
    public RaftNode(RaftNodeConfig config, IRaftLog log, IRaftTransport transport,
                    IRaftStateMachine stateMachine, IRaftStateStorage stateStorage) {
        _config = config;
        _log = log;
        _transport = transport;
        _stateMachine = stateMachine;
        _stateStorage = stateStorage;
    }

    /// <summary>
    /// Loads durable state, starts the mailbox consumer, and begins the election timer.
    /// </summary>
    /// <param name="ct">Token to cancel startup.</param>
    public async Task StartAsync(CancellationToken ct = default) {
        var state = await _stateStorage.LoadAsync(ct);
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
        _electionTimer?.Dispose();
        _heartbeatTimer?.Dispose();
        _heartbeatTask = null;
    }

    /// <inheritdoc/>
    public void Dispose() {
        _cts?.Dispose();
        _electionTimer?.Dispose();
        _heartbeatTimer?.Dispose();
    }

    // --- public API ---

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
            var entry = new RaftLogEntry(_currentTerm, _log.LastIndex + 1, RaftLogEntryType.Command, command);
            await _log.AppendAsync([entry]);
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

    // --- mailbox ---

    void Post(Func<Task> work) => _mailbox.Writer.TryWrite(work);

    async Task RunMailboxAsync(CancellationToken ct) {
        await foreach (var work in _mailbox.Reader.ReadAllAsync(ct))
            try { await work(); } catch { /* individual work items handle their own errors */ }
    }

    // --- election timer ---

    void ResetElectionTimer() {
        var timeout = _rng.Next(_config.ElectionTimeoutMinMs, _config.ElectionTimeoutMaxMs + 1);
        _electionTimer?.Change(timeout, Timeout.Infinite);
        if (_electionTimer is null)
            _electionTimer = new System.Threading.Timer(_ => Post(StartElectionAsync), null, timeout, Timeout.Infinite);
        else
            _electionTimer.Change(timeout, Timeout.Infinite);
    }

    void StopElectionTimer() => _electionTimer?.Change(Timeout.Infinite, Timeout.Infinite);

    // --- role transitions ---

    async Task BecomeFollowerAsync(ulong term) {
        if (term > _currentTerm) {
            _currentTerm = term;
            _votedFor = null;
            await _stateStorage.SaveAsync(new RaftPersistentState(_currentTerm, _votedFor));
        }
        if (_role == RaftRole.Leader)
            await StopHeartbeatAsync();
        _role = RaftRole.Follower;
        _noOpCommitted = false;
        FailAllPendingProposals();
        ResetElectionTimer();
    }

    async Task BecomeLeaderAsync() {
        _role = RaftRole.Leader;
        _leaderId = _config.NodeId;
        _noOpCommitted = false;

        // initialize leader replication state
        var nextIdx = _log.LastIndex + 1;
        foreach (var peerId in _config.PeerIds) {
            _nextIndex[peerId] = nextIdx;
            _matchIndex[peerId] = 0;
        }

        // no-op entry to commit any uncommitted entries from prior terms
        var noOp = new RaftLogEntry(_currentTerm, _log.LastIndex + 1, RaftLogEntryType.NoOp, ReadOnlyMemory<byte>.Empty);
        await _log.AppendAsync([noOp]);

        await StartHeartbeatAsync();
        await ReplicateToAllPeersAsync();
        // for single-node clusters (no peers) advance commit index immediately
        await TryAdvanceCommitIndexAsync();
    }

    // --- election ---

    async Task StartElectionAsync() {
        if (_cts?.IsCancellationRequested == true) return;

        _currentTerm++;
        _role = RaftRole.Candidate;
        _votedFor = _config.NodeId;
        _leaderId = null;
        await _stateStorage.SaveAsync(new RaftPersistentState(_currentTerm, _votedFor));
        ResetElectionTimer();

        if (_config.PeerIds.Length == 0) {
            // single-node cluster: immediately become leader
            await BecomeLeaderAsync();
            return;
        }

        var request = new VoteRequest(_currentTerm, _config.NodeId, _log.LastIndex, _log.LastTerm);
        var electionTerm = _currentTerm;
        var votesNeeded = (_config.PeerIds.Length + 1) / 2 + 1; // majority including self
        var votes = 1; // self-vote

        // fire-and-forget: responses come back via Post
        var nodeToken = _cts?.Token ?? CancellationToken.None;
        foreach (var peerId in _config.PeerIds) {
            var pid = peerId;
            _ = Task.Run(async () => {
                try {
                    var response = await _transport.RequestVoteAsync(pid, request, nodeToken);
                    Post(async () => {
                        if (_role != RaftRole.Candidate || _currentTerm != electionTerm) return;
                        if (response.Term > _currentTerm) { await BecomeFollowerAsync(response.Term); return; }
                        if (!response.VoteGranted) return;
                        votes++;
                        if (votes >= votesNeeded && _role == RaftRole.Candidate)
                            await BecomeLeaderAsync();
                    });
                } catch { /* peer unavailable or cancelled */ }
            });
        }
    }

    // --- vote handler ---

    async Task<VoteResponse> HandleVoteRequestAsync(VoteRequest request) {
        if (request.Term > _currentTerm)
            await BecomeFollowerAsync(request.Term);

        if (request.Term < _currentTerm)
            return new VoteResponse(_currentTerm, false);

        var alreadyVotedForOther = _votedFor is not null && _votedFor != request.CandidateId;
        if (alreadyVotedForOther)
            return new VoteResponse(_currentTerm, false);

        // candidate's log must be at least as up-to-date as ours
        var logOk = request.LastLogTerm > _log.LastTerm ||
                    (request.LastLogTerm == _log.LastTerm && request.LastLogIndex >= _log.LastIndex);
        if (!logOk)
            return new VoteResponse(_currentTerm, false);

        _votedFor = request.CandidateId;
        await _stateStorage.SaveAsync(new RaftPersistentState(_currentTerm, _votedFor));
        ResetElectionTimer();
        return new VoteResponse(_currentTerm, true);
    }

    // --- AppendEntries handler ---

    async Task<AppendEntriesResponse> HandleAppendEntriesAsync(AppendEntriesRequest request) {
        if (request.Term > _currentTerm)
            await BecomeFollowerAsync(request.Term);

        if (request.Term < _currentTerm)
            return new AppendEntriesResponse(_currentTerm, false, 0, 0);

        // valid AppendEntries from current leader
        _leaderId = request.LeaderId;
        if (_role == RaftRole.Candidate) _role = RaftRole.Follower;
        ResetElectionTimer();

        // consistency check
        if (request.PrevLogIndex > 0) {
            if (_log.LastIndex < request.PrevLogIndex)
                return new AppendEntriesResponse(_currentTerm, false, _log.LastIndex + 1, 0);

            var termAtPrev = await _log.GetTermAtAsync(request.PrevLogIndex);
            if (termAtPrev != request.PrevLogTerm) {
                // fast backtracking: find first index of the conflicting term
                var conflictTerm = termAtPrev;
                var conflictIndex = request.PrevLogIndex;
                while (conflictIndex > 1 && await _log.GetTermAtAsync(conflictIndex - 1) == conflictTerm)
                    conflictIndex--;
                return new AppendEntriesResponse(_currentTerm, false, conflictIndex, conflictTerm);
            }
        }

        // append new entries, truncating any conflicting tail
        if (request.Entries.Count > 0) {
            var insertFrom = request.PrevLogIndex + 1;
            foreach (var entry in request.Entries) {
                if (entry.Index <= _log.LastIndex) {
                    var existing = await _log.GetEntryAsync(entry.Index);
                    if (existing.Term != entry.Term)
                        await _log.TruncateFromAsync(entry.Index);
                }
                if (entry.Index > _log.LastIndex)
                    await _log.AppendAsync([entry]);
                insertFrom++;
            }
        }

        // advance commit index
        if (request.LeaderCommit > _commitIndex) {
            _commitIndex = Math.Min(request.LeaderCommit, _log.LastIndex);
            await ApplyCommittedEntriesAsync();
        }

        return new AppendEntriesResponse(_currentTerm, true, 0, 0);
    }

    // --- replication ---

    async Task ReplicateToAllPeersAsync() {
        var nodeToken = _cts?.Token ?? CancellationToken.None;
        foreach (var peerId in _config.PeerIds) {
            var pid = peerId;
            _ = Task.Run(() => ReplicateToPeerAsync(pid, nodeToken));
        }
        await Task.CompletedTask;
    }

    async Task ReplicateToPeerAsync(string peerId, CancellationToken ct = default) {
        if (_role != RaftRole.Leader) return;
        if (ct.IsCancellationRequested) return;

        ulong nextIdx;
        lock (_nextIndex) nextIdx = _nextIndex.GetValueOrDefault(peerId, _log.LastIndex + 1);

        ulong prevLogIndex = nextIdx - 1;
        ulong prevLogTerm = prevLogIndex == 0 ? 0 : await _log.GetTermAtAsync(prevLogIndex);

        var entries = new List<RaftLogEntry>();
        await foreach (var entry in _log.GetEntriesFromAsync(nextIdx))
            entries.Add(entry);

        var request = new AppendEntriesRequest(
            _currentTerm, _config.NodeId,
            prevLogIndex, prevLogTerm,
            entries, _commitIndex);

        try {
            var response = await _transport.AppendEntriesAsync(peerId, request, ct);
            Post(async () => {
                if (_role != RaftRole.Leader) return;
                if (response.Term > _currentTerm) { await BecomeFollowerAsync(response.Term); return; }
                if (response.Success) {
                    if (entries.Count > 0) {
                        _matchIndex[peerId] = entries[^1].Index;
                        _nextIndex[peerId] = entries[^1].Index + 1;
                    }
                    await TryAdvanceCommitIndexAsync();
                } else {
                    // fast backtracking
                    if (response.ConflictTerm > 0) {
                        // find last entry in leader log with ConflictTerm
                        ulong newNext = response.ConflictIndex;
                        for (var i = _log.LastIndex; i >= 1; i--) {
                            if (await _log.GetTermAtAsync(i) == response.ConflictTerm) {
                                newNext = i + 1;
                                break;
                            }
                        }
                        _nextIndex[peerId] = newNext;
                    } else {
                        _nextIndex[peerId] = Math.Max(1, response.ConflictIndex);
                    }
                    _ = Task.Run(() => ReplicateToPeerAsync(peerId, _cts?.Token ?? CancellationToken.None));
                }
            });
        } catch { /* peer unavailable, will retry on next heartbeat */ }
    }

    // --- commit advancement ---

    async Task TryAdvanceCommitIndexAsync() {
        if (_role != RaftRole.Leader) return;

        var quorum = (_config.PeerIds.Length + 1) / 2 + 1;
        for (var n = _log.LastIndex; n > _commitIndex; n--) {
            var termAtN = await _log.GetTermAtAsync(n);
            if (termAtN != _currentTerm) break; // only commit from current term

            var matchCount = 1; // leader itself
            foreach (var peerId in _config.PeerIds)
                if (_matchIndex.GetValueOrDefault(peerId) >= n)
                    matchCount++;

            if (matchCount >= quorum) {
                _commitIndex = n;
                await ApplyCommittedEntriesAsync();
                break;
            }
        }
    }

    async Task ApplyCommittedEntriesAsync() {
        while (_stateMachine.LastAppliedIndex < _commitIndex) {
            var idx = _stateMachine.LastAppliedIndex + 1;
            var entry = await _log.GetEntryAsync(idx);
            var success = await _stateMachine.ApplyAsync(entry);

            if (_pending.TryGetValue(idx, out var tcs)) {
                _pending.Remove(idx);
                if (entry.Type == RaftLogEntryType.NoOp) {
                    _noOpCommitted = true;
                    tcs.TrySetResult(new RaftApplyResult(false));
                } else {
                    tcs.TrySetResult(new RaftApplyResult(!success));
                }
            } else if (entry.Type == RaftLogEntryType.NoOp && _role == RaftRole.Leader) {
                _noOpCommitted = true;
            }
        }
    }

    // --- heartbeat ---

    async Task StartHeartbeatAsync() {
        StopElectionTimer();
        _heartbeatTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(_config.HeartbeatIntervalMs));
        _heartbeatTask = Task.Run(async () => {
            while (await _heartbeatTimer.WaitForNextTickAsync()) {
                Post(async () => {
                    if (_role == RaftRole.Leader)
                        await ReplicateToAllPeersAsync();
                });
            }
        });
        await Task.CompletedTask;
    }

    async Task StopHeartbeatAsync() {
        _heartbeatTimer?.Dispose();
        _heartbeatTimer = null;
        if (_heartbeatTask is not null) {
            await _heartbeatTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            _heartbeatTask = null;
        }
    }

    // --- helpers ---

    void FailAllPendingProposals() {
        foreach (var tcs in _pending.Values)
            tcs.TrySetException(new NotLeaderException(_leaderId));
        _pending.Clear();
    }
}
