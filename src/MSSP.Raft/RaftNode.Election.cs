namespace MSSP.Raft;

public sealed partial class RaftNode {
    Timer? _electionTimer;

    void ResetElectionTimer() {
        var timeout = _rng.Next(config.ElectionTimeoutMinMs, config.ElectionTimeoutMaxMs + 1);
        if (_electionTimer is null)
            _electionTimer = new Timer(_ => Post(StartElectionAsync), null, timeout, Timeout.Infinite);
        else
            _electionTimer.Change(timeout, Timeout.Infinite);
    }

    void StopElectionTimer() => _electionTimer?.Change(Timeout.Infinite, Timeout.Infinite);

    async Task BecomeFollowerAsync(ulong term) {
        if (term > _currentTerm) {
            _currentTerm = term;
            _votedFor = null;
            await stateStorage.SaveAsync(new RaftPersistentState(_currentTerm, _votedFor));
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
        _leaderId = config.NodeId;
        _noOpCommitted = false;

        var nextIdx = log.LastIndex + 1;
        foreach (var peerId in config.PeerIds) {
            _nextIndex[peerId] = nextIdx;
            _matchIndex[peerId] = 0;
        }

        // no-op entry to commit any uncommitted entries from prior terms (Raft Figure 8)
        var noOp = new RaftLogEntry(_currentTerm, log.LastIndex + 1, RaftLogEntryType.NoOp, ReadOnlyMemory<byte>.Empty);
        await log.AppendAsync([noOp]);

        await StartHeartbeatAsync();
        await ReplicateToAllPeersAsync();
        await TryAdvanceCommitIndexAsync();
    }

    async Task StartElectionAsync() {
        if (_cts?.IsCancellationRequested == true) return;

        _currentTerm++;
        _role = RaftRole.Candidate;
        _votedFor = config.NodeId;
        _leaderId = null;
        await stateStorage.SaveAsync(new RaftPersistentState(_currentTerm, _votedFor));
        ResetElectionTimer();

        if (config.PeerIds.Length == 0) {
            await BecomeLeaderAsync();
            return;
        }

        var request = new VoteRequest(_currentTerm, config.NodeId, log.LastIndex, log.LastTerm);
        var electionTerm = _currentTerm;
        var votesNeeded = (config.PeerIds.Length + 1) / 2 + 1;
        var votes = 1;

        var nodeToken = _cts?.Token ?? CancellationToken.None;
        foreach (var peerId in config.PeerIds) {
            var pid = peerId;
            _ = Task.Run(async () => {
                try {
                    var response = await transport.RequestVoteAsync(pid, request, nodeToken);
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

    async Task<VoteResponse> HandleVoteRequestAsync(VoteRequest request) {
        if (request.Term > _currentTerm)
            await BecomeFollowerAsync(request.Term);

        if (request.Term < _currentTerm)
            return new VoteResponse(_currentTerm, false);

        var alreadyVotedForOther = _votedFor is not null && _votedFor != request.CandidateId;
        if (alreadyVotedForOther)
            return new VoteResponse(_currentTerm, false);

        // candidate's log must be at least as up-to-date as ours
        var logOk = request.LastLogTerm > log.LastTerm ||
                    (request.LastLogTerm == log.LastTerm && request.LastLogIndex >= log.LastIndex);
        if (!logOk)
            return new VoteResponse(_currentTerm, false);

        _votedFor = request.CandidateId;
        await stateStorage.SaveAsync(new RaftPersistentState(_currentTerm, _votedFor));
        ResetElectionTimer();
        return new VoteResponse(_currentTerm, true);
    }
}
