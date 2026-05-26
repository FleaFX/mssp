namespace MSSP.Raft;

public sealed partial class RaftNode {

    /// <summary>
    /// Transitions this node to the candidate role and starts a new election.
    /// </summary>
    /// <remarks>
    /// Increments the current term (Raft §5.2), records a self-vote, and fires
    /// <see cref="RequestVotesFromPeers"/> to solicit remote votes. In a single-node cluster
    /// the quorum is immediately satisfied and the node transitions directly to leader.
    /// </remarks>
    async Task BecomeCandidateAsync() {
        // Raft §5.2: increment term and vote for self.
        _currentTerm++;
        _votedFor = _config.NodeId;
        await PersistStateAsync();

        _leaderHint   = null;
        _votesGranted = 1;  // self-vote

        // Re-arm the election timer in case this election times out without a winner.
        RestartElectionTimer();

        // Assign role before soliciting votes so inbound responses see the correct role.
        _role = NodeRole.Candidate;

        // Check whether the self-vote already satisfies the quorum (single-node cluster).
        var quorum = (_config.PeerIds.Length + 1) / 2 + 1;
        if (_votesGranted >= quorum) {
            await BecomeLeaderAsync();
            return;
        }

        // Solicit votes from peers; responses are posted back as VoteResponseReceived messages.
        RequestVotesFromPeers();
    }

    /// <summary>
    /// Fires a background vote-request RPC to every peer. Each task posts its result back to
    /// the actor channel as a <see cref="VoteResponseReceived"/> message.
    /// </summary>
    void RequestVotesFromPeers() {
        var request = new VoteRequest(_currentTerm, _config.NodeId, _log.LastIndex, _log.LastTerm);
        var capturedTerm = _currentTerm;
        foreach (var peerId in _config.PeerIds)
            _ = SendVoteRequestToPeerAsync(peerId, request, capturedTerm);
    }

    /// <summary>
    /// Sends a single <see cref="VoteRequest"/> to <paramref name="peerId"/> and posts the
    /// result back to the actor channel.
    /// </summary>
    /// <param name="peerId">The peer node to contact.</param>
    /// <param name="request">The vote request to send.</param>
    /// <param name="capturedTerm">The term at the moment the RPC was fired; used for staleness detection.</param>
    async Task SendVoteRequestToPeerAsync(string peerId, VoteRequest request, ulong capturedTerm) {
        var ct = _cts?.Token ?? CancellationToken.None;
        try {
            var response = await _transport.RequestVoteAsync(peerId, request, ct);
            _channel.Writer.TryWrite(new VoteResponseReceived(peerId, response, capturedTerm));
        } catch {
            // Peer unavailable or cancelled; the election timer will retry if needed.
        }
    }

    /// <summary>
    /// Handles a <see cref="VoteResponseReceived"/> message. Counts granted votes and transitions
    /// to leader once a quorum is reached (Raft §5.2).
    /// </summary>
    async Task OnVoteResponseReceivedAsync(string peerId, VoteResponse response, ulong sentTerm) {
        // Ignore votes from stale elections.
        if (sentTerm != _currentTerm || _role != NodeRole.Candidate) return;

        if (response.Term > _currentTerm) {
            await BecomeFollowerAsync(response.Term);
            return;
        }

        if (!response.VoteGranted) return;

        _votesGranted++;
        // PeerIds contains only *other* nodes; add 1 for self to get the cluster size.
        var quorum = (_config.PeerIds.Length + 1) / 2 + 1;
        if (_votesGranted >= quorum)
            await BecomeLeaderAsync();
    }
}
