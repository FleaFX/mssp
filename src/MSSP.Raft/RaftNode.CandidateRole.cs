namespace MSSP.Raft;

public sealed partial class RaftNode {
    /// <summary>
    /// The candidate role: solicits votes from peers in an attempt to win a leader election.
    /// Transitions to <see cref="LeaderRole"/> on winning a quorum, or back to
    /// <see cref="FollowerRole"/> on receiving a valid message from a higher-term leader.
    /// Re-runs the election when the timer fires without a decisive outcome.
    /// </summary>
    sealed class CandidateRole : RaftRole {
        readonly Timer _electionTimer;
        int _votes = 1;

        /// <summary>
        /// Initialises the candidate role, arms the election timer, and enqueues the vote
        /// solicitation via the node's mailbox.
        /// </summary>
        public CandidateRole(RaftNode node) : base(node) {
            var timeout = node._rng.Next(node._config.ElectionTimeoutMinMs, node._config.ElectionTimeoutMaxMs + 1);
            _electionTimer = new Timer(
                _ => node.Post(node.TransitionToCandidateAsync),
                null, timeout, Timeout.Infinite);
            node.Post(StartElectionAsync);
        }

        async Task StartElectionAsync() {
            if (Node._cts?.IsCancellationRequested == true) return;

            if (Node._config.PeerIds.Length == 0) {
                await Node.TransitionToLeaderAsync();
                return;
            }

            var request = new VoteRequest(Node._currentTerm, Node._config.NodeId, Node._log.LastIndex, Node._log.LastTerm);
            var electionTerm = Node._currentTerm;
            var votesNeeded = (Node._config.PeerIds.Length + 1) / 2 + 1;
            var nodeToken = Node._cts?.Token ?? CancellationToken.None;

            foreach (var peerId in Node._config.PeerIds) {
                var pid = peerId;
                _ = Task.Run(async () => {
                    try {
                        var response = await Node._transport.RequestVoteAsync(pid, request, nodeToken);
                        Node.Post(async () => {
                            if (Node._role is not CandidateRole candidate || Node._currentTerm != electionTerm) return;
                            if (response.Term > Node._currentTerm) { await Node.TransitionToFollowerAsync(response.Term); return; }
                            if (!response.VoteGranted) return;
                            if (++candidate._votes >= votesNeeded)
                                await Node.TransitionToLeaderAsync();
                        });
                    } catch { /* peer unavailable or cancelled */ }
                }, nodeToken);
            }
        }

        /// <inheritdoc/>
        internal override void ResetElectionTimer() {
            var timeout = Node._rng.Next(Node._config.ElectionTimeoutMinMs, Node._config.ElectionTimeoutMaxMs + 1);
            _electionTimer.Change(timeout, Timeout.Infinite);
        }

        /// <inheritdoc/>
        public override void Dispose() => _electionTimer.Dispose();
    }
}
