namespace MSSP.Raft;

public sealed partial class RaftNode {
    /// <summary>
    /// The follower role: passively replicates log entries from the leader and grants votes
    /// to eligible candidates. Transitions to <see cref="CandidateRole"/> when the election
    /// timer fires without receiving a valid heartbeat or granting a vote.
    /// </summary>
    sealed class FollowerRole : RaftRole {
        readonly Timer _electionTimer;

        /// <summary>
        /// Initialises the follower role and arms the election timer with a randomised timeout.
        /// </summary>
        public FollowerRole(RaftNode node) : base(node) {
            // Capture the term at construction time. If the timer callback arrives in the
            // mailbox after a new role has been installed (e.g. a TransitionToFollowerAsync
            // due to a higher-term message just before this timer fires), the term will have
            // changed and the callback is silently discarded — preventing spurious elections.
            var capturedTerm = node._currentTerm;
            var timeout = node._rng.Next(node._config.ElectionTimeoutMinMs, node._config.ElectionTimeoutMaxMs + 1);
            _electionTimer = new Timer(
                _ => node.Post(async () => {
                    if (node._currentTerm == capturedTerm && node._role is FollowerRole)
                        await node.TransitionToCandidateAsync();
                }),
                null, timeout, Timeout.Infinite);
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
