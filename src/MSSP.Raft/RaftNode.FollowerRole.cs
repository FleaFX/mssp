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
            var timeout = node._rng.Next(node._config.ElectionTimeoutMinMs, node._config.ElectionTimeoutMaxMs + 1);
            _electionTimer = new Timer(
                _ => node.Post(node.TransitionToCandidateAsync),
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
