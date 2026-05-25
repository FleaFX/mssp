namespace MSSP.Raft;

public sealed partial class RaftNode {
    /// <summary>
    /// Base class for the three Raft node roles: follower, candidate, and leader.
    /// </summary>
    /// <remarks>
    /// Each role owns the resources and state that are valid only while that role is active.
    /// Transitioning roles disposes the outgoing instance and constructs the incoming one,
    /// so resource lifetimes are tied directly to role lifetimes.
    /// <para>
    /// The shared RPC handlers (<see cref="HandleVoteRequestAsync"/> and
    /// <see cref="HandleAppendEntriesAsync"/>) live here as template methods. Concrete roles
    /// override <see cref="ResetElectionTimer"/> and <see cref="OnEntryApplied"/> to provide
    /// role-specific behaviour without duplicating the common algorithm flow.
    /// </para>
    /// </remarks>
    abstract class RaftRole(RaftNode node) {
        /// <summary>
        /// Gets the node this role instance belongs to.
        /// </summary>
        protected RaftNode Node => node;

        /// <summary>
        /// Handles a client proposal. Default implementation rejects immediately with
        /// <see cref="NotLeaderException"/>; overridden by <see cref="LeaderRole"/> to
        /// append the command to the log and drive replication.
        /// </summary>
        public virtual Task ProposeAsync(ReadOnlyMemory<byte> command, TaskCompletionSource<RaftApplyResult> tcs) {
            tcs.TrySetException(new NotLeaderException(node._leaderId));
            return Task.CompletedTask;
        }

        /// <summary>
        /// Processes an inbound <see cref="VoteRequest"/>. Grants the vote when the
        /// candidate's term and log are at least as up-to-date as this node's, and resets
        /// the election timer on grant.
        /// </summary>
        public async Task<VoteResponse> HandleVoteRequestAsync(VoteRequest request) {
            if (request.Term > node._currentTerm)
                await node.TransitionToFollowerAsync(request.Term);

            if (request.Term < node._currentTerm)
                return new VoteResponse(node._currentTerm, false);

            var alreadyVotedForOther = node._votedFor is not null && node._votedFor != request.CandidateId;
            if (alreadyVotedForOther)
                return new VoteResponse(node._currentTerm, false);

            var logOk = request.LastLogTerm > node._log.LastTerm ||
                        (request.LastLogTerm == node._log.LastTerm && request.LastLogIndex >= node._log.LastIndex);
            if (!logOk)
                return new VoteResponse(node._currentTerm, false);

            node._votedFor = request.CandidateId;
            await node._stateStorage.SaveAsync(new RaftPersistentState(node._currentTerm, node._votedFor));
            node._role.ResetElectionTimer();
            return new VoteResponse(node._currentTerm, true);
        }

        /// <summary>
        /// Processes an inbound <see cref="AppendEntriesRequest"/>. Validates term and log
        /// consistency, appends new entries (truncating any conflicting tail), advances the
        /// commit index, and resets the election timer on a valid message.
        /// </summary>
        public async Task<AppendEntriesResponse> HandleAppendEntriesAsync(AppendEntriesRequest request) {
            if (request.Term > node._currentTerm)
                await node.TransitionToFollowerAsync(request.Term);

            if (request.Term < node._currentTerm)
                return new AppendEntriesResponse(node._currentTerm, false, 0, 0);

            if (node._role is CandidateRole)
                await node.TransitionToFollowerAsync(node._currentTerm);

            node._leaderId = request.LeaderId;
            node._role.ResetElectionTimer();

            if (request.PrevLogIndex > 0) {
                // reject if prevLogIndex is before our snapshot; InstallSnapshot handles catch-up (phase 2)
                if (request.PrevLogIndex < node._log.LastIncludedIndex)
                    return new AppendEntriesResponse(node._currentTerm, false, node._log.LastIncludedIndex + 1, 0);

                if (node._log.LastIndex < request.PrevLogIndex)
                    return new AppendEntriesResponse(node._currentTerm, false, node._log.LastIndex + 1, 0);

                var termAtPrev = await node._log.GetTermAtAsync(request.PrevLogIndex);
                if (termAtPrev != request.PrevLogTerm) {
                    var conflictTerm = termAtPrev;
                    var conflictIndex = request.PrevLogIndex;
                    while (conflictIndex > 1 && await node._log.GetTermAtAsync(conflictIndex - 1) == conflictTerm)
                        conflictIndex--;
                    return new AppendEntriesResponse(node._currentTerm, false, conflictIndex, conflictTerm);
                }
            }

            if (request.Entries.Count > 0) {
                foreach (var entry in request.Entries) {
                    if (entry.Index <= node._log.LastIncludedIndex) continue; // already in snapshot
                    if (entry.Index <= node._log.LastIndex) {
                        var existing = await node._log.GetEntryAsync(entry.Index);
                        if (existing.Term != entry.Term)
                            await node._log.TruncateFromAsync(entry.Index);
                    }
                    if (entry.Index > node._log.LastIndex)
                        await node._log.AppendAsync([entry]);
                }
            }

            if (request.LeaderCommit > node._commitIndex) {
                node._commitIndex = Math.Min(request.LeaderCommit, node._log.LastIndex);
                await node.ApplyCommittedEntriesAsync();
            }

            return new AppendEntriesResponse(node._currentTerm, true, 0, 0);
        }

        /// <summary>
        /// Resets the election timeout. No-op for <see cref="LeaderRole"/>; implemented by
        /// follower and candidate roles to prevent spurious elections while the cluster is healthy.
        /// </summary>
        internal virtual void ResetElectionTimer() { }

        /// <summary>
        /// Called after each log entry is applied to the state machine. No-op for follower and
        /// candidate; overridden by <see cref="LeaderRole"/> to resolve pending proposals and
        /// set <see cref="LeaderRole.NoOpCommitted"/> on the initial no-op entry.
        /// </summary>
        internal virtual void OnEntryApplied(ulong index, RaftLogEntry entry, bool success) { }

        /// <summary>
        /// Releases role-owned resources (timers, pending proposals).
        /// </summary>
        public virtual void Dispose() { }
    }
}
