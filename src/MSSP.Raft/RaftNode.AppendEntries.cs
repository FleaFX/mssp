namespace MSSP.Raft;

public sealed partial class RaftNode {
    async Task<AppendEntriesResponse> HandleAppendEntriesAsync(AppendEntriesRequest request) {
        if (request.Term > _currentTerm)
            await BecomeFollowerAsync(request.Term);

        if (request.Term < _currentTerm)
            return new AppendEntriesResponse(_currentTerm, false, 0, 0);

        _leaderId = request.LeaderId;
        if (_role == RaftRole.Candidate) _role = RaftRole.Follower;
        ResetElectionTimer();

        if (request.PrevLogIndex > 0) {
            if (log.LastIndex < request.PrevLogIndex)
                return new AppendEntriesResponse(_currentTerm, false, log.LastIndex + 1, 0);

            var termAtPrev = await log.GetTermAtAsync(request.PrevLogIndex);
            if (termAtPrev != request.PrevLogTerm) {
                var conflictTerm = termAtPrev;
                var conflictIndex = request.PrevLogIndex;
                while (conflictIndex > 1 && await log.GetTermAtAsync(conflictIndex - 1) == conflictTerm)
                    conflictIndex--;
                return new AppendEntriesResponse(_currentTerm, false, conflictIndex, conflictTerm);
            }
        }

        if (request.Entries.Count > 0) {
            foreach (var entry in request.Entries) {
                if (entry.Index <= log.LastIndex) {
                    var existing = await log.GetEntryAsync(entry.Index);
                    if (existing.Term != entry.Term)
                        await log.TruncateFromAsync(entry.Index);
                }
                if (entry.Index > log.LastIndex)
                    await log.AppendAsync([entry]);
            }
        }

        if (request.LeaderCommit > _commitIndex) {
            _commitIndex = Math.Min(request.LeaderCommit, log.LastIndex);
            await ApplyCommittedEntriesAsync();
        }

        return new AppendEntriesResponse(_currentTerm, true, 0, 0);
    }
}
