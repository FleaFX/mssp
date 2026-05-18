namespace MSSP.Raft;

public sealed record VoteRequest(ulong Term, string CandidateId, ulong LastLogIndex, ulong LastLogTerm);

public sealed record VoteResponse(ulong Term, bool VoteGranted);

public sealed record AppendEntriesRequest(
    ulong Term,
    string LeaderId,
    ulong PrevLogIndex,
    ulong PrevLogTerm,
    IReadOnlyList<RaftLogEntry> Entries,
    ulong LeaderCommit);

public sealed record AppendEntriesResponse(ulong Term, bool Success, ulong ConflictIndex, ulong ConflictTerm);
