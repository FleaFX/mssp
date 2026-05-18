namespace MSSP.Raft;

/// <summary>
/// Sent by a candidate to solicit votes from peers during an election.
/// </summary>
/// <param name="Term">The candidate's current term.</param>
/// <param name="CandidateId">The node ID of the candidate requesting the vote.</param>
/// <param name="LastLogIndex">The index of the candidate's last log entry.</param>
/// <param name="LastLogTerm">The term of the candidate's last log entry.</param>
public sealed record VoteRequest(ulong Term, string CandidateId, ulong LastLogIndex, ulong LastLogTerm);

/// <summary>
/// The response a peer returns after receiving a <see cref="VoteRequest"/>.
/// </summary>
/// <param name="Term">The responding peer's current term; the candidate uses this to step down if stale.</param>
/// <param name="VoteGranted"><c>true</c> if the peer voted for the candidate; otherwise <c>false</c>.</param>
public sealed record VoteResponse(ulong Term, bool VoteGranted);

/// <summary>
/// Sent by the leader to replicate log entries and/or serve as a heartbeat.
/// </summary>
/// <param name="Term">The leader's current term.</param>
/// <param name="LeaderId">The node ID of the leader, so followers can redirect clients.</param>
/// <param name="PrevLogIndex">The index of the log entry immediately preceding the new entries.</param>
/// <param name="PrevLogTerm">The term of the entry at <paramref name="PrevLogIndex"/>; used for the log consistency check.</param>
/// <param name="Entries">The log entries to append; empty for heartbeats.</param>
/// <param name="LeaderCommit">The leader's current commit index; followers advance their own commit index accordingly.</param>
public sealed record AppendEntriesRequest(
    ulong Term,
    string LeaderId,
    ulong PrevLogIndex,
    ulong PrevLogTerm,
    IReadOnlyList<RaftLogEntry> Entries,
    ulong LeaderCommit);

/// <summary>
/// The response a follower returns after receiving an <see cref="AppendEntriesRequest"/>.
/// </summary>
/// <param name="Term">The follower's current term; the leader uses this to step down if stale.</param>
/// <param name="Success"><c>true</c> if the follower accepted the entries; <c>false</c> if the consistency check failed.</param>
/// <param name="ConflictIndex">
/// On rejection, the first index of the conflicting term so the leader can fast-backtrack;
/// zero on success.
/// </param>
/// <param name="ConflictTerm">
/// On rejection, the term of the conflicting entry so the leader can skip the entire term;
/// zero on success.
/// </param>
public sealed record AppendEntriesResponse(ulong Term, bool Success, ulong ConflictIndex, ulong ConflictTerm);
