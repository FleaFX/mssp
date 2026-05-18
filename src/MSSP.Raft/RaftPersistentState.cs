namespace MSSP.Raft;

public sealed record RaftPersistentState(ulong CurrentTerm, string? VotedFor);
