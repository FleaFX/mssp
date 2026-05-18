namespace MSSP.Raft;

public sealed record RaftApplyResult(bool IsOccConflict, string? LeaderHint = null);
