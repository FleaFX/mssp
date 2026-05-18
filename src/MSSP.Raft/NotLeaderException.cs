namespace MSSP.Raft;

public sealed class NotLeaderException(string? leaderHint = null) : Exception("Not the leader.") {
    public string? LeaderHint { get; } = leaderHint;
}
