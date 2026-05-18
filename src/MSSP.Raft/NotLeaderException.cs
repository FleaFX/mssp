namespace MSSP.Raft;

/// <summary>
/// Thrown when a write or read operation is directed at a node that is not the current Raft leader.
/// </summary>
public sealed class NotLeaderException(string? leaderHint = null) : Exception("Not the leader.") {
    /// <summary>
    /// Gets the node ID of the known leader, or <c>null</c> if the leader is currently unknown.
    /// Callers can use this to redirect the request to the correct node.
    /// </summary>
    public string? LeaderHint { get; } = leaderHint;
}
