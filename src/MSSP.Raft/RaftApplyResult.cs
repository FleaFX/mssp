namespace MSSP.Raft;

/// <summary>
/// The outcome returned by <see cref="RaftNode.ProposeAsync"/> after a command is committed and applied.
/// </summary>
/// <param name="IsOccConflict">
/// <c>true</c> if the state machine detected an optimistic concurrency conflict during apply;
/// the entry was committed by the cluster but produced no state change.
/// </param>
/// <param name="LeaderHint">
/// When the node is not the leader, the node ID of the known leader so callers can redirect;
/// <c>null</c> if the leader is unknown.
/// </param>
public sealed record RaftApplyResult(bool IsOccConflict, string? LeaderHint = null);
