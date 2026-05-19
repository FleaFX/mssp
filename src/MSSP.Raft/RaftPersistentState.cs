namespace MSSP.Raft;

/// <summary>
/// The subset of Raft node state that must survive crashes and restarts.
/// </summary>
/// <remarks>
/// Per the Raft paper, <see cref="CurrentTerm"/> and <see cref="VotedFor"/> must be written to
/// stable storage before responding to any RPC.
/// </remarks>
/// <param name="CurrentTerm">The latest term this node has seen.</param>
/// <param name="VotedFor">
/// The candidate ID this node voted for in <see cref="CurrentTerm"/>,
/// or <c>null</c> if no vote has been cast in the current term.
/// </param>
public sealed record RaftPersistentState(ulong CurrentTerm, string? VotedFor);
