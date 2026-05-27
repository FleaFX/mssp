namespace MSSP.Raft;

/// <summary>
/// Base type for all messages processed by the <see cref="RaftNode"/> actor loop.
/// </summary>
/// <remarks>
/// All concrete subtypes are <c>internal sealed</c>; the discriminated union is closed to this
/// assembly. The actor loop dispatches incoming messages through a <c>switch</c> expression on
/// <see cref="RaftNode.DispatchAsync"/>.
/// </remarks>
internal abstract record RaftMessage;

/// <summary>
/// Fired by the election timer. The <see cref="Generation"/> must equal the node's current
/// election-timer generation; stale firings (from a superseded timer) are silently discarded.
/// </summary>
internal sealed record ElectionTimerFired(ulong Generation) : RaftMessage;

/// <summary>
/// Fired by the heartbeat timer. The <see cref="Generation"/> must equal the node's current
/// heartbeat-timer generation; stale firings are silently discarded.
/// </summary>
internal sealed record HeartbeatTimerFired(ulong Generation) : RaftMessage;

/// <summary>
/// Posted when a peer sends a <see cref="VoteRequest"/> to this node. The actor resolves
/// <see cref="Reply"/> with the response once the message is processed.
/// </summary>
internal sealed record VoteRequestReceived(VoteRequest Request, TaskCompletionSource<VoteResponse> Reply) : RaftMessage;

/// <summary>
/// Posted when a peer sends an <see cref="AppendEntriesRequest"/> to this node. The actor resolves
/// <see cref="Reply"/> with the response once the message is processed.
/// </summary>
internal sealed record AppendEntriesReceived(AppendEntriesRequest Request, TaskCompletionSource<AppendEntriesResponse> Reply) : RaftMessage;

/// <summary>
/// Posted when a peer sends an <see cref="InstallSnapshotRequest"/> to this node. The actor resolves
/// <see cref="Reply"/> with the response once the message is processed.
/// </summary>
internal sealed record InstallSnapshotReceived(InstallSnapshotRequest Request, TaskCompletionSource<InstallSnapshotResponse> Reply) : RaftMessage;

/// <summary>
/// Posted by a client that wants to replicate a command. The actor resolves <see cref="Reply"/>
/// once the entry is committed and applied to the state machine.
/// </summary>
internal sealed record ProposeReceived(ReadOnlyMemory<byte> Payload, TaskCompletionSource<RaftApplyResult> Reply) : RaftMessage;

/// <summary>
/// Posted by the background task that sent a <see cref="VoteRequest"/> RPC to a peer.
/// <see cref="SentTerm"/> carries the term at which the RPC was fired; the handler discards
/// the response if the current term has since advanced.
/// </summary>
internal sealed record VoteResponseReceived(string PeerId, VoteResponse Response, ulong SentTerm) : RaftMessage;

/// <summary>
/// Posted by the background task that sent an <see cref="AppendEntriesRequest"/> RPC to a peer.
/// <see cref="SentTerm"/> and <see cref="SentUpToIndex"/> are captured at send time.
/// </summary>
internal sealed record AppendEntriesResponseReceived(string PeerId, AppendEntriesResponse Response, ulong SentTerm, ulong SentUpToIndex) : RaftMessage;

/// <summary>
/// Posted by the background task that sent an <see cref="InstallSnapshotRequest"/> chunk to a peer.
/// On non-final chunks <see cref="SentMatchIndex"/> is zero; the actor only advances replication
/// state after the final chunk.
/// </summary>
internal sealed record InstallSnapshotResponseReceived(string PeerId, InstallSnapshotResponse Response, ulong SentTerm, ulong SentMatchIndex) : RaftMessage;

/// <summary>
/// Used exclusively by tests to detect when the actor has processed all previously injected messages.
/// The actor resolves <see cref="Completion"/> as soon as it dequeues this sentinel.
/// </summary>
internal sealed record DrainSentinel(TaskCompletionSource Completion) : RaftMessage;
