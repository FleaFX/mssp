namespace MSSP.Raft;

/// <summary>
/// Distinguishes the purpose of a <see cref="RaftLogEntry"/>.
/// </summary>
public enum RaftLogEntryType : byte {
    /// <summary>
    /// A no-op entry appended by a newly elected leader to establish its authority and
    /// advance the commit index for any entries from prior terms (Raft Figure 8).
    /// </summary>
    NoOp = 0,

    /// <summary>
    /// A client command that is applied to the state machine once committed.
    /// </summary>
    Command = 1,
}

/// <summary>
/// An immutable entry in the Raft replicated log.
/// </summary>
/// <param name="Term">The leader term in which this entry was created.</param>
/// <param name="Index">The one-based position of this entry in the log.</param>
/// <param name="Type">Whether this is a <see cref="RaftLogEntryType.NoOp"/> or a <see cref="RaftLogEntryType.Command"/>.</param>
/// <param name="Payload">The opaque byte payload forwarded to the state machine on commit; empty for <see cref="RaftLogEntryType.NoOp"/> entries.</param>
public readonly record struct RaftLogEntry(ulong Term, ulong Index, RaftLogEntryType Type, ReadOnlyMemory<byte> Payload);
