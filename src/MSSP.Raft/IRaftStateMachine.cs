namespace MSSP.Raft;

/// <summary>
/// The application state machine driven by the Raft log.
/// Implementations receive committed <see cref="RaftLogEntry"/> records in order and mutate
/// application state accordingly.
/// </summary>
public interface IRaftStateMachine {
    /// <summary>
    /// Gets the one-based index of the last entry that has been applied to this state machine,
    /// or zero if no entries have been applied yet.
    /// </summary>
    ulong LastAppliedIndex { get; }

    /// <summary>
    /// Applies a committed log entry to the state machine.
    /// </summary>
    /// <param name="entry">The committed entry to apply.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>
    /// <c>true</c> if the entry was applied successfully;
    /// <c>false</c> if the entry was accepted by consensus but rejected by the application
    /// (e.g. an optimistic concurrency conflict).
    /// </returns>
    ValueTask<bool> ApplyAsync(RaftLogEntry entry, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a binary archive of the current state machine state suitable for shipping to a
    /// lagging follower via <see cref="InstallSnapshotRequest"/>.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>
    /// An opaque byte sequence that can later be passed to
    /// <see cref="InstallSnapshotAsync"/> on the receiving node.
    /// </returns>
    ValueTask<ReadOnlyMemory<byte>> CreateSnapshotAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces the current state machine state with <paramref name="data"/> and advances
    /// <see cref="LastAppliedIndex"/> to <paramref name="lastIncludedIndex"/>. Called after all
    /// chunks of an <c>InstallSnapshot</c> RPC have been received and reassembled.
    /// Implementations must never go backwards: if <paramref name="lastIncludedIndex"/> is less
    /// than or equal to the current <see cref="LastAppliedIndex"/>, the call is a no-op.
    /// </summary>
    /// <param name="lastIncludedIndex">The index of the last entry covered by the snapshot.</param>
    /// <param name="lastIncludedTerm">The term of the last entry covered by the snapshot.</param>
    /// <param name="data">The snapshot archive produced by <see cref="CreateSnapshotAsync"/>.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    ValueTask InstallSnapshotAsync(ulong lastIncludedIndex, ulong lastIncludedTerm, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default);
}
