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
    /// <param name="ct">Token to cancel the operation.</param>
    /// <returns>
    /// <c>true</c> if the entry was applied successfully;
    /// <c>false</c> if the entry was accepted by consensus but rejected by the application
    /// (e.g. an optimistic concurrency conflict).
    /// </returns>
    ValueTask<bool> ApplyAsync(RaftLogEntry entry, CancellationToken ct = default);
}
