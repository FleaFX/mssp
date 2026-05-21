namespace MSSP.Raft;

/// <summary>
/// Persistent, ordered log of <see cref="RaftLogEntry"/> records used by the Raft consensus algorithm.
/// </summary>
/// <remarks>
/// Implementations must guarantee durability: entries appended via <see cref="AppendAsync"/> survive
/// process crashes before the caller receives the result. All index values are one-based.
/// </remarks>
public interface IRaftLog {
    /// <summary>
    /// Gets the one-based index of the last entry in the log, or zero if the log is empty.
    /// </summary>
    ulong LastIndex { get; }

    /// <summary>
    /// Gets the term of the last entry in the log, or zero if the log is empty.
    /// </summary>
    ulong LastTerm { get; }

    /// <summary>
    /// Returns the entry at the specified one-based <paramref name="index"/>.
    /// </summary>
    /// <param name="index">The one-based index of the entry to retrieve.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown if <paramref name="index"/> is zero or greater than <see cref="LastIndex"/>.
    /// </exception>
    ValueTask<RaftLogEntry> GetEntryAsync(ulong index, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns all entries starting from the given one-based index, in ascending order.
    /// </summary>
    /// <param name="fromIndex">The one-based index of the first entry to yield.</param>
    /// <param name="cancellationToken">Token to cancel the enumeration.</param>
    IAsyncEnumerable<RaftLogEntry> GetEntriesFromAsync(ulong fromIndex, CancellationToken cancellationToken = default);

    /// <summary>
    /// Durably appends one or more entries to the end of the log.
    /// </summary>
    /// <param name="entries">The entries to append, in order.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    ValueTask AppendAsync(IEnumerable<RaftLogEntry> entries, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes all entries at <paramref name="fromIndex"/> and beyond, truncating the log.
    /// Used when a follower discovers a conflicting entry that must be overwritten.
    /// </summary>
    /// <param name="fromIndex">The one-based index of the first entry to remove.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    ValueTask TruncateFromAsync(ulong fromIndex, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the term of the entry at the specified one-based <paramref name="index"/>.
    /// </summary>
    /// <param name="index">The one-based index of the entry whose term to retrieve.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    ValueTask<ulong> GetTermAtAsync(ulong index, CancellationToken cancellationToken = default);
}
