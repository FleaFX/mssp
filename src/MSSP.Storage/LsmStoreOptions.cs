namespace MSSP.Storage;

/// <summary>
/// Invoked after each MemTable flush, e.g. to rotate the WAL so flushed records are no longer replayed on recovery.
/// </summary>
/// <param name="cancellationToken">Token to cancel the callback.</param>
delegate ValueTask MemTableFlushedDelegate(CancellationToken cancellationToken);

/// <summary>
/// Configuration for opening or creating a <see cref="LsmStore{TKey}"/>.
/// </summary>
/// <param name="DataDirectory">The directory in which SST files are stored.</param>
/// <param name="CapacityBytes">The maximum size of the MemTable before it is flushed to an SST file.</param>
/// <param name="OnFlushed">Callback invoked after each MemTable flush, e.g. to rotate the WAL.</param>
/// <param name="CompactionThreshold">
/// Number of SST files that triggers automatic compaction after a flush.
/// Set to <see cref="int.MaxValue"/> to disable automatic compaction.
/// </param>
/// <param name="SstAccess">
/// Strategy for SST file I/O. Defaults to <see cref="DefaultSstAccess{TKey}"/> when <c>null</c>.
/// Decorate to add cross-cutting behaviour such as bloom filter sidecars.
/// </param>
readonly record struct LsmStoreOptions<TKey>(
    string DataDirectory,
    int CapacityBytes,
    MemTableFlushedDelegate OnFlushed,
    int CompactionThreshold = 4,
    ISstAccess<TKey>? SstAccess = null
) where TKey : IKey<TKey>;
