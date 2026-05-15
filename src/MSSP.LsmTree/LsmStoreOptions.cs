namespace MSSP.LsmTree;

/// <summary>
/// Configuration for opening or creating a <see cref="LsmStore{TKey}"/>.
/// </summary>
/// <param name="DataDirectory">The directory in which SST files are stored.</param>
/// <param name="CapacityBytes">The maximum size of the MemTable before it is flushed to an SST file.</param>
/// <param name="WalAppend">Delegate used to append records to the WAL.</param>
/// <param name="OnFlushed">Callback invoked after each MemTable flush, e.g. to rotate the WAL.</param>
/// <param name="CompactionThreshold">
/// Number of SST files that triggers automatic compaction after a flush.
/// Set to <see cref="int.MaxValue"/> to disable automatic compaction.
/// </param>
readonly record struct LsmStoreOptions(
    string DataDirectory,
    int CapacityBytes,
    WalAppendDelegate WalAppend,
    MemTableFlushedDelegate OnFlushed,
    int CompactionThreshold = 4
);
