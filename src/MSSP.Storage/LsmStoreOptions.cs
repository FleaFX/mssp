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
/// <param name="BaseLevelSizeBytes">
/// Size target for L1 (level index 0). Default: 4 x CapacityBytes (e.g., 256 MiB for 64 MiB MemTable).
/// This is the total size of all SST files in L1 that triggers compaction to L2.
/// </param>
/// <param name="LevelSizeMultiplier">
/// Multiplier for size targets per level. Default: 10 (L2 target = L1 x 10, L3 = L2 x 10, etc.).
/// </param>
/// <param name="SstAccess">
/// Strategy for SST file I/O. Defaults to <see cref="DefaultSstAccess{TKey}"/> when <c>null</c>.
/// Decorate to add cross-cutting behaviour such as bloom filter sidecars.
/// </param>
/// <param name="Metrics">
/// Optional metrics collector for LSM store operations. When <c>null</c>, no metrics are collected.
/// </param>
readonly record struct LsmStoreOptions<TKey>(
    string DataDirectory,
    int CapacityBytes,
    MemTableFlushedDelegate OnFlushed,
    long BaseLevelSizeBytes = -1,
    int LevelSizeMultiplier = 10,
    ISstAccess<TKey>? SstAccess = null,
    LsmStoreMetrics? Metrics = null
) where TKey : IKey<TKey> {
    /// <summary>
    /// Returns the resolved L1 size target: <see cref="BaseLevelSizeBytes"/> when explicitly set
    /// (positive), otherwise 4 × <see cref="CapacityBytes"/> as a sensible default.
    /// </summary>
    internal long EffectiveBaseLevelSizeBytes =>
        BaseLevelSizeBytes > 0 ? BaseLevelSizeBytes : CapacityBytes * 4L;
}
