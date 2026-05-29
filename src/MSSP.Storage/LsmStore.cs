namespace MSSP.Storage;

/// <summary>
/// Log-Structured Merge-tree store. Provides keyed storage backed by an in-memory
/// <see cref="MemTable{TKey}"/> (Level 0) and a set of immutable SST files on disk organized
/// in multiple levels (Levels 1-N). Writes are applied directly to the MemTable; when the MemTable
/// is full it is flushed to a new SST file in L1. When the size of a level reaches its target,
/// all files in that level are merged into one file in the next level (cascading compaction).
/// </summary>
/// <remarks>
/// This class is not thread-safe. Callers are responsible for ensuring that writes, reads,
/// flushes, and compactions are not executed concurrently.
/// </remarks>
public sealed partial class LsmStore<TKey> : ILsmStore<TKey> where TKey : IKey<TKey> {
    readonly string _dataDirectory;
    readonly int _capacityBytes;
    readonly long _baseLevelSizeBytes;
    readonly int _levelSizeMultiplier;
    readonly MemTableFlushedDelegate _onFlushed;
    readonly ISstAccess<TKey> _sst;
    readonly List<List<SstFileInfo>> _sstLevels;
    readonly LsmStoreMetrics? _metrics;
    MemTable<TKey> _memTable;

    LsmStore(LsmStoreOptions<TKey> options, List<List<SstFileInfo>> sstLevels) {
        _dataDirectory = options.DataDirectory;
        _capacityBytes = options.CapacityBytes;
        _baseLevelSizeBytes = options.EffectiveBaseLevelSizeBytes;
        _levelSizeMultiplier = options.LevelSizeMultiplier;
        _onFlushed = options.OnFlushed;
        _sst = options.SstAccess ?? DefaultSstAccess<TKey>.Instance;
        _sstLevels = sstLevels;
        _memTable = new MemTable<TKey>(options.CapacityBytes);
        _metrics = options.Metrics;
    }

    /// <summary>
    /// Opens or creates a <see cref="LsmStore{TKey}"/> at <see cref="LsmStoreOptions{TKey}.DataDirectory"/>,
    /// replaying any WAL records not yet reflected in the SST files.
    /// </summary>
    internal static async ValueTask<LsmStore<TKey>> OpenAsync(LsmStoreOptions<TKey> options, IAsyncEnumerable<ReadOnlyMemory<byte>> walRecords, CancellationToken cancellationToken) {
        if (options.CapacityBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), $"{nameof(LsmStoreOptions<>.CapacityBytes)} must be positive.");

        var store = new LsmStore<TKey>(options, LoadSstLevels(options.DataDirectory));
        await store.RecoverAsync(walRecords, cancellationToken);
        return store;
    }

    /// <summary>
    /// Scans <paramref name="directory"/> for <c>*.sst</c> files (ordered by name) and
    /// organises them into a per-level list. Guarantees at least one (empty) L1 entry so
    /// the rest of the store can always index into level 0 without a bounds check.
    /// </summary>
    static List<List<SstFileInfo>> LoadSstLevels(string directory) {
        var levels = new List<List<SstFileInfo>>();
        foreach (var path in Directory.EnumerateFiles(directory, "*.sst").OrderBy(f => f)) {
            var file = SstFileInfo.Parse(path, new FileInfo(path).Length);
            AddFileToLevel(levels, file);
        }
        if (levels.Count == 0)
            levels.Add(new List<SstFileInfo>());
        return levels;
    }

    /// <summary>
    /// Appends <paramref name="file"/> to its level's list inside <paramref name="levels"/>,
    /// growing the outer list with empty buckets as needed.
    /// </summary>
    static void AddFileToLevel(List<List<SstFileInfo>> levels, SstFileInfo file) {
        var levelIndex = file.Level - 1; // L1 → index 0, L2 → index 1, …
        while (levels.Count <= levelIndex)
            levels.Add(new List<SstFileInfo>());
        levels[levelIndex].Add(file);
    }

    /// <summary>
    /// Returns the paths of all SST files and their bloom filter sidecars currently
    /// tracked by this store. Safe to call while holding the caller's write lock.
    /// </summary>
    internal IReadOnlyList<string> GetActiveFilePaths() {
        var paths = new List<string>();
        foreach (var level in _sstLevels)
            foreach (var file in level) {
                paths.Add(file.FilePath);
                if (File.Exists(file.BloomFilterPath))
                    paths.Add(file.BloomFilterPath);
            }
        return paths;
    }

    /// <inheritdoc />
    public void Dispose() {
        _memTable.Dispose();
        _metrics?.Dispose();
    }
}
