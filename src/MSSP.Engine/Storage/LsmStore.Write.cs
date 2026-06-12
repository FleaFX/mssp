namespace MSSP.Engine.Storage;

public sealed partial class LsmStore<TKey> {
    /// <summary>
    /// Applies <paramref name="key"/> and <paramref name="value"/> directly to the live MemTable.
    /// The caller is responsible for calling <see cref="TryBeginFlush"/> and completing the flush first if needed.
    /// </summary>
    public ValueTask WriteAsync(TKey key, Memory<byte> value, CancellationToken cancellationToken) {
        ReadOnlyMemory<byte> keyBytes = key;
        var entrySize = keyBytes.Length + value.Length;

        if (entrySize > _capacityBytes)
            throw new InvalidOperationException("Single event exceeds MemTable capacity.");

        ReadOnlyMemory<byte> bytes = WalRecord.From(key, value);
        _memTable.ApplyRecord(bytes);
        _metrics?.UpdateMemTableSize(_memTable.Size);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Builds snapshots of all SST levels for metrics reporting.
    /// </summary>
    internal static LsmStoreMetrics.LevelSnapshot[] BuildLevelSnapshots(List<List<SstFileInfo>> levels) =>
        levels.Select((files, i) => new LsmStoreMetrics.LevelSnapshot(
            LevelName: $"L{i + 1}",
            FileCount: files.Count,
            TotalBytes: files.Sum(f => f.SizeBytes)
        )).ToArray();
}
