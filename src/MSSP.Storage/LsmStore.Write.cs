namespace MSSP.Storage;

public sealed partial class LsmStore<TKey> {
    /// <summary>
    /// Applies <paramref name="key"/> and <paramref name="value"/> directly to the MemTable,
    /// flushing first if the table is full.
    /// </summary>
    public async ValueTask WriteAsync(TKey key, Memory<byte> value, CancellationToken cancellationToken) {
        ReadOnlyMemory<byte> keyBytes = key;
        var entrySize = keyBytes.Length + value.Length;

        if (entrySize > _capacityBytes)
            throw new InvalidOperationException("Single event exceeds MemTable capacity.");

        if (_memTable.Size + entrySize > _capacityBytes)
            await FlushAsync(cancellationToken);

        ReadOnlyMemory<byte> bytes = WalRecord.From(key, value);
        _memTable.ApplyRecord(bytes);
    }

    async ValueTask FlushAsync(CancellationToken cancellationToken) {
        var sstPath = Path.Combine(
            _dataDirectory,
            $"{DateTimeOffset.UtcNow.Ticks:D19}_L1.sst");

        await _sst.WriteAsync(_memTable, sstPath, cancellationToken);

        // Use actual file size (more accurate than _memTable.Size,
        // which is the in-memory byte sum and differs due to SST-encoding overhead).
        var fileSize = new FileInfo(sstPath).Length;
        _sstLevels[0].Add(new SstFileInfo(sstPath, 1, fileSize));

        await _onFlushed(cancellationToken);
        _memTable = new MemTable<TKey>(_capacityBytes);

        await CompactAsync(cancellationToken);
    }
}
