using System.Buffers.Binary;

namespace MSSP.LsmTree;

sealed class LsmStore<TKey> : IDisposable where TKey : IKey<TKey> {
    // Matches MemTable<TKey>.WriteMarker — both define the on-disk WAL record format.
    const byte WriteMarker = 0x01;

    readonly string _dataDirectory;
    readonly int _capacityBytes;
    readonly WalAppendDelegate _walAppend;
    readonly Func<CancellationToken, ValueTask> _onFlushed;
    readonly List<string> _sstFiles;
    MemTable<TKey> _memTable;

    LsmStore(string dataDirectory, int capacityBytes, List<string> sstFiles, WalAppendDelegate walAppend, Func<CancellationToken, ValueTask> onFlushed) {
        _dataDirectory = dataDirectory;
        _capacityBytes = capacityBytes;
        _walAppend = walAppend;
        _onFlushed = onFlushed;
        _sstFiles = sstFiles;
        _memTable = new MemTable<TKey>(capacityBytes, walAppend);
    }

    /// <summary>
    /// Opens or creates a <see cref="LsmStore{TKey}"/> at <see cref="LsmStoreOptions.DataDirectory"/>,
    /// then replays any WAL records not yet reflected in the SST files.
    /// </summary>
    internal static async ValueTask<LsmStore<TKey>> OpenAsync(LsmStoreOptions options, IAsyncEnumerable<ReadOnlyMemory<byte>> walRecords, CancellationToken ct) {
        var sstFiles = Directory.EnumerateFiles(options.DataDirectory, "*.sst").OrderBy(f => f).ToList();
        var store = new LsmStore<TKey>(options.DataDirectory, options.CapacityBytes, sstFiles, options.WalAppend, options.OnFlushed);
        await store.RecoverAsync(walRecords, ct);
        return store;
    }

    /// <summary>
    /// Writes a key-value pair to the MemTable, flushing to an SST file first if the MemTable is full.
    /// </summary>
    internal async ValueTask WriteAsync(TKey key, ReadOnlyMemory<byte> value, CancellationToken ct) {
        ReadOnlyMemory<byte> keyBytes = key;
        var entrySize = keyBytes.Length + value.Length;

        if (entrySize > _capacityBytes)
            throw new InvalidOperationException("Single event exceeds MemTable capacity.");

        if (_memTable.Size + entrySize > _capacityBytes)
            await FlushAsync(ct);

        if (!await _memTable.TryWriteAsync(key, value, ct))
            throw new InvalidOperationException("WAL append failed.");
    }

    /// <summary>
    /// Scans SST files then the MemTable, starting at <paramref name="from"/>. Safe to call under the write lock.
    /// </summary>
    internal IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> ScanAllFrom(TKey from) {
        foreach (var sstPath in _sstFiles) {
            using var stream = new FileStream(sstPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096);
            foreach (var entry in new SstReader<TKey>(stream).Scan(from))
                yield return entry;
        }
        foreach (var entry in _memTable.ScanFrom(from))
            yield return entry;
    }

    /// <summary>
    /// Captures a snapshot of the current store state immediately, then yields lazily.
    /// Safe to iterate after releasing the write lock.
    /// </summary>
    internal IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> ScanSnapshotFrom(TKey from) {
        var sstFiles = _sstFiles.ToArray();
        var memTable = _memTable;

        foreach (var sstPath in sstFiles) {
            using var stream = new FileStream(sstPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096);
            foreach (var entry in new SstReader<TKey>(stream).Scan(from))
                yield return entry;
        }

        foreach (var entry in memTable.ScanFrom(from))
            yield return entry;
    }

    /// <summary>
    /// Replays WAL records into the MemTable, skipping any entries already present in SST files.
    /// </summary>
    internal async ValueTask RecoverAsync(IAsyncEnumerable<ReadOnlyMemory<byte>> walRecords, CancellationToken ct) {
        var sstKeys = new HashSet<TKey>();
        foreach (var sstPath in _sstFiles) {
            await using var stream = new FileStream(sstPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096);
            foreach (var (key, _) in new SstReader<TKey>(stream).Scan())
                sstKeys.Add(key);
        }

        await foreach (var bytes in walRecords.WithCancellation(ct)) {
            var span = bytes.Span;
            if (span.Length < 5 || span[0] != WriteMarker) continue;
            TKey key = bytes.Slice(5, BinaryPrimitives.ReadInt32LittleEndian(span[1..]));
            if (!sstKeys.Contains(key))
                _memTable.ApplyRecord(bytes);
        }
    }

    async ValueTask FlushAsync(CancellationToken ct) {
        var sstPath = Path.Combine(_dataDirectory, $"{DateTimeOffset.UtcNow.Ticks:D19}.sst");
        await using var sstStream = new FileStream(sstPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous);
        await SstWriter.WriteAsync<TKey>(_memTable, sstStream, cancellationToken: ct);
        _sstFiles.Add(sstPath);

        await _onFlushed(ct);
        _memTable = new MemTable<TKey>(_capacityBytes, _walAppend);
    }

    /// <inheritdoc />
    public void Dispose() => _memTable.Dispose();
}
