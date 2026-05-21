using System.Buffers.Binary;

namespace MSSP.Storage;

public sealed class LsmStore<TKey> : ILsmStore<TKey> where TKey : IKey<TKey> {
    readonly string _dataDirectory;
    readonly int _capacityBytes;
    readonly int _compactionThreshold;
    readonly MemTableFlushedDelegate _onFlushed;
    readonly ISstAccess<TKey> _sst;
    readonly List<string> _sstFiles;
    MemTable<TKey> _memTable;

    LsmStore(string dataDirectory, int capacityBytes, int compactionThreshold, List<string> sstFiles, MemTableFlushedDelegate onFlushed, ISstAccess<TKey> sst) {
        _dataDirectory = dataDirectory;
        _capacityBytes = capacityBytes;
        _compactionThreshold = compactionThreshold;
        _onFlushed = onFlushed;
        _sst = sst;
        _sstFiles = sstFiles;
        _memTable = new MemTable<TKey>(capacityBytes);
    }

    /// <summary>
    /// Opens or creates a <see cref="LsmStore{TKey}"/> at <see cref="LsmStoreOptions{TKey}.DataDirectory"/>,
    /// replaying any WAL records not yet reflected in the SST files.
    /// </summary>
    internal static async ValueTask<LsmStore<TKey>> OpenAsync(LsmStoreOptions<TKey> options, IAsyncEnumerable<ReadOnlyMemory<byte>> walRecords, CancellationToken cancellationToken) {
        if (options.CapacityBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), $"{nameof(LsmStoreOptions<TKey>.CapacityBytes)} must be positive.");

        var sstFiles = Directory.EnumerateFiles(options.DataDirectory, "*.sst").OrderBy(f => f).ToList();
        var sst = options.SstAccess ?? DefaultSstAccess<TKey>.Instance;
        var store = new LsmStore<TKey>(options.DataDirectory, options.CapacityBytes, options.CompactionThreshold, sstFiles, options.OnFlushed, sst);
        await store.RecoverAsync(walRecords, cancellationToken);
        return store;
    }

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

    /// <summary>
    /// Scans SST files then the MemTable, starting at <paramref name="from"/>. Safe to call under the write lock.
    /// </summary>
    public IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> ScanAllFrom(TKey from) {
        foreach (var sstPath in _sstFiles) {
            using var reader = _sst.OpenReader(sstPath);
            foreach (var entry in reader.Scan(from))
                yield return entry;
        }
        foreach (var entry in _memTable.ScanFrom(from))
            yield return entry;
    }

    /// <summary>
    /// Captures a snapshot of the current store state immediately, then yields lazily.
    /// Safe to iterate after releasing the write lock.
    /// </summary>
    public IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> ScanSnapshotFrom(TKey from) {
        var sstFiles = _sstFiles.ToArray();
        var memTable = _memTable;

        foreach (var sstPath in sstFiles) {
            using var reader = _sst.OpenReader(sstPath);
            foreach (var entry in reader.Scan(from))
                yield return entry;
        }

        foreach (var entry in memTable.ScanFrom(from))
            yield return entry;
    }

    /// <summary>
    /// Replays WAL records into the MemTable, skipping any entries already present in SST files.
    /// </summary>
    internal async ValueTask RecoverAsync(IAsyncEnumerable<ReadOnlyMemory<byte>> walRecords, CancellationToken cancellationToken) {
        var sstKeys = new HashSet<TKey>();
        foreach (var sstPath in _sstFiles) {
            using var reader = _sst.OpenReader(sstPath);
            foreach (var (key, _) in reader.Scan())
                sstKeys.Add(key);
        }

        await foreach (var bytes in walRecords.WithCancellation(cancellationToken)) {
            var span = bytes.Span;
            if (span.Length < 5) continue;
            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(span[1..]);
            if (keyLen < 0 || 5 + keyLen > span.Length) continue;

            if (span[0] == WalRecord.TombstoneMarker) {
                _memTable.ApplyRecord(bytes);
                continue;
            }

            if (span[0] != WalRecord.WriteMarker) continue;
            TKey key = bytes.Slice(5, keyLen);
            if (!sstKeys.Contains(key))
                _memTable.ApplyRecord(bytes);
        }
    }

    async ValueTask FlushAsync(CancellationToken cancellationToken) {
        var sstPath = Path.Combine(_dataDirectory, $"{DateTimeOffset.UtcNow.Ticks:D19}.sst");
        await _sst.WriteAsync(_memTable, sstPath, cancellationToken);
        _sstFiles.Add(sstPath);

        await _onFlushed(cancellationToken);
        _memTable = new MemTable<TKey>(_capacityBytes);

        if (_sstFiles.Count >= _compactionThreshold)
            await CompactAsync(cancellationToken);
    }

    /// <summary>
    /// Merges all SST files into a single new SST file and removes the originals.
    /// </summary>
    internal async ValueTask CompactAsync(CancellationToken cancellationToken) {
        if (_sstFiles.Count < 2) return;

        var readers = new List<ISstReader<TKey>>(_sstFiles.Count);
        try {
            foreach (var path in _sstFiles)
                readers.Add(_sst.OpenReader(path));

            var compactedPath = Path.Combine(_dataDirectory, $"{DateTimeOffset.UtcNow.Ticks:D19}.sst");
            await _sst.WriteAsync(MergeAll(readers), compactedPath, cancellationToken);

            foreach (var reader in readers) reader.Dispose();
            readers.Clear();

            var oldPaths = _sstFiles.ToList();
            _sstFiles.Clear();
            _sstFiles.Add(compactedPath);

            foreach (var path in oldPaths)
                _sst.Delete(path);
        } finally {
            foreach (var reader in readers)
                reader.Dispose();
        }
    }

    static IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> MergeAll(List<ISstReader<TKey>> readers) {
        var pq = new PriorityQueue<(KeyValuePair<TKey, ReadOnlyMemory<byte>?> Entry, IEnumerator<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> Enumerator), TKey>();

        foreach (var reader in readers) {
            var enumerator = reader.Scan().GetEnumerator();
            if (enumerator.MoveNext())
                pq.Enqueue((enumerator.Current, enumerator), enumerator.Current.Key);
            else
                enumerator.Dispose();
        }

        try {
            while (pq.Count > 0) {
                var (entry, enumerator) = pq.Dequeue();
                yield return entry;
                if (enumerator.MoveNext())
                    pq.Enqueue((enumerator.Current, enumerator), enumerator.Current.Key);
                else
                    enumerator.Dispose();
            }
        } finally {
            while (pq.TryDequeue(out var item, out _))
                item.Enumerator.Dispose();
        }
    }

    /// <inheritdoc />
    public void Dispose() => _memTable.Dispose();
}
