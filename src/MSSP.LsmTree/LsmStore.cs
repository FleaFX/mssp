using System.Buffers.Binary;

namespace MSSP.LsmTree;

/// <summary>
/// Invoked after each MemTable flush, e.g. to rotate the WAL so flushed records are no longer replayed on recovery.
/// </summary>
/// <param name="cancellationToken">Token to cancel the callback.</param>
delegate ValueTask MemTableFlushedDelegate(CancellationToken cancellationToken);

sealed class LsmStore<TKey> : IDisposable where TKey : IKey<TKey> {
    // Both constants match MemTable<TKey> — they share the on-disk WAL record format.
    const byte WriteMarker = 0x01;
    const byte TombstoneMarker = 0x02;

    readonly string _dataDirectory;
    readonly int _capacityBytes;
    readonly int _compactionThreshold;
    readonly WalAppendDelegate _walAppend;
    readonly MemTableFlushedDelegate _onFlushed;
    readonly List<string> _sstFiles;
    MemTable<TKey> _memTable;

    LsmStore(string dataDirectory, int capacityBytes, int compactionThreshold, List<string> sstFiles, WalAppendDelegate walAppend, MemTableFlushedDelegate onFlushed) {
        _dataDirectory = dataDirectory;
        _capacityBytes = capacityBytes;
        _compactionThreshold = compactionThreshold;
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
        if (options.CapacityBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), $"{nameof(LsmStoreOptions.CapacityBytes)} must be positive.");

        var sstFiles = Directory.EnumerateFiles(options.DataDirectory, "*.sst").OrderBy(f => f).ToList();
        var store = new LsmStore<TKey>(options.DataDirectory, options.CapacityBytes, options.CompactionThreshold, sstFiles, options.WalAppend, options.OnFlushed);
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
            using var reader = new SstReader<TKey>(new FileStream(sstPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096));
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
    internal IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> ScanSnapshotFrom(TKey from) {
        var sstFiles = _sstFiles.ToArray();
        var memTable = _memTable;

        foreach (var sstPath in sstFiles) {
            using var reader = new SstReader<TKey>(new FileStream(sstPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096));
            foreach (var entry in reader.Scan(from))
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
            using var reader = new SstReader<TKey>(new FileStream(sstPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096));
            foreach (var (key, _) in reader.Scan())
                sstKeys.Add(key);
        }

        await foreach (var bytes in walRecords.WithCancellation(ct)) {
            var span = bytes.Span;
            if (span.Length < 5) continue;
            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(span[1..]);
            if (keyLen < 0 || 5 + keyLen > span.Length) continue;

            if (span[0] == TombstoneMarker) {
                // Deletes that occurred after the last flush are not in any SST; always replay.
                _memTable.ApplyRecord(bytes);
                continue;
            }

            if (span[0] != WriteMarker) continue;
            TKey key = bytes.Slice(5, keyLen);
            if (!sstKeys.Contains(key))
                _memTable.ApplyRecord(bytes);
        }
    }

    async ValueTask FlushAsync(CancellationToken ct) {
        var tmpPath = Path.Combine(_dataDirectory, $"{DateTimeOffset.UtcNow.Ticks:D19}.sst.tmp");
        {
            await using var sstStream = new FileStream(tmpPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous);
            await SstWriter.WriteAsync(_memTable, sstStream, cancellationToken: ct);
        }
        var sstPath = Path.ChangeExtension(tmpPath, ".sst");
        File.Move(tmpPath, sstPath);
        _sstFiles.Add(sstPath);

        await _onFlushed(ct);
        _memTable = new MemTable<TKey>(_capacityBytes, _walAppend);

        if (_sstFiles.Count >= _compactionThreshold)
            await CompactAsync(ct);
    }

    /// <summary>
    /// Merges all SST files into a single new SST file and removes the originals.
    /// Writes to a <c>.sst.tmp</c> file first; the subsequent rename ensures that a crash
    /// or cancellation mid-compaction leaves the original files intact.
    /// </summary>
    internal async ValueTask CompactAsync(CancellationToken ct) {
        if (_sstFiles.Count < 2) return;

        var readers = new List<SstReader<TKey>>(_sstFiles.Count);
        try {
            foreach (var path in _sstFiles)
                readers.Add(new SstReader<TKey>(new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096)));

            var tmpPath = Path.Combine(_dataDirectory, $"{DateTimeOffset.UtcNow.Ticks:D19}.sst.tmp");
            {
                await using var tmpStream = new FileStream(tmpPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous);
                await SstWriter.WriteAsync(MergeAll(readers), tmpStream, cancellationToken: ct);
            }

            // Release file handles before renaming and deleting the source files.
            foreach (var reader in readers) reader.Dispose();
            readers.Clear();

            var compactedPath = Path.ChangeExtension(tmpPath, ".sst");
            File.Move(tmpPath, compactedPath);

            var oldPaths = _sstFiles.ToList();
            _sstFiles.Clear();
            _sstFiles.Add(compactedPath);

            foreach (var path in oldPaths)
                File.Delete(path);
        } finally {
            foreach (var reader in readers)
                reader.Dispose();
        }
    }

    // K-way merge of pre-sorted SST readers using a min-heap. Keys are unique across files
    // (event store invariant: (streamId, revision) pairs are immutable), so no deduplication needed.
    static IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> MergeAll(List<SstReader<TKey>> readers) {
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
            // Dispose enumerators that remain queued if iteration was abandoned early.
            while (pq.TryDequeue(out var item, out _))
                item.Enumerator.Dispose();
        }
    }

    /// <inheritdoc />
    public void Dispose() => _memTable.Dispose();
}
