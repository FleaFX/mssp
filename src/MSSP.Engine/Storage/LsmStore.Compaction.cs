using MSSP.Engine;

namespace MSSP.Storage;

public sealed partial class LsmStore<TKey> {
    /// <summary>
    /// Triggers compaction starting at L1, cascading upwards until no level exceeds its size target.
    /// </summary>
    /// <remarks>
    /// <see cref="LsmStore{TKey}"/> is not thread-safe. The caller is responsible for ensuring
    /// no concurrent writes, flushes, or compactions are in progress.
    /// </remarks>
    internal async ValueTask CompactAsync(CancellationToken cancellationToken) {
        if (_sstLevels.Count > 0 && EstimateLevelSize(0) >= GetLevelTarget(0))
            await CompactLevelAsync(0, cancellationToken);
    }

    async ValueTask CompactLevelAsync(int levelIndex, CancellationToken cancellationToken) {
        var timer = OperationTimer.Start();
        var levelName = $"L{levelIndex + 1}";
        var readers = new List<ISstReader<TKey>>();
        try {
            // 1. Open readers for all files in this level.
            foreach (var file in _sstLevels[levelIndex])
                readers.Add(_sst.OpenReader(file.FilePath));

            // 2. Merge into a single file in the next level.
            var nextLevelName = levelIndex + 2; // L1 (index 0) → L2, L2 (index 1) → L3, …
            var compactedPath = Path.Combine(
                _dataDirectory,
                $"{DateTimeOffset.UtcNow.Ticks:D19}_L{nextLevelName}.sst");

            await _sst.WriteAsync(
                MergeAll(readers.Select(r => r.Scan())),
                compactedPath,
                cancellationToken);

            var newFileSize = new FileInfo(compactedPath).Length;

            // 3. Dispose readers BEFORE deleting — on Windows a file cannot be deleted
            // while an open FileStream holds a handle to it.
            foreach (var reader in readers) reader.Dispose();
            readers.Clear();

            // 4. Register the compacted file in the next level (grows the list if needed).
            var nextLevelIndex = levelIndex + 1;
            AddFileToLevel(_sstLevels, new SstFileInfo(compactedPath, nextLevelName, newFileSize));

            // 5. Delete source files (handles are already closed in step 3).
            foreach (var file in _sstLevels[levelIndex])
                _sst.Delete(file.FilePath);
            _sstLevels[levelIndex].Clear();

            // Record metrics after compaction
            if (_metrics is not null)
                _metrics.RecordCompaction(levelName, timer.ElapsedMs, BuildLevelSnapshots(_sstLevels));

            // 6. Cascade: check if the next level now exceeds its target.
            for (var nextIndex = nextLevelIndex; nextIndex < _sstLevels.Count; nextIndex++) {
                if (EstimateLevelSize(nextIndex) >= GetLevelTarget(nextIndex))
                    await CompactLevelAsync(nextIndex, cancellationToken);
            }
        } finally {
            // Safety net: readers.Clear() in step 3 makes this a no-op in the happy path.
            foreach (var reader in readers) reader.Dispose();
        }
    }

    /// <summary>
    /// Calculates the size target for a given level.
    /// Target = <c>BaseLevelSizeBytes × LevelSizeMultiplier^levelIndex</c>.
    /// Uses integer exponentiation to avoid the precision loss of <see cref="Math.Pow"/>.
    /// Returns <see cref="long.MaxValue"/> on overflow so the level is never considered full.
    /// </summary>
    long GetLevelTarget(int levelIndex) {
        try {
            checked {
                var result = _baseLevelSizeBytes;
                for (var i = 0; i < levelIndex; i++)
                    result *= _levelSizeMultiplier;
                return result;
            }
        } catch (OverflowException) {
            return long.MaxValue;
        }
    }

    long EstimateLevelSize(int levelIndex) => _sstLevels[levelIndex].Sum(f => f.SizeBytes);

    /// <summary>
    /// k-way merge of multiple sorted SST streams via a min-heap.
    /// Each stream must already be sorted in ascending key order.
    /// </summary>
    static IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> MergeAll(
        IEnumerable<IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>>> sources) {
        var pq = new PriorityQueue<(
            KeyValuePair<TKey, ReadOnlyMemory<byte>?> Entry,
            IEnumerator<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> Enumerator
        ), TKey>();

        foreach (var source in sources) {
            var enumerator = source.GetEnumerator();
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
}
