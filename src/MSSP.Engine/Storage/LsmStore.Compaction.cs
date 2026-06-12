namespace MSSP.Engine.Storage;

public sealed partial class LsmStore<TKey> {
    /// <summary>
    /// Captures the state of a single compaction operation across its three phases.
    /// Created by <see cref="PlanCompaction"/>; driven by
    /// <see cref="CompactionJob.RunAsync"/> (I/O phase) and <see cref="CompactionJob.CompleteAsync"/> (commit phase).
    /// </summary>
    internal sealed class CompactionJob(LsmStore<TKey> store, int levelIndex, List<SstFileInfo> sourceFiles, string outputPath, OperationTimer timer) : IMaintenanceJob {
        SstFileInfo _merged;

        /// <summary>
        /// Merges all source SST files into a single output file. Safe to run off the actor thread;
        /// reads only the immutable source file snapshot, touches no shared state.
        /// </summary>
        public async ValueTask RunAsync(CancellationToken cancellationToken) {
            var readers = new List<ISstReader<TKey>>();
            try {
                readers.AddRange(sourceFiles.Select(file => store._sst.OpenReader(file.FilePath)));
                await store._sst.WriteAsync(
                    MergeAll(readers.Select(r => r.Scan())),
                    outputPath,
                    cancellationToken
                );
            } finally {
                foreach (var reader in readers)
                    reader.Dispose();
            }

            var nextLevel = levelIndex + 2; // levelIndex 0 = L1 → nextLevel 2 = L2
            _merged = new SstFileInfo(outputPath, nextLevel, new FileInfo(outputPath).Length);
        }

        /// <summary>
        /// Registers the compacted file, removes exactly the source files from the level, and deletes
        /// them from disk. Must be called on the actor thread.
        /// </summary>
        /// <remarks>
        /// Only the files captured at plan time are removed — not the entire level. A flush that added
        /// a new file to this level while the compaction ran must not be discarded (fixes P5).
        /// </remarks>
        internal ValueTask CompleteAsync(CancellationToken cancellationToken) {
            AddFileToLevel(store._sstLevels, _merged);
            
            foreach (var source in sourceFiles)
                store._sstLevels[levelIndex].Remove(source);

            foreach (var source in sourceFiles)
                store._sst.Delete(source.FilePath);

            store._metrics?.RecordCompaction($"L{levelIndex + 1}", timer.ElapsedMs, BuildLevelSnapshots(store._sstLevels));

            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// Finds the first SST level that exceeds its size target and returns a <see cref="CompactionJob"/>
    /// for it, or <see langword="null"/> if all levels are within their targets.
    /// </summary>
    /// <remarks>
    /// Captures the source file list as an immutable snapshot on the actor thread so that
    /// <see cref="CompactionJob.RunAsync"/> can run off-thread without racing with concurrent
    /// flushes that may add new files to the same level.
    /// </remarks>
    internal CompactionJob? PlanCompaction() {
        for (var i = 0; i < _sstLevels.Count; i++) {
            if (_sstLevels[i].Count == 0) continue;
            if (EstimateLevelSize(i) < GetLevelTarget(i)) continue;

            var timer = OperationTimer.Start();
            var nextLevel = i + 2;
            var outputPath = Path.Combine(_dataDirectory, $"{DateTimeOffset.UtcNow.Ticks:D19}_L{nextLevel}.sst");
            return new CompactionJob(this, i, _sstLevels[i].ToList(), outputPath, timer);
        }
        return null;
    }

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
