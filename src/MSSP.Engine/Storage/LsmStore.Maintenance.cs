namespace MSSP.Engine.Storage;

public sealed partial class LsmStore<TKey> {
    /// <summary>
    /// Captures the state of a single flush operation across its three phases.
    /// Created by <see cref="LsmStore{TKey}.TryBeginFlushAsync"/> or <see cref="LsmStore{TKey}.BeginFlushAsync"/>; driven by
    /// <see cref="FlushJob.RunAsync"/> (I/O phase) and <see cref="FlushJob.CompleteAsync"/> (commit phase).
    /// </summary>
    internal sealed class FlushJob(LsmStore<TKey> store, MemTable<TKey> sealedMemTable, string path, OperationTimer timer) : IMaintenanceJob {
        SstFileInfo _file;

        /// <summary>
        /// Writes the sealed MemTable to disk. Safe to run off the actor thread; touches no shared state.
        /// </summary>
        public async ValueTask RunAsync(CancellationToken cancellationToken) {
            await store._sst.WriteAsync(sealedMemTable, path, cancellationToken);
            var fileSize = new FileInfo(path).Length;
            _file = new SstFileInfo(path, 1, fileSize);
        }

        /// <summary>
        /// Registers the new SST file and removes the sealed MemTable from the flushing list.
        /// Must be called on the actor thread.
        /// </summary>
        /// <remarks>
        /// The sealed MemTable is intentionally not disposed here: a read snapshot captured before this
        /// completion may still reference it and iterate it off-thread. Disposing would tear down the
        /// SkipList's lock underneath that reader. The MemTable holds only managed state, so it is left
        /// for the GC to reclaim once both <see cref="_flushing"/> and any snapshots release it.
        /// Compaction is not triggered here — the engine calls <c>MaybeStartCompaction</c> after
        /// receiving <c>FlushCompleted</c>.
        /// </remarks>
        internal ValueTask CompleteAsync(CancellationToken cancellationToken) {
            store._sstLevels[0].Add(_file);
            store._flushing.Remove(sealedMemTable);
            store._metrics?.RecordFlush(timer.ElapsedMs, memTableSize: 0, BuildLevelSnapshots(store._sstLevels));
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Deletes the output SST file written by <see cref="RunAsync"/>.
        /// Called when the flush is discarded after a reload (epoch mismatch).
        /// </summary>
        internal void Abandon() => store._sst.Delete(path);
    }

    /// <summary>
    /// Seals the live MemTable and returns a <see cref="FlushJob"/> if <paramref name="entrySize"/>
    /// would cause an overflow; returns <see langword="null"/> if there is still room.
    /// </summary>
    internal ValueTask<FlushJob?> TryBeginFlushAsync(int entrySize, CancellationToken cancellationToken) =>
        _memTable.Size + entrySize > _capacityBytes
            ? BeginFlushAsync(cancellationToken)
            : ValueTask.FromResult<FlushJob?>(null!);

    internal async ValueTask<FlushJob?> BeginFlushAsync(CancellationToken cancellationToken) {
        var timer = OperationTimer.Start();
        var path = Path.Combine(_dataDirectory, $"{DateTimeOffset.UtcNow.Ticks:D19}_L1.sst");
        var @sealed = _memTable;
        
        _flushing.Add(@sealed);
        await _onFlushed(cancellationToken);

        _memTable = new MemTable<TKey>(_capacityBytes);

        return new FlushJob(this, @sealed, path, timer);
    }
}
