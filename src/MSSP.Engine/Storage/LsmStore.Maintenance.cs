namespace MSSP.Engine.Storage;

public sealed partial class LsmStore<TKey> {
    /// <summary>
    /// Captures the state of a single flush operation across its three phases.
    /// Created by <see cref="TryBeginFlush"/> or <see cref="BeginFlush"/>; driven by
    /// <see cref="FlushJob.RunAsync"/> (I/O phase) and <see cref="FlushJob.CompleteAsync"/> (commit phase).
    /// </summary>
    internal sealed class FlushJob(LsmStore<TKey> store, MemTable<TKey> sealedMemTable, string path, OperationTimer timer) {
        SstFileInfo _file;

        /// <summary>
        /// Writes the sealed MemTable to disk. Safe to run off the actor thread; touches no shared state.
        /// </summary>
        internal async ValueTask RunAsync(CancellationToken cancellationToken) {
            await store._sst.WriteAsync(sealedMemTable, path, cancellationToken);
            var fileSize = new FileInfo(path).Length;
            _file = new SstFileInfo(path, 1, fileSize);
        }

        /// <summary>
        /// Registers the new SST file, removes the sealed MemTable from the flushing list, and triggers compaction.
        /// Must be called on the actor thread.
        /// </summary>
        internal async ValueTask CompleteAsync(CancellationToken cancellationToken) {
            store._sstLevels[0].Add(_file);
            store._flushing.Remove(sealedMemTable);
            sealedMemTable.Dispose();
            await store._onFlushed(cancellationToken);
            store._metrics?.RecordFlush(timer.ElapsedMs, memTableSize: 0, BuildLevelSnapshots(store._sstLevels));
            await store.CompactAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Seals the live MemTable and returns a <see cref="FlushJob"/> if <paramref name="entrySize"/>
    /// would cause an overflow; returns <see langword="null"/> if there is still room.
    /// </summary>
    internal FlushJob? TryBeginFlush(int entrySize) =>
        _memTable.Size + entrySize > _capacityBytes
            ? BeginFlush()
            : null;

    internal FlushJob BeginFlush() {
        var timer = OperationTimer.Start();
        var path = Path.Combine(_dataDirectory, $"{DateTimeOffset.UtcNow.Ticks:D19}_L1.sst");
        var @sealed = _memTable;

        _flushing.Add(@sealed);
        _memTable = new MemTable<TKey>(_capacityBytes);

        return new FlushJob(this, @sealed, path, timer);
    }
}
