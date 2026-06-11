namespace MSSP.Storage;

public sealed partial class LsmStore<TKey> {
    /// <summary>
    /// Scans SST files then the MemTable, starting at <paramref name="from"/>.
    /// Levels are scanned from the highest (oldest data) down to L1, then the MemTable,
    /// so the combined output is in ascending key order. Safe to call under the write lock.
    /// </summary>
    public IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> ScanAllFrom(TKey from) {
        // Scan from highest level to lowest (Lmax -> ... -> L2 -> L1): oldest data first, ascending key order.
        // Files within each level are in insertion order (oldest flush first), which matches key order
        // in an append-only store.
        for (var levelIndex = _sstLevels.Count - 1; levelIndex >= 0; levelIndex--) {
            foreach (var file in _sstLevels[levelIndex]) {
                using var reader = _sst.OpenReader(file.FilePath);
                foreach (var entry in reader.Scan(from)) {
                    yield return entry;
                }
            }
        }
        foreach (var entry in _memTable.ScanFrom(from)) {
            yield return entry;
        }
    }

    /// <summary>
    /// Captures a snapshot of the current store state immediately, then yields lazily.
    /// Safe to iterate after releasing the write lock.
    /// </summary>
    public IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> ScanSnapshotFrom(TKey from) {
        var sstLevels = new List<List<SstFileInfo>>(_sstLevels.Count);
        for (var i = 0; i < _sstLevels.Count; i++) {
            sstLevels.Add(_sstLevels[i].ToList());
        }
        var memTable = _memTable;

        // Scan from highest level to lowest: oldest data first, ascending key order.
        for (var levelIndex = sstLevels.Count - 1; levelIndex >= 0; levelIndex--) {
            foreach (var file in sstLevels[levelIndex]) {
                using var reader = _sst.OpenReader(file.FilePath);
                foreach (var entry in reader.Scan(from)) {
                    yield return entry;
                }
            }
        }
        foreach (var entry in memTable.ScanFrom(from)) {
            yield return entry;
        }
    }

    /// <summary>
    /// Creates a handle-owning snapshot of the current store state.
    /// All SST readers are opened immediately with <c>FileShare.ReadWrite | FileShare.Delete</c>
    /// so compaction may unlink files while the snapshot is alive.
    /// Must be called on the engine actor thread (the sole owner of this store).
    /// </summary>
    /// <inheritdoc/>
    public LsmStoreSnapshot<TKey> TakeReadSnapshot() {
        var readers = new List<ISstReader<TKey>>();
        try {
            for (var i = _sstLevels.Count - 1; i >= 0; i--) {
                foreach (var file in _sstLevels[i]) {
                    var stream = new FileStream(file.FilePath, FileMode.Open, FileAccess.Read,
                        FileShare.ReadWrite | FileShare.Delete, bufferSize: 4096);
                    readers.Add(new SstReader<TKey>(stream));
                }
            }
            return new LsmStoreSnapshot<TKey>(readers, _memTable);
        } catch {
            foreach (var r in readers) r.Dispose();
            throw;
        }
    }
}
