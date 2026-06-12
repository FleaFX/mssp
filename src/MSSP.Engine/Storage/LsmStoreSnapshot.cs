namespace MSSP.Engine.Storage;

/// <summary>
/// A read-only snapshot of a <see cref="LsmStore{TKey}"/> at a point in time.
/// All SST file handles are opened eagerly with <c>FileShare.ReadWrite | FileShare.Delete</c>,
/// so compaction may unlink files while the snapshot is alive without affecting reads.
/// Dispose when iteration is complete.
/// </summary>
public sealed class LsmStoreSnapshot<TKey> : IDisposable where TKey : IKey<TKey> {
    readonly List<ISstReader<TKey>> _readers;
    readonly MemTable<TKey> _memTable;

    internal LsmStoreSnapshot(List<ISstReader<TKey>> readers, MemTable<TKey> memTable) {
        _readers = readers;
        _memTable = memTable;
    }

    /// <summary>
    /// Yields all entries in ascending key order starting from <paramref name="from"/>.
    /// Safe to iterate off the actor thread once the snapshot is constructed.
    /// </summary>
    public IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> ScanFrom(TKey from) {
        foreach (var reader in _readers)
            foreach (var entry in reader.Scan(from))
                yield return entry;
        foreach (var entry in _memTable.ScanFrom(from))
            yield return entry;
    }

    /// <inheritdoc/>
    public void Dispose() {
        foreach (var reader in _readers)
            reader.Dispose();
    }
}
