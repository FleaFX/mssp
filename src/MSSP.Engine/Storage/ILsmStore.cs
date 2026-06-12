namespace MSSP.Engine.Storage;

/// <summary>
/// Abstraction over a key-value store that supports append writes and forward scans.
/// </summary>
public interface ILsmStore<TKey> : IDisposable where TKey : IKey<TKey> {
    /// <summary>
    /// Appends a value for <paramref name="key"/>, waiting until the write is durable.
    /// The caller must not modify <paramref name="value"/> after this call returns.
    /// </summary>
    ValueTask WriteAsync(TKey key, Memory<byte> value, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns a snapshot-isolated forward scan starting at <paramref name="from"/>.
    /// The returned enumerable may be iterated outside any lock.
    /// </summary>
    IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> ScanSnapshotFrom(TKey from);

    /// <summary>
    /// Returns a live forward scan starting at <paramref name="from"/>, including entries
    /// written after the scan started.
    /// </summary>
    IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> ScanAllFrom(TKey from);

    /// <summary>
    /// Creates a handle-owning snapshot of the current store state for off-thread iteration.
    /// All SST file handles are opened immediately; the caller is responsible for disposing the snapshot.
    /// Must be called while the store is not being mutated concurrently.
    /// </summary>
    LsmStoreSnapshot<TKey> TakeReadSnapshot();

    /// <summary>
    /// Opens a raw file stream for each active SST file, suitable for streaming into a backup archive.
    /// Must be called while the store is not being mutated concurrently.
    /// The caller is responsible for disposing each returned stream.
    /// </summary>
    IReadOnlyList<FileStream> OpenBackupStreams();

    /// <summary>
    /// Flushes any buffered writes to durable storage.
    /// Called by the apply loop after processing a batch so that all writes in the batch
    /// become durable before their callers are notified. The default implementation is a
    /// no-op; stores that buffer writes override this.
    /// </summary>
    ValueTask FlushAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;

    /// <summary>
    /// Replaces the store contents with a snapshot from <paramref name="stagingDirectory"/>,
    /// discarding all current SST files and resetting the MemTable.
    /// The caller is responsible for ensuring no concurrent reads or writes are in progress.
    /// </summary>
    ValueTask ReloadAsync(string stagingDirectory, CancellationToken cancellationToken = default);
}
