namespace MSSP.Storage;

/// <summary>
/// Abstraction over a key-value store that supports append writes and forward scans.
/// </summary>
public interface ILsmStore<TKey> : IDisposable {
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
    /// Flushes any buffered writes to durable storage.
    /// Called by the apply loop after processing a batch so that all writes in the batch
    /// become durable before their callers are notified. The default implementation is a
    /// no-op; stores that buffer writes (e.g. the subscription pipeline) override this.
    /// </summary>
    ValueTask FlushAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
}
