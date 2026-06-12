namespace MSSP.Storage;

/// <summary>
/// Abstraction over SST file I/O. Implement and decorate to add cross-cutting behaviour
/// (e.g. bloom filter sidecars) without coupling <see cref="LsmStore{TKey}"/> to it.
/// </summary>
/// <typeparam name="TKey">The type of the key.</typeparam>
public interface ISstAccess<TKey> where TKey : IKey<TKey> {
    /// <summary>
    /// Opens a reader for the SST file at <paramref name="sstPath"/>.
    /// </summary>
    ISstReader<TKey> OpenReader(string sstPath);

    /// <summary>
    /// Writes <paramref name="entries"/> as an immutable SST file at <paramref name="sstPath"/>.
    /// Implementations must guarantee that <paramref name="sstPath"/> is only visible after
    /// the write completes (e.g. via an atomic tmp-then-rename).
    /// </summary>
    ValueTask WriteAsync(IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> entries, string sstPath, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes the SST file at <paramref name="sstPath"/> and any associated sidecars.
    /// </summary>
    void Delete(string sstPath);
}
