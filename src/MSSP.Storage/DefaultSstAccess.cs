namespace MSSP.Storage;

/// <summary>
/// Default <see cref="ISstAccess{TKey}"/> that reads and writes SST files directly on the local filesystem.
/// Writes are atomic: entries are first written to a <c>.tmp</c> file, then renamed to the final path.
/// </summary>
sealed class DefaultSstAccess<TKey> : ISstAccess<TKey> where TKey : IKey<TKey> {
    internal static readonly DefaultSstAccess<TKey> Instance = new();

    /// <inheritdoc />
    public ISstReader<TKey> OpenReader(string sstPath) =>
        new SstReader<TKey>(new FileStream(sstPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096));

    /// <inheritdoc />
    public async ValueTask WriteAsync(IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> entries, string sstPath, CancellationToken ct) {
        var tmpPath = sstPath + ".tmp";
        {
            await using var stream = new FileStream(tmpPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous);
            await SstWriter.WriteAsync(entries, stream, cancellationToken: ct);
        }
        File.Move(tmpPath, sstPath);
    }

    /// <inheritdoc />
    public void Delete(string sstPath) => File.Delete(sstPath);
}
