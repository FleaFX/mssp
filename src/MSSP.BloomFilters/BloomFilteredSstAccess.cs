using MSSP.Storage;

namespace MSSP.BloomFilters;

/// <summary>
/// Decorator over <see cref="ISstAccess{TKey}"/> that maintains a bloom filter sidecar
/// (<c>.bf</c>) alongside each SST file to accelerate point lookups.
/// </summary>
/// <remarks>
/// When a <c>.bf</c> sidecar is absent or unreadable (e.g. for SST files written before
/// bloom filters were enabled), the decorator falls back to the unfiltered inner reader.
/// </remarks>
public sealed class BloomFilteredSstAccess<TKey>(ISstAccess<TKey> inner) : ISstAccess<TKey> where TKey : IKey<TKey> {
    /// <inheritdoc/>
    public ISstReader<TKey> OpenReader(string sstPath) {
        var reader = inner.OpenReader(sstPath);
        var bfPath = new BloomFilterPath(sstPath);
        if (!File.Exists(bfPath)) return reader;
        try {
            using var stream = new FileStream(bfPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            return new BloomFilteredSstReader<TKey>(reader, BloomFilter.ReadFrom(stream));
        } catch (Exception e) when (e is IOException or InvalidDataException) {
            // Corrupt or unreadable sidecar — fall back to unfiltered reader.
            return reader;
        }
    }

    /// <inheritdoc/>
    public async ValueTask WriteAsync(IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> entries, string sstPath, CancellationToken cancellationToken) {
        var keys = new List<ReadOnlyMemory<byte>>();
        await inner.WriteAsync(
            entries.Select(kv => { keys.Add(kv.Key); return kv; }),
            sstPath, cancellationToken);
        await WriteSidecarAsync(sstPath, keys);
    }

    /// <inheritdoc/>
    public void Delete(string sstPath) {
        var bfPath = new BloomFilterPath(sstPath);
        if (File.Exists(bfPath)) File.Delete(bfPath);
        inner.Delete(sstPath);
    }

    static async ValueTask WriteSidecarAsync(string sstPath, List<ReadOnlyMemory<byte>> keys) {
        var filter = BloomFilter.Create(Math.Max(1, keys.Count));
        foreach (var key in keys)
            filter.Add(key.Span);
        var bfPath = new BloomFilterPath(sstPath);
        var tmpPath = bfPath + ".tmp";
        {
            await using var stream = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous);
            filter.WriteTo(stream);
        }
        File.Move(tmpPath, bfPath, overwrite: true);
    }
}
