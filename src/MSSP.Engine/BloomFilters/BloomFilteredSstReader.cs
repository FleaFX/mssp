using MSSP.Engine.Storage;

namespace MSSP.Engine.BloomFilters;

/// <summary>
/// Decorator over <see cref="ISstReader{TKey}"/> that skips disk I/O for point lookups
/// when the bloom filter can definitively rule out a key's presence.
/// </summary>
sealed class BloomFilteredSstReader<TKey> : ISstReader<TKey> where TKey : IKey<TKey> {
    readonly ISstReader<TKey> _inner;
    readonly BloomFilter _filter;

    internal BloomFilteredSstReader(ISstReader<TKey> inner, BloomFilter filter) {
        _inner = inner;
        _filter = filter;
    }

    /// <inheritdoc />
    public bool TryGet(TKey key, out ReadOnlyMemory<byte>? value) {
        ReadOnlyMemory<byte> keyBytes = key;
        value = null;

        return _filter.MayContain(keyBytes.Span) && _inner.TryGet(key, out value);
    }

    /// <inheritdoc />
    public IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> Scan() => _inner.Scan();

    /// <inheritdoc />
    public IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> Scan(TKey from) => _inner.Scan(from);

    /// <inheritdoc />
    public void Dispose() => _inner.Dispose();
}
