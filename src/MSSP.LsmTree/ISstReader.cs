using System.Diagnostics.CodeAnalysis;

namespace MSSP.LsmTree;

/// <summary>
/// Read-only view over a single SST file.
/// </summary>
/// <typeparam name="TKey">The type of the key.</typeparam>
public interface ISstReader<TKey> : IDisposable where TKey : IKey<TKey> {
    /// <summary>
    /// Attempts to retrieve the value for <paramref name="key"/>.
    /// </summary>
    bool TryGet(TKey key, [MaybeNullWhen(false)] out ReadOnlyMemory<byte>? value);

    /// <summary>
    /// Returns all entries in ascending key order.
    /// </summary>
    IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> Scan();

    /// <summary>
    /// Returns entries in ascending key order starting from the first key greater than or equal to <paramref name="from"/>.
    /// </summary>
    IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> Scan(TKey from);
}
