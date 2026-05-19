using System.Buffers.Binary;
using System.Collections;

namespace MSSP.LsmTree;

/// <summary>
/// The Level 0 component of the LSM tree. Buffers writes in an ordered in-memory
/// skip list. Mutations are applied via <see cref="ApplyRecord"/> once a record has
/// been committed to the write-ahead log. Callers should flush to an SST file when
/// <see cref="IsFull"/> is true.
/// </summary>
/// <typeparam name="TKey">The type of the key.</typeparam>
sealed class MemTable<TKey>(int capacityBytes) :
    IDisposable, IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>>
    where TKey : IKey<TKey> {

    readonly SkipList<TKey, Entry> _data = new();
    long _size;

    /// <summary>
    /// Gets a value indicating whether the accumulated byte size has reached or exceeded the configured capacity.
    /// </summary>
    public bool IsFull => Interlocked.Read(ref _size) >= capacityBytes;

    /// <summary>
    /// Gets the number of entries currently in the table, including tombstones.
    /// </summary>
    public int Count => _data.Count;

    /// <summary>
    /// Gets the approximate number of bytes occupied by all keys and values written so far.
    /// </summary>
    public long Size => Interlocked.Read(ref _size);

    /// <summary>
    /// Attempts to retrieve the value for <paramref name="key"/>.
    /// </summary>
    /// <param name="key">The key to look up.</param>
    /// <param name="value">
    /// The value associated with <paramref name="key"/> if found and not deleted;
    /// <c>null</c> if the key has a tombstone; or <c>default</c> if the key is not present.
    /// </param>
    /// <returns><c>true</c> if the key exists (even as a tombstone); otherwise <c>false</c>.</returns>
    public bool TryGet(TKey key, out ReadOnlyMemory<byte>? value) {
        if (!_data.TryGet(key, out var entry)) {
            value = default;
            return false;
        }
        if (entry.IsTombstone) {
            value = null;
            return true;
        }
        value = entry.Data;
        return true;
    }

    /// <summary>
    /// Applies a committed WAL record to the in-memory state.
    /// </summary>
    /// <remarks>
    /// Called by the apply loop after a record has been committed to the write-ahead log,
    /// and during recovery to replay records that are already durable.
    /// </remarks>
    /// <exception cref="InvalidDataException">The record contains an unrecognized marker byte.</exception>
    internal void ApplyRecord(ReadOnlyMemory<byte> record) {
        var span = record.Span;
        var marker = span[0];
        var keyLen = BinaryPrimitives.ReadInt32LittleEndian(span[1..]);
        TKey key = new ReadOnlyMemory<byte>(record.Slice(5, keyLen).ToArray());

        if (marker == WalRecord.TombstoneMarker) {
            _data.Write(key, new Entry(Data: default, IsTombstone: true));
            Interlocked.Add(ref _size, keyLen);
            return;
        }

        if (marker == WalRecord.WriteMarker) {
            var value = new ReadOnlyMemory<byte>(record.Slice(5 + keyLen).ToArray());
            _data.Write(key, new Entry(value, IsTombstone: false));
            Interlocked.Add(ref _size, (long)keyLen + value.Length);
            return;
        }

        throw new InvalidDataException($"Unknown WAL record marker 0x{marker:X2}.");
    }

    /// <inheritdoc/>
    public void Dispose() => _data.Dispose();

    /// <summary>
    /// Returns an enumerable that yields entries in ascending key order, starting from the first key
    /// greater than or equal to <paramref name="from"/>.
    /// </summary>
    public IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> ScanFrom(TKey from) =>
        _data.Scan(from)
             .Select(static pair => new KeyValuePair<TKey, ReadOnlyMemory<byte>?>(
                 pair.Key,
                 pair.Value.IsTombstone ? (ReadOnlyMemory<byte>?)null : pair.Value.Data));

    /// <summary>
    /// Returns an enumerator that yields all entries in ascending key order.
    /// </summary>
    IEnumerator<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>>.GetEnumerator() =>
        _data
            .Select(static pair => new KeyValuePair<TKey, ReadOnlyMemory<byte>?>(
                pair.Key,
                pair.Value.IsTombstone ? (ReadOnlyMemory<byte>?)null : pair.Value.Data))
            .GetEnumerator();

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() =>
        ((IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>>)this).GetEnumerator();

    readonly record struct Entry(ReadOnlyMemory<byte> Data, bool IsTombstone);
}
