using System.Buffers;
using System.Buffers.Binary;

namespace MSSP.LsmTree;

/// <summary>
/// Appends a WAL record durably. The delegate must copy or persist the
/// <paramref name="record"/> bytes before returning, because the underlying
/// buffer is returned to the pool immediately after the <c>ValueTask</c> completes.
/// </summary>
/// <param name="record">The record bytes to persist.</param>
/// <param name="cancellationToken">Token to cancel the append.</param>
/// <returns><c>true</c> if the record was durably appended; otherwise <c>false</c>.</returns>
delegate ValueTask<bool> WalAppendDelegate(ReadOnlyMemory<byte> record, CancellationToken cancellationToken = default);

/// <summary>
/// The Level 0 component of the LSM tree. Buffers writes in an ordered in-memory
/// skip list, with every mutation durably appended to the WAL before being committed
/// to memory. Callers should flush to an SST file when <see cref="IsFull"/> is true.
/// </summary>
/// <typeparam name="TKey">The type of the key.</typeparam>
sealed class MemTable<TKey>(int capacityBytes, WalAppendDelegate walAppend) : IDisposable
    where TKey : IKey<TKey> {

    const byte WriteMarker = 0x01;
    const byte TombstoneMarker = 0x02;

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
    /// Appends a write record to the WAL and, on success, stores the key/value pair in memory.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value to associate with <paramref name="key"/>.</param>
    /// <param name="cancellationToken">Token to cancel the WAL append.</param>
    /// <returns><c>true</c> if the WAL append succeeded and the entry was committed; otherwise <c>false</c>.</returns>
    public async ValueTask<bool> TryWriteAsync(TKey key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) {
        ReadOnlyMemory<byte> keyBytes = key;
        var recordLength = 1 + 4 + keyBytes.Length + value.Length;
        var buffer = ArrayPool<byte>.Shared.Rent(recordLength);
        try {
            var span = buffer.AsSpan(0, recordLength);
            span[0] = WriteMarker;
            BinaryPrimitives.WriteInt32LittleEndian(span[1..], keyBytes.Length);
            keyBytes.Span.CopyTo(span[5..]);
            value.Span.CopyTo(span[(5 + keyBytes.Length)..]);

            if (!await walAppend(buffer.AsMemory(0, recordLength), cancellationToken))
                return false;
        } finally {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        _data.Write(key, new Entry(value, IsTombstone: false));
        Interlocked.Add(ref _size, (long)keyBytes.Length + value.Length);
        return true;
    }

    /// <summary>
    /// Appends a tombstone record to the WAL and, on success, marks the key as deleted in memory.
    /// </summary>
    /// <remarks>
    /// Deletes in an LSM tree are soft: a tombstone entry is written rather than physically removing
    /// the key. The tombstone is resolved during compaction or SST merging.
    /// </remarks>
    /// <param name="key">The key to delete.</param>
    /// <param name="cancellationToken">Token to cancel the WAL append.</param>
    /// <returns><c>true</c> if the WAL append succeeded and the tombstone was committed; otherwise <c>false</c>.</returns>
    public async ValueTask<bool> TryDeleteAsync(TKey key, CancellationToken cancellationToken = default) {
        ReadOnlyMemory<byte> keyBytes = key;
        var recordLength = 1 + 4 + keyBytes.Length;
        var buffer = ArrayPool<byte>.Shared.Rent(recordLength);
        try {
            var span = buffer.AsSpan(0, recordLength);
            span[0] = TombstoneMarker;
            BinaryPrimitives.WriteInt32LittleEndian(span[1..], keyBytes.Length);
            keyBytes.Span.CopyTo(span[5..]);

            if (!await walAppend(buffer.AsMemory(0, recordLength), cancellationToken))
                return false;
        } finally {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        _data.Write(key, new Entry(Data: default, IsTombstone: true));
        Interlocked.Add(ref _size, keyBytes.Length);
        return true;
    }

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

    /// <inheritdoc/>
    public void Dispose() => _data.Dispose();

    readonly record struct Entry(ReadOnlyMemory<byte> Data, bool IsTombstone);
}
