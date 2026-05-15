using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;

namespace MSSP.LsmTree;

/// <summary>
/// Reads entries from a stream in the SST file format.
/// Loads the sparse index into memory on construction for efficient point lookups.
/// </summary>
/// <remarks>
/// The reader is not thread-safe. <see cref="TryGet"/> and <see cref="Scan"/> must not
/// be called concurrently, as they both manipulate the underlying stream position.
/// </remarks>
/// <typeparam name="TKey">The type of the key.</typeparam>
sealed class SstReader<TKey> : IDisposable where TKey : IKey<TKey> {
    readonly Stream _stream;
    readonly Footer _footer;
    readonly IndexEntry[] _index;

    /// <summary>
    /// Opens an SST file from <paramref name="input"/>. The stream must be seekable,
    /// readable, and positioned at offset 0.
    /// </summary>
    /// <exception cref="InvalidDataException">The stream does not contain a valid SST file.</exception>
    internal SstReader(Stream input) {
        _stream = input;
        _footer = ReadFooter(input);
        _index = ReadIndex(input, _footer);
    }

    /// <summary>
    /// Attempts to retrieve the value for <paramref name="key"/>.
    /// </summary>
    /// <param name="key">The key to look up.</param>
    /// <param name="value">
    /// The value if found and not deleted; <c>null</c> if the key has a tombstone;
    /// or <c>default</c> if the key is not present.
    /// </param>
    /// <returns><c>true</c> if the key exists (even as a tombstone); otherwise <c>false</c>.</returns>
    internal bool TryGet(TKey key, [MaybeNullWhen(false)] out ReadOnlyMemory<byte>? value) {
        var blockIndex = FindBlockIndex(key);
        if (blockIndex < 0) {
            value = default;
            return false;
        }

        var blockEnd = blockIndex + 1 < _index.Length
            ? _index[blockIndex + 1].DataOffset
            : _footer.IndexOffset;

        _stream.Seek(_index[blockIndex].DataOffset, SeekOrigin.Begin);

        while (_stream.Position < blockEnd) {
            var (entryKey, entryValue) = ReadEntry();
            var cmp = entryKey.CompareTo(key);
            if (cmp == 0) {
                value = entryValue;
                return true;
            }
            if (cmp > 0) break;
        }

        value = default;
        return false;
    }

    /// <summary>
    /// Returns an enumerable that yields all entries in ascending key order.
    /// Resets the read position each time iteration begins.
    /// </summary>
    internal IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> Scan() {
        _stream.Seek(0, SeekOrigin.Begin);
        while (_stream.Position < _footer.IndexOffset) {
            var (key, value) = ReadEntry();
            yield return new(key, value);
        }
    }

    /// <summary>
    /// Returns an enumerable that yields entries in ascending key order, starting from the first
    /// entry with a key greater than or equal to <paramref name="from"/>.
    /// </summary>
    internal IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> Scan(TKey from) {
        var blockIndex = FindBlockIndex(from);
        _stream.Seek(blockIndex >= 0 ? _index[blockIndex].DataOffset : 0L, SeekOrigin.Begin);
        while (_stream.Position < _footer.IndexOffset) {
            var (key, value) = ReadEntry();
            if (key.CompareTo(from) < 0) continue;
            yield return new(key, value);
        }
    }

    /// <inheritdoc/>
    public void Dispose() => _stream.Dispose();

    // Binary search: returns the largest index i where _index[i].Key <= key, or -1 if none.
    int FindBlockIndex(TKey key) {
        var hi = -1;
        var left = 0;
        var right = _index.Length - 1;
        while (left <= right) {
            var mid = left + (right - left) / 2;
            var cmp = _index[mid].Key.CompareTo(key);
            if (cmp <= 0) {
                hi = mid;
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return hi;
    }

    (TKey Key, ReadOnlyMemory<byte>? Value) ReadEntry() {
        Span<byte> intBuf = stackalloc byte[4];

        var marker = (byte)_stream.ReadByte();

        _stream.ReadExactly(intBuf);
        var keyLen = BinaryPrimitives.ReadInt32LittleEndian(intBuf);

        var keyBuf = new byte[keyLen];
        _stream.ReadExactly(keyBuf);
        TKey key = new ReadOnlyMemory<byte>(keyBuf);

        if (marker == Sst.TombstoneMarker)
            return (key, null);

        if (marker != Sst.WriteMarker)
            throw new InvalidDataException($"Unknown entry marker 0x{marker:X2} at offset {_stream.Position - 1 - 4 - keyLen}.");

        _stream.ReadExactly(intBuf);
        var valueLen = BinaryPrimitives.ReadInt32LittleEndian(intBuf);

        var valueBuf = new byte[valueLen];
        _stream.ReadExactly(valueBuf);

        return (key, new ReadOnlyMemory<byte>(valueBuf));
    }

    static Footer ReadFooter(Stream input) {
        Span<byte> buf = stackalloc byte[Sst.FooterSize];
        input.Seek(-Sst.FooterSize, SeekOrigin.End);
        input.ReadExactly(buf);

        if (!buf[..8].SequenceEqual(Sst.Magic))
            throw new InvalidDataException("Not a valid SST file.");

        return new Footer(
            IndexOffset: BinaryPrimitives.ReadInt64LittleEndian(buf[8..]),
            EntryCount: BinaryPrimitives.ReadInt32LittleEndian(buf[16..]),
            IndexEntryCount: BinaryPrimitives.ReadInt32LittleEndian(buf[20..]),
            SparseInterval: BinaryPrimitives.ReadInt32LittleEndian(buf[24..]));
    }

    static IndexEntry[] ReadIndex(Stream input, Footer footer) {
        input.Seek(footer.IndexOffset, SeekOrigin.Begin);
        var entries = new IndexEntry[footer.IndexEntryCount];
        Span<byte> intBuf = stackalloc byte[4];
        Span<byte> longBuf = stackalloc byte[8];

        for (var i = 0; i < footer.IndexEntryCount; i++) {
            input.ReadExactly(intBuf);
            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(intBuf);

            var keyBuf = new byte[keyLen];
            input.ReadExactly(keyBuf);
            TKey key = new ReadOnlyMemory<byte>(keyBuf);

            input.ReadExactly(longBuf);
            entries[i] = new IndexEntry(key, BinaryPrimitives.ReadInt64LittleEndian(longBuf));
        }

        return entries;
    }

    readonly record struct Footer(long IndexOffset, int EntryCount, int IndexEntryCount, int SparseInterval);
    readonly record struct IndexEntry(TKey Key, long DataOffset);
}
