using System.Buffers.Binary;

namespace MSSP.Storage;

/// <summary>
/// A single record in the write-ahead log; wraps an opaque byte payload.
/// </summary>
readonly struct WalRecord : ILogRecord<WalRecord> {
    readonly ReadOnlyMemory<byte> _data;

    WalRecord(ReadOnlyMemory<byte> data) => _data = data;

    internal const byte WriteMarker = 0x01;
    internal const byte TombstoneMarker = 0x02;

    /// <summary>
    /// Creates a record encoding <paramref name="key"/> and <paramref name="value"/>.
    /// </summary>
    internal static WalRecord From<TKey>(TKey key, ReadOnlyMemory<byte> value) where TKey : IKey<TKey> {
        ReadOnlyMemory<byte> keyBytes = key;
        var buf = new byte[1 + 4 + keyBytes.Length + value.Length];
        buf[0] = WriteMarker;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(1), keyBytes.Length);
        keyBytes.Span.CopyTo(buf.AsSpan(5));
        value.Span.CopyTo(buf.AsSpan(5 + keyBytes.Length));
        return new WalRecord(buf);
    }

    /// <summary>
    /// Creates a tombstone record encoding <paramref name="key"/>.
    /// </summary>
    internal static WalRecord Tombstone<TKey>(TKey key) where TKey : IKey<TKey> {
        ReadOnlyMemory<byte> keyBytes = key;
        var buf = new byte[1 + 4 + keyBytes.Length];
        buf[0] = TombstoneMarker;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(1), keyBytes.Length);
        keyBytes.Span.CopyTo(buf.AsSpan(5));
        return new WalRecord(buf);
    }

    /// <summary>
    /// Converts the record to its underlying byte representation.
    /// </summary>
    public static implicit operator ReadOnlyMemory<byte>(WalRecord record) => record._data;

    /// <summary>
    /// Wraps a raw byte buffer as a <see cref="WalRecord"/>.
    /// </summary>
    public static implicit operator WalRecord(ReadOnlyMemory<byte> memory) => new(memory);
}
