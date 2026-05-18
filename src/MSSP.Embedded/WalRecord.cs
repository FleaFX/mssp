using MSSP.Log;

namespace MSSP.Embedded;

/// <summary>
/// A single record in the embedded WAL; wraps an opaque byte payload.
/// </summary>
readonly struct WalRecord : ILogRecord<WalRecord> {
    readonly ReadOnlyMemory<byte> _data;

    WalRecord(ReadOnlyMemory<byte> data) => _data = data;

    /// <summary>
    /// Converts the record to its underlying byte representation.
    /// </summary>
    public static implicit operator ReadOnlyMemory<byte>(WalRecord record) => record._data;

    /// <summary>
    /// Wraps a raw byte buffer as a <see cref="WalRecord"/>.
    /// </summary>
    public static implicit operator WalRecord(ReadOnlyMemory<byte> memory) => new(memory);
}
