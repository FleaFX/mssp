using MSSP.Log;

namespace MSSP.Embedded;

readonly struct WalRecord : ILogRecord<WalRecord> {
    readonly ReadOnlyMemory<byte> _data;

    WalRecord(ReadOnlyMemory<byte> data) => _data = data;

    public static implicit operator ReadOnlyMemory<byte>(WalRecord record) => record._data;
    public static implicit operator WalRecord(ReadOnlyMemory<byte> memory) => new(memory);
}
