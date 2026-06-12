using MSSP.Storage;

namespace MSSP.Engine.Storage;

class TestLogRecord(byte[] payload) : ILogRecord<TestLogRecord> {
    readonly byte[] _payload = payload;

    public static implicit operator ReadOnlyMemory<byte>(TestLogRecord record) => record._payload;

    public static implicit operator TestLogRecord(ReadOnlyMemory<byte> memory) => new(memory.ToArray());

    bool Equals(TestLogRecord other) => _payload.SequenceEqual(other._payload);

    public override bool Equals(object? obj) {
        if (ReferenceEquals(null, obj)) return false;
        if (ReferenceEquals(this, obj)) return true;
        return obj.GetType() == GetType() && Equals((TestLogRecord)obj);
    }

    public override int GetHashCode() => _payload.GetHashCode();
}
