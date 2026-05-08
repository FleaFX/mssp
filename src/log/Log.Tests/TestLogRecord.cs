namespace Log;

class TestLogRecord : ILogRecord<TestLogRecord> {
    readonly byte[] _payload;

    public TestLogRecord(byte[] payload) => _payload = payload;

    public static implicit operator Memory<byte>(TestLogRecord record) => record._payload;

    public static implicit operator TestLogRecord(Memory<byte> memory) => new(memory.ToArray());

    bool Equals(TestLogRecord other) => _payload.SequenceEqual(other._payload);

    public override bool Equals(object? obj) {
        if (ReferenceEquals(null, obj)) return false;
        if (ReferenceEquals(this, obj)) return true;
        return obj.GetType() == GetType() && Equals((TestLogRecord)obj);
    }

    public override int GetHashCode() => _payload.GetHashCode();
}