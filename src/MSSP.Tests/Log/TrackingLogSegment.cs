namespace MSSP.Log;

class TrackingLogSegment(int segmentSize, Action onDispose) : MemorySegment<TestLogRecord>(segmentSize) {
    public override void Dispose() {
        onDispose();
        base.Dispose();
    }
}
