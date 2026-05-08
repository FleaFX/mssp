namespace MSSP.Log;

class TrackingLogSegment(int segmentSize, Action onDispose) : LogSegment<TestLogRecord>(segmentSize) {
    public override void Dispose() {
        onDispose();
        base.Dispose();
    }
}
