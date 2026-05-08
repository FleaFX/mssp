using FluentAssertions;
using Log.Extensions;

namespace Log;

public class LogSegmentTests : IDisposable {
    readonly LogSegment<TestLogRecord> _logSegment;

    public LogSegmentTests() {
        _logSegment = new LogSegment<TestLogRecord>(10);
    }

    [Fact]
    public async Task Append() {
        (await _logSegment.TryAppendAsync(new TestLogRecord(new byte[] { 1, 2, 3, 4, 5, 6 }))).Should().BeTrue();

        (await _logSegment.EnumerateAsync()).Should().BeEquivalentTo(new[] { new TestLogRecord(new byte[] { 1, 2, 3, 4, 5, 6 }) });
        (await _logSegment.EnumerateAsync()).Should().BeEquivalentTo(new[] { new TestLogRecord(new byte[] { 1, 2, 3, 4, 5, 6 }) }); // should be able to enumerate again
    }

    [Fact]
    public async Task AppendRejection() {
        (await _logSegment.TryAppendAsync(new TestLogRecord(new byte[] { 1, 2, 3, 4, 5, 6 }))).Should().BeTrue();
        (await _logSegment.TryAppendAsync(new TestLogRecord(new byte[] { 7, 8, 9, 10, 11, 12 }))).Should().BeFalse(); // won't fit

        (await _logSegment.EnumerateAsync()).Should().BeEquivalentTo(new[] { new TestLogRecord(new byte[] { 1, 2, 3, 4, 5, 6 }) });
    }

    public void Dispose() => _logSegment.Dispose();
}