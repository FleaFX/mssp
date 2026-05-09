using FluentAssertions;
using MSSP.Extensions;

namespace MSSP.Log;

public class LogSegmentTests : IDisposable {
    readonly MemorySegment<TestLogRecord> _logSegment;

    public LogSegmentTests() {
        _logSegment = new MemorySegment<TestLogRecord>(10);
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

    [Fact]
    public async Task TryAppend_ReturnsFalse_AfterCompleteCalledFromAnotherThread() {
        var segment = new MemorySegment<TestLogRecord>(1024);

        var completeTask = Task.Run(() => segment.Complete());
        await completeTask;

        (await segment.TryAppendAsync(new TestLogRecord(new byte[] { 1, 2, 3 })))
            .Should().BeFalse("Complete() must be visible across threads");

        segment.Dispose();
    }

    [Fact]
    public async Task Enumerate_CompletesWithoutHanging_WhenCompleteCalledFromAnotherThread() {
        var segment = new MemorySegment<TestLogRecord>(1024);
        await segment.TryAppendAsync(new TestLogRecord(new byte[] { 1, 2, 3 }));

        // Complete() is called from a concurrent task; the enumerator must see _completed = true
        // and terminate rather than waiting indefinitely for new entries
        var completeTask = Task.Run(() => segment.Complete());
        var enumerateTask = segment.EnumerateAsync();

        await Task.WhenAll(completeTask, enumerateTask).WaitAsync(TimeSpan.FromSeconds(2));

        segment.Dispose();
    }

    public void Dispose() => _logSegment.Dispose();
}
