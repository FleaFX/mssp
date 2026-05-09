using FluentAssertions;
using MSSP.Extensions;

namespace MSSP.Log; 

public class SegmentedLogTests {
    readonly SegmentedLog<TestLogRecord> _segmentedLog = new(10);

    [Fact]
    public async Task Append() {
        (await _segmentedLog.TryAppendAsync(new TestLogRecord(new byte[] { 1, 2, 3, 4, 5, 6 }))).Should().BeTrue();

        (await _segmentedLog.EnumerateAsync()).Should().BeEquivalentTo(new[] { new TestLogRecord(new byte[] { 1, 2, 3, 4, 5, 6 }) });
        (await _segmentedLog.EnumerateAsync()).Should().BeEquivalentTo(new[] { new TestLogRecord(new byte[] { 1, 2, 3, 4, 5, 6 }) }); // should be able to enumerate again
    }

    [Fact]
    public async Task Dispose_DisposesAllSegments() {
        var disposed = new List<bool>();

        ILogSegment<TestLogRecord> TrackingFactory(int size) {
            var i = disposed.Count;
            disposed.Add(false);
            return new TrackingLogSegment(size, () => disposed[i] = true);
        }

        var log = new SegmentedLog<TestLogRecord>(10, TrackingFactory);
        await log.TryAppendAsync(new TestLogRecord(new byte[] { 1, 2, 3, 4, 5, 6 }));
        await log.TryAppendAsync(new TestLogRecord(new byte[] { 7, 8, 9, 10, 11, 12 })); // forces a second segment

        log.Dispose();

        disposed.Should().AllSatisfy(d => d.Should().BeTrue("every segment created should be disposed"));
    }

    [Fact]
    public async Task AppendNewSegment() {
        (await _segmentedLog.TryAppendAsync(new TestLogRecord(new byte[] { 1, 2, 3, 4, 5, 6 }))).Should().BeTrue();

        // segment size is only 10 bytes, so this should start a new segment
        (await _segmentedLog.TryAppendAsync(new TestLogRecord(new byte[] { 7, 8, 9, 10, 11, 12 }))).Should().BeTrue();

        var contents = await _segmentedLog.EnumerateAsync();
        contents.Should().BeEquivalentTo(new[] {
            new TestLogRecord(new byte[] { 1, 2, 3, 4, 5, 6 }),
            new TestLogRecord(new byte[] { 7, 8, 9, 10, 11, 12 })
        });
    }
}
