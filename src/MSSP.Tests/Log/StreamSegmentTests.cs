using FluentAssertions;
using MSSP.Extensions;

namespace MSSP.Log;

public class StreamSegmentTests {
    public class TryAppendAsync : StreamSegmentTests {
        [Fact]
        public async Task SingleRecord_ReadableByEnumeration() {
            using var log = new StreamSegment<TestLogRecord>(new MemoryStream());

            await log.TryAppendAsync(new TestLogRecord(new byte[] { 0x01, 0x02, 0x03 }));

            (await log.EnumerateAsync()).Should().ContainSingle()
                .Which.Should().BeEquivalentTo(new TestLogRecord(new byte[] { 0x01, 0x02, 0x03 }));
        }

        [Fact]
        public async Task MultipleRecords_AllPreserved() {
            using var log = new StreamSegment<TestLogRecord>(new MemoryStream());

            await log.TryAppendAsync(new TestLogRecord(new byte[] { 0x01 }));
            await log.TryAppendAsync(new TestLogRecord(new byte[] { 0x02, 0x03 }));
            await log.TryAppendAsync(new TestLogRecord(new byte[] { 0x04, 0x05, 0x06 }));

            (await log.EnumerateAsync()).Should().HaveCount(3);
        }

        [Fact]
        public async Task Returns_True_OnSuccess() {
            using var log = new StreamSegment<TestLogRecord>(new MemoryStream());

            var result = await log.TryAppendAsync(new TestLogRecord(new byte[] { 0x01 }));

            result.Should().BeTrue();
        }

        [Fact]
        public async Task Returns_False_OnIoError() {
            using var log = new StreamSegment<TestLogRecord>(new ThrowingStream());

            var result = await log.TryAppendAsync(new TestLogRecord(new byte[] { 0x01 }));

            result.Should().BeFalse();
        }

    }

    public class Enumerate : StreamSegmentTests {
        [Fact]
        public async Task EmptyStream_YieldsNothing() {
            using var log = new StreamSegment<TestLogRecord>(new MemoryStream());
            (await log.EnumerateAsync()).Should().BeEmpty();
        }

        [Fact]
        public async Task PreservesRecordBytes() {
            using var log = new StreamSegment<TestLogRecord>(new MemoryStream());
            await log.TryAppendAsync(new TestLogRecord(new byte[] { 0xAA, 0xBB, 0xCC }));

            (await log.EnumerateAsync()).Should().ContainSingle()
                .Which.Should().BeEquivalentTo(new TestLogRecord(new byte[] { 0xAA, 0xBB, 0xCC }));
        }

        [Fact]
        public async Task TruncatedLengthHeader_StopsEarly() {
            var stream = new MemoryStream();
            using var log = new StreamSegment<TestLogRecord>(stream);
            await log.TryAppendAsync(new TestLogRecord(new byte[] { 0xAA }));
            stream.Write([0x05, 0x00]); // only 2 of the 4 length bytes

            (await log.EnumerateAsync()).Should().HaveCount(1);
        }

        [Fact]
        public async Task TruncatedData_StopsEarly() {
            var stream = new MemoryStream();
            using var log = new StreamSegment<TestLogRecord>(stream);
            await log.TryAppendAsync(new TestLogRecord(new byte[] { 0xAA }));
            stream.Write([0x0A, 0x00, 0x00, 0x00, 0x01, 0x02]); // claims 10 bytes, only 2 present

            (await log.EnumerateAsync()).Should().HaveCount(1);
        }

        [Fact]
        public async Task CorruptData_StopsEarly() {
            var stream = new MemoryStream();
            using var log = new StreamSegment<TestLogRecord>(stream);
            await log.TryAppendAsync(new TestLogRecord(new byte[] { 0xAA }));
            // Overwrite the data byte with garbage — CRC will no longer match
            stream.Position = 4;
            stream.WriteByte(0xFF);

            (await log.EnumerateAsync()).Should().BeEmpty();
        }

        [Fact]
        public async Task CorruptRecord_AfterValidRecord_StopsEarly() {
            var stream = new MemoryStream();
            using var log = new StreamSegment<TestLogRecord>(stream);
            await log.TryAppendAsync(new TestLogRecord(new byte[] { 0xAA }));
            var corruptRecordStart = stream.Length;
            await log.TryAppendAsync(new TestLogRecord(new byte[] { 0xBB }));
            // Overwrite the data byte of the second record with garbage
            stream.Position = corruptRecordStart + 4;
            stream.WriteByte(0xFF);

            (await log.EnumerateAsync()).Should().HaveCount(1);
        }
    }
}

sealed class ThrowingStream : Stream {
    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();
    public override long Position {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }
    public override void Flush() { }
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new IOException("Disk full");
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct) =>
        ValueTask.FromException(new IOException("Disk full"));
    public override Task FlushAsync(CancellationToken ct) => Task.CompletedTask;
}
