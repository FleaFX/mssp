using System.Text;
using FluentAssertions;
using MSSP.Extensions;
using MSSP.Log;

namespace MSSP.LsmTree;

public class StreamLogTests {
    public class TryAppendAsync : StreamLogTests {
        [Fact]
        public async Task SingleRecord_ReadableByEnumeration() {
            using var log = new StreamLog<WalRecord>(new MemoryStream());

            await log.TryAppendAsync(new WalRecord(new byte[] { 0x01, 0x02, 0x03 }));

            var records = await log.EnumerateAsync();
            records.Should().ContainSingle().Which.Bytes.ToArray().Should().Equal(0x01, 0x02, 0x03);
        }

        [Fact]
        public async Task MultipleRecords_AllPreserved() {
            using var log = new StreamLog<WalRecord>(new MemoryStream());

            await log.TryAppendAsync(new WalRecord(new byte[] { 0x01 }));
            await log.TryAppendAsync(new WalRecord(new byte[] { 0x02, 0x03 }));
            await log.TryAppendAsync(new WalRecord(new byte[] { 0x04, 0x05, 0x06 }));

            (await log.EnumerateAsync()).Should().HaveCount(3);
        }

        [Fact]
        public async Task Returns_True_OnSuccess() {
            using var log = new StreamLog<WalRecord>(new MemoryStream());

            var result = await log.TryAppendAsync(new WalRecord(new byte[] { 0x01 }));

            result.Should().BeTrue();
        }

        [Fact]
        public async Task Returns_False_OnIoError() {
            using var log = new StreamLog<WalRecord>(new ThrowingStream());

            var result = await log.TryAppendAsync(new WalRecord(new byte[] { 0x01 }));

            result.Should().BeFalse();
        }

        [Fact]
        public async Task Returns_False_AfterComplete() {
            using var log = new StreamLog<WalRecord>(new MemoryStream());
            log.Complete();

            var result = await log.TryAppendAsync(new WalRecord(new byte[] { 0x01 }));

            result.Should().BeFalse();
        }
    }

    public class Enumerate : StreamLogTests {
        [Fact]
        public async Task EmptyStream_YieldsNothing() {
            using var log = new StreamLog<WalRecord>(new MemoryStream());
            (await log.EnumerateAsync()).Should().BeEmpty();
        }

        [Fact]
        public async Task PreservesRecordBytes() {
            using var log = new StreamLog<WalRecord>(new MemoryStream());
            await log.TryAppendAsync(new WalRecord(new byte[] { 0xAA, 0xBB, 0xCC }));

            (await log.EnumerateAsync()).Single().Bytes.ToArray().Should().Equal(0xAA, 0xBB, 0xCC);
        }

        [Fact]
        public async Task TruncatedLengthHeader_StopsEarly() {
            var stream = new MemoryStream();
            using var log = new StreamLog<WalRecord>(stream);
            await log.TryAppendAsync(new WalRecord(new byte[] { 0xAA }));
            stream.Write([0x05, 0x00]); // only 2 of the 4 length bytes

            (await log.EnumerateAsync()).Should().HaveCount(1);
        }

        [Fact]
        public async Task TruncatedData_StopsEarly() {
            var stream = new MemoryStream();
            using var log = new StreamLog<WalRecord>(stream);
            await log.TryAppendAsync(new WalRecord(new byte[] { 0xAA }));
            stream.Write([0x0A, 0x00, 0x00, 0x00, 0x01, 0x02]); // claims 10 bytes, only 2 present

            (await log.EnumerateAsync()).Should().HaveCount(1);
        }
    }
}

public class WalRecoveryTests {
    static ReadOnlyMemory<byte> Bytes(string s) => Encoding.UTF8.GetBytes(s);
    static string Text(ReadOnlyMemory<byte> b) => Encoding.UTF8.GetString(b.Span);

    [Fact]
    public async Task Recovery_RestoresWrittenEntries() {
        using var walLog = new StreamLog<WalRecord>(new MemoryStream());
        using var original = new MemTable<StringKey>(4096, (bytes, ct) => walLog.TryAppendAsync(new WalRecord(bytes), ct));
        await original.TryWriteAsync(new StringKey("a"), Bytes("value-a"));
        await original.TryWriteAsync(new StringKey("b"), Bytes("value-b"));

        using var recovered = new MemTable<StringKey>(4096, (_, _) => ValueTask.FromResult(true));
        await foreach (var record in walLog)
            recovered.ApplyRecord(record.Bytes);

        recovered.Count.Should().Be(2);
        recovered.TryGet(new StringKey("a"), out var va).Should().BeTrue();
        Text(va!.Value).Should().Be("value-a");
        recovered.TryGet(new StringKey("b"), out var vb).Should().BeTrue();
        Text(vb!.Value).Should().Be("value-b");
    }

    [Fact]
    public async Task Recovery_RestoresTombstone() {
        using var walLog = new StreamLog<WalRecord>(new MemoryStream());
        using var original = new MemTable<StringKey>(4096, (bytes, ct) => walLog.TryAppendAsync(new WalRecord(bytes), ct));
        await original.TryWriteAsync(new StringKey("x"), Bytes("value"));
        await original.TryDeleteAsync(new StringKey("x"));

        using var recovered = new MemTable<StringKey>(4096, (_, _) => ValueTask.FromResult(true));
        await foreach (var record in walLog)
            recovered.ApplyRecord(record.Bytes);

        recovered.TryGet(new StringKey("x"), out var value).Should().BeTrue();
        value.Should().BeNull();
    }

    [Fact]
    public async Task Recovery_LastWriteWins_OnDuplicateKey() {
        using var walLog = new StreamLog<WalRecord>(new MemoryStream());
        using var original = new MemTable<StringKey>(4096, (bytes, ct) => walLog.TryAppendAsync(new WalRecord(bytes), ct));
        await original.TryWriteAsync(new StringKey("k"), Bytes("first"));
        await original.TryWriteAsync(new StringKey("k"), Bytes("second"));

        using var recovered = new MemTable<StringKey>(4096, (_, _) => ValueTask.FromResult(true));
        await foreach (var record in walLog)
            recovered.ApplyRecord(record.Bytes);

        recovered.TryGet(new StringKey("k"), out var value).Should().BeTrue();
        Text(value!.Value).Should().Be("second");
    }

    [Fact]
    public async Task Recovery_EmptyWal_ProducesEmptyMemTable() {
        using var walLog = new StreamLog<WalRecord>(new MemoryStream());
        using var recovered = new MemTable<StringKey>(4096, (_, _) => ValueTask.FromResult(true));
        await foreach (var record in walLog)
            recovered.ApplyRecord(record.Bytes);

        recovered.Count.Should().Be(0);
    }
}

readonly record struct WalRecord(ReadOnlyMemory<byte> Bytes) : ILogRecord<WalRecord> {
    public static implicit operator ReadOnlyMemory<byte>(WalRecord record) => record.Bytes;
    public static implicit operator WalRecord(ReadOnlyMemory<byte> memory) => new(memory);
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
