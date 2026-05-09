using System.Text;
using FluentAssertions;
using MSSP.Log;

namespace MSSP.LsmTree;

public class WalRecoveryTests {
    static ReadOnlyMemory<byte> Bytes(string s) => Encoding.UTF8.GetBytes(s);
    static string Text(ReadOnlyMemory<byte> b) => Encoding.UTF8.GetString(b.Span);

    [Fact]
    public async Task Recovery_RestoresWrittenEntries() {
        using var walLog = new StreamSegment<WalRecord>(new MemoryStream());
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
        using var walLog = new StreamSegment<WalRecord>(new MemoryStream());
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
        using var walLog = new StreamSegment<WalRecord>(new MemoryStream());
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
        using var walLog = new StreamSegment<WalRecord>(new MemoryStream());
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
