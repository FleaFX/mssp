using System.Text;
using FluentAssertions;
using MSSP.Storage;

namespace MSSP.Engine.Storage;

public class WalRecoveryTests {
    static ReadOnlyMemory<byte> Bytes(string s) => Encoding.UTF8.GetBytes(s);
    static string Text(ReadOnlyMemory<byte> b) => Encoding.UTF8.GetString(b.Span);

    [Fact]
    public void Recovery_RestoresWrittenEntries() {
        var walRecords = new[] {
            WalRecord.From(new StringKey("a"), Bytes("value-a")),
            WalRecord.From(new StringKey("b"), Bytes("value-b")),
        };

        using var recovered = new MemTable<StringKey>(4096);
        foreach (var record in walRecords)
            recovered.ApplyRecord(record);

        recovered.Count.Should().Be(2);
        recovered.TryGet(new StringKey("a"), out var va).Should().BeTrue();
        Text(va!.Value).Should().Be("value-a");
        recovered.TryGet(new StringKey("b"), out var vb).Should().BeTrue();
        Text(vb!.Value).Should().Be("value-b");
    }

    [Fact]
    public void Recovery_RestoresTombstone() {
        var walRecords = new[] {
            WalRecord.From(new StringKey("x"), Bytes("value")),
            WalRecord.Tombstone(new StringKey("x")),
        };

        using var recovered = new MemTable<StringKey>(4096);
        foreach (var record in walRecords)
            recovered.ApplyRecord(record);

        recovered.TryGet(new StringKey("x"), out var value).Should().BeTrue();
        value.Should().BeNull();
    }

    [Fact]
    public void Recovery_LastWriteWins_OnDuplicateKey() {
        var walRecords = new[] {
            WalRecord.From(new StringKey("k"), Bytes("first")),
            WalRecord.From(new StringKey("k"), Bytes("second")),
        };

        using var recovered = new MemTable<StringKey>(4096);
        foreach (var record in walRecords)
            recovered.ApplyRecord(record);

        recovered.TryGet(new StringKey("k"), out var value).Should().BeTrue();
        Text(value!.Value).Should().Be("second");
    }

    [Fact]
    public void Recovery_EmptyWal_ProducesEmptyMemTable() {
        using var recovered = new MemTable<StringKey>(4096);

        recovered.Count.Should().Be(0);
    }
}
