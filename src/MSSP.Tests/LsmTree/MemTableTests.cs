using System.Buffers.Binary;
using System.Text;
using FluentAssertions;

namespace MSSP.LsmTree;

public class MemTableTests : IDisposable {
    readonly List<byte[]> _walRecords = [];
    readonly MemTable<StringKey> _memTable;

    public MemTableTests() =>
        _memTable = new MemTable<StringKey>(1024, (record, _) => {
            _walRecords.Add(record.ToArray());
            return ValueTask.FromResult(true);
        });

    public void Dispose() => _memTable.Dispose();

    static ReadOnlyMemory<byte> Bytes(string value) => Encoding.UTF8.GetBytes(value);
    static string Text(ReadOnlyMemory<byte> bytes) => Encoding.UTF8.GetString(bytes.Span);

    public class TryWriteAsync : MemTableTests {
        [Fact]
        public async Task StoresValue_RetrievableAfterWrite() {
            await _memTable.TryWriteAsync(new StringKey("key"), Bytes("value"));

            _memTable.TryGet(new StringKey("key"), out var value).Should().BeTrue();
            value.Should().NotBeNull();
            Text(value!.Value).Should().Be("value");
        }

        [Fact]
        public async Task AppendsToWal() {
            await _memTable.TryWriteAsync(new StringKey("key"), Bytes("value"));

            _walRecords.Should().HaveCount(1);
        }

        [Fact]
        public async Task WalRecord_HasCorrectFormat() {
            await _memTable.TryWriteAsync(new StringKey("k"), Bytes("v"));

            var record = _walRecords[0].AsSpan();
            record[0].Should().Be(0x01);
            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(record[1..]);
            keyLen.Should().Be(1);
            Encoding.UTF8.GetString(record.Slice(5, keyLen)).Should().Be("k");
            Encoding.UTF8.GetString(record[(5 + keyLen)..]).Should().Be("v");
        }

        [Fact]
        public async Task SameKey_UpdatesValue() {
            await _memTable.TryWriteAsync(new StringKey("key"), Bytes("first"));
            await _memTable.TryWriteAsync(new StringKey("key"), Bytes("second"));

            _memTable.TryGet(new StringKey("key"), out var value).Should().BeTrue();
            Text(value!.Value).Should().Be("second");
        }

        [Fact]
        public async Task WalFailure_ReturnsFalseAndDoesNotCommitToMemory() {
            using var failingMemTable = new MemTable<StringKey>(1024, (_, _) => ValueTask.FromResult(false));

            var result = await failingMemTable.TryWriteAsync(new StringKey("key"), Bytes("value"));

            result.Should().BeFalse();
            failingMemTable.TryGet(new StringKey("key"), out _).Should().BeFalse();
        }

        [Fact]
        public async Task Returns_True_OnSuccess() {
            var result = await _memTable.TryWriteAsync(new StringKey("key"), Bytes("value"));

            result.Should().BeTrue();
        }
    }

    public class TryDeleteAsync : MemTableTests {
        [Fact]
        public async Task StoresTombstone_TryGetReturnsTrueWithNullValue() {
            await _memTable.TryWriteAsync(new StringKey("key"), Bytes("value"));
            await _memTable.TryDeleteAsync(new StringKey("key"));

            _memTable.TryGet(new StringKey("key"), out var value).Should().BeTrue();
            value.Should().BeNull();
        }

        [Fact]
        public async Task DeleteNonExistentKey_StoresTombstone() {
            await _memTable.TryDeleteAsync(new StringKey("key"));

            _memTable.TryGet(new StringKey("key"), out var value).Should().BeTrue();
            value.Should().BeNull();
        }

        [Fact]
        public async Task AppendsToWal() {
            await _memTable.TryDeleteAsync(new StringKey("key"));

            _walRecords.Should().HaveCount(1);
        }

        [Fact]
        public async Task WalRecord_HasTombstoneMarker() {
            await _memTable.TryDeleteAsync(new StringKey("k"));

            _walRecords[0][0].Should().Be(0x02);
        }

        [Fact]
        public async Task WalRecord_HasCorrectFormat() {
            await _memTable.TryDeleteAsync(new StringKey("k"));

            var record = _walRecords[0].AsSpan();
            record[0].Should().Be(0x02);
            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(record[1..]);
            keyLen.Should().Be(1);
            Encoding.UTF8.GetString(record.Slice(5, keyLen)).Should().Be("k");
            record.Length.Should().Be(6); // no value bytes
        }

        [Fact]
        public async Task WalFailure_ReturnsFalseAndDoesNotCommitToMemory() {
            using var failingMemTable = new MemTable<StringKey>(1024, (_, _) => ValueTask.FromResult(false));

            var result = await failingMemTable.TryDeleteAsync(new StringKey("key"));

            result.Should().BeFalse();
            failingMemTable.TryGet(new StringKey("key"), out _).Should().BeFalse();
        }

        [Fact]
        public async Task WriteAfterDelete_ResurrectsKey() {
            await _memTable.TryWriteAsync(new StringKey("key"), Bytes("original"));
            await _memTable.TryDeleteAsync(new StringKey("key"));
            await _memTable.TryWriteAsync(new StringKey("key"), Bytes("resurrected"));

            _memTable.TryGet(new StringKey("key"), out var value).Should().BeTrue();
            value.Should().NotBeNull();
            Text(value!.Value).Should().Be("resurrected");
        }
    }

    public class TryGet : MemTableTests {
        [Fact]
        public void AbsentKey_ReturnsFalse() {
            _memTable.TryGet(new StringKey("key"), out var value).Should().BeFalse();
            value.Should().BeNull();
        }

        [Fact]
        public async Task PresentKey_ReturnsTrueWithValue() {
            await _memTable.TryWriteAsync(new StringKey("key"), Bytes("value"));

            _memTable.TryGet(new StringKey("key"), out var value).Should().BeTrue();
            value.Should().NotBeNull();
            Text(value!.Value).Should().Be("value");
        }

        [Fact]
        public async Task TombstonedKey_ReturnsTrueWithNullValue() {
            await _memTable.TryDeleteAsync(new StringKey("key"));

            _memTable.TryGet(new StringKey("key"), out var value).Should().BeTrue();
            value.Should().BeNull();
        }
    }

    public class Size : MemTableTests {
        [Fact]
        public void StartsAtZero() =>
            _memTable.Size.Should().Be(0);

        [Fact]
        public async Task IncreasesOnWrite() {
            await _memTable.TryWriteAsync(new StringKey("k"), Bytes("v"));

            _memTable.Size.Should().Be(2); // 1 byte key + 1 byte value
        }

        [Fact]
        public async Task IncreasesOnDelete() {
            await _memTable.TryDeleteAsync(new StringKey("k"));

            _memTable.Size.Should().Be(1); // 1 byte key, no value
        }
    }

    public class IsFull : MemTableTests {
        [Fact]
        public void StartsNotFull() =>
            _memTable.IsFull.Should().BeFalse();

        [Fact]
        public async Task Full_WhenSizeReachesCapacity() {
            using var tinyMemTable = new MemTable<StringKey>(2, (_, _) => ValueTask.FromResult(true));

            await tinyMemTable.TryWriteAsync(new StringKey("k"), Bytes("v"));

            tinyMemTable.IsFull.Should().BeTrue();
        }

        [Fact]
        public async Task NotFull_WhenSizeBelowCapacity() {
            using var tinyMemTable = new MemTable<StringKey>(100, (_, _) => ValueTask.FromResult(true));

            await tinyMemTable.TryWriteAsync(new StringKey("k"), Bytes("v"));

            tinyMemTable.IsFull.Should().BeFalse();
        }
    }

    public class Enumerate : MemTableTests {
        [Fact]
        public void EmptyTable_YieldsNothing() =>
            _memTable.Should().BeEmpty();

        [Fact]
        public async Task SingleEntry_YieldsKeyAndValue() {
            await _memTable.TryWriteAsync(new StringKey("a"), Bytes("1"));

            _memTable.Should().ContainSingle(kv =>
                kv.Key == new StringKey("a") && kv.Value != null && Text(kv.Value.Value) == "1");
        }

        [Fact]
        public async Task MultipleEntries_YieldsInAscendingKeyOrder() {
            await _memTable.TryWriteAsync(new StringKey("c"), Bytes("3"));
            await _memTable.TryWriteAsync(new StringKey("a"), Bytes("1"));
            await _memTable.TryWriteAsync(new StringKey("b"), Bytes("2"));

            _memTable.Select(kv => kv.Key.Value).Should().BeInAscendingOrder();
        }

        [Fact]
        public async Task TombstonedKey_YieldsNullValue() {
            await _memTable.TryDeleteAsync(new StringKey("a"));

            var results = _memTable.ToList();
            results.Should().HaveCount(1);
            results[0].Key.Should().Be(new StringKey("a"));
            results[0].Value.Should().BeNull();
        }
    }

    public class ScanFrom : MemTableTests {
        [Fact]
        public void EmptyTable_YieldsNothing() =>
            _memTable.ScanFrom(new StringKey("b")).Should().BeEmpty();

        [Fact]
        public async Task ScanFromExistingKey_IncludesThatKeyAndAfter() {
            await _memTable.TryWriteAsync(new StringKey("a"), Bytes("1"));
            await _memTable.TryWriteAsync(new StringKey("b"), Bytes("2"));
            await _memTable.TryWriteAsync(new StringKey("c"), Bytes("3"));

            _memTable.ScanFrom(new StringKey("b"))
                     .Select(kv => kv.Key.Value)
                     .Should().Equal("b", "c");
        }

        [Fact]
        public async Task ScanFromBetweenKeys_StartsAtNextKey() {
            await _memTable.TryWriteAsync(new StringKey("a"), Bytes("1"));
            await _memTable.TryWriteAsync(new StringKey("c"), Bytes("3"));

            _memTable.ScanFrom(new StringKey("b"))
                     .Should().ContainSingle(kv => kv.Key == new StringKey("c"));
        }

        [Fact]
        public async Task ScanFromAfterLast_YieldsNothing() {
            await _memTable.TryWriteAsync(new StringKey("a"), Bytes("1"));

            _memTable.ScanFrom(new StringKey("b")).Should().BeEmpty();
        }

        [Fact]
        public async Task TombstonedKey_YieldsNullValue() {
            await _memTable.TryDeleteAsync(new StringKey("a"));

            var results = _memTable.ScanFrom(new StringKey("a")).ToList();
            results.Should().ContainSingle();
            results[0].Key.Should().Be(new StringKey("a"));
            results[0].Value.Should().BeNull();
        }
    }

    public class CountTests : MemTableTests {
        [Fact]
        public void StartsAtZero() =>
            _memTable.Count.Should().Be(0);

        [Fact]
        public async Task IncreasesOnWrite() {
            await _memTable.TryWriteAsync(new StringKey("a"), Bytes("1"));
            await _memTable.TryWriteAsync(new StringKey("b"), Bytes("2"));

            _memTable.Count.Should().Be(2);
        }

        [Fact]
        public async Task UnchangedOnUpdate() {
            await _memTable.TryWriteAsync(new StringKey("a"), Bytes("1"));
            await _memTable.TryWriteAsync(new StringKey("a"), Bytes("2"));

            _memTable.Count.Should().Be(1);
        }

        [Fact]
        public async Task TombstoneCountsAsEntry() {
            await _memTable.TryDeleteAsync(new StringKey("a"));

            _memTable.Count.Should().Be(1);
        }
    }
}
