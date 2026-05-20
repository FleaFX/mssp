using System.Text;
using FluentAssertions;

namespace MSSP.Storage;

public class MemTableTests : IDisposable {
    readonly MemTable<StringKey> _memTable = new(1024);

    public void Dispose() => _memTable.Dispose();

    static ReadOnlyMemory<byte> Bytes(string value) => Encoding.UTF8.GetBytes(value);
    static string Text(ReadOnlyMemory<byte> bytes) => Encoding.UTF8.GetString(bytes.Span);

    public class ApplyWrite : MemTableTests {
        [Fact]
        public void StoresValue_RetrievableAfterApply() {
            _memTable.ApplyRecord(WalRecord.From(new StringKey("key"), Bytes("value")));

            _memTable.TryGet(new StringKey("key"), out var value).Should().BeTrue();
            value.Should().NotBeNull();
            Text(value!.Value).Should().Be("value");
        }

        [Fact]
        public void SameKey_UpdatesValue() {
            _memTable.ApplyRecord(WalRecord.From(new StringKey("key"), Bytes("first")));
            _memTable.ApplyRecord(WalRecord.From(new StringKey("key"), Bytes("second")));

            _memTable.TryGet(new StringKey("key"), out var value).Should().BeTrue();
            Text(value!.Value).Should().Be("second");
        }
    }

    public class ApplyDelete : MemTableTests {
        [Fact]
        public void StoresTombstone_TryGetReturnsTrueWithNullValue() {
            _memTable.ApplyRecord(WalRecord.From(new StringKey("key"), Bytes("value")));
            _memTable.ApplyRecord(WalRecord.Tombstone(new StringKey("key")));

            _memTable.TryGet(new StringKey("key"), out var value).Should().BeTrue();
            value.Should().BeNull();
        }

        [Fact]
        public void DeleteNonExistentKey_StoresTombstone() {
            _memTable.ApplyRecord(WalRecord.Tombstone(new StringKey("key")));

            _memTable.TryGet(new StringKey("key"), out var value).Should().BeTrue();
            value.Should().BeNull();
        }

        [Fact]
        public void WriteAfterDelete_ResurrectsKey() {
            _memTable.ApplyRecord(WalRecord.From(new StringKey("key"), Bytes("original")));
            _memTable.ApplyRecord(WalRecord.Tombstone(new StringKey("key")));
            _memTable.ApplyRecord(WalRecord.From(new StringKey("key"), Bytes("resurrected")));

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
        public void PresentKey_ReturnsTrueWithValue() {
            _memTable.ApplyRecord(WalRecord.From(new StringKey("key"), Bytes("value")));

            _memTable.TryGet(new StringKey("key"), out var value).Should().BeTrue();
            value.Should().NotBeNull();
            Text(value!.Value).Should().Be("value");
        }

        [Fact]
        public void TombstonedKey_ReturnsTrueWithNullValue() {
            _memTable.ApplyRecord(WalRecord.Tombstone(new StringKey("key")));

            _memTable.TryGet(new StringKey("key"), out var value).Should().BeTrue();
            value.Should().BeNull();
        }
    }

    public class Size : MemTableTests {
        [Fact]
        public void StartsAtZero() =>
            _memTable.Size.Should().Be(0);

        [Fact]
        public void IncreasesOnWrite() {
            _memTable.ApplyRecord(WalRecord.From(new StringKey("k"), Bytes("v")));

            _memTable.Size.Should().Be(2); // 1 byte key + 1 byte value
        }

        [Fact]
        public void IncreasesOnDelete() {
            _memTable.ApplyRecord(WalRecord.Tombstone(new StringKey("k")));

            _memTable.Size.Should().Be(1); // 1 byte key, no value
        }
    }

    public class IsFull : MemTableTests {
        [Fact]
        public void StartsNotFull() =>
            _memTable.IsFull.Should().BeFalse();

        [Fact]
        public void Full_WhenSizeReachesCapacity() {
            using var tinyMemTable = new MemTable<StringKey>(2);
            tinyMemTable.ApplyRecord(WalRecord.From(new StringKey("k"), Bytes("v")));
            tinyMemTable.IsFull.Should().BeTrue();
        }

        [Fact]
        public void NotFull_WhenSizeBelowCapacity() {
            using var tinyMemTable = new MemTable<StringKey>(100);
            tinyMemTable.ApplyRecord(WalRecord.From(new StringKey("k"), Bytes("v")));
            tinyMemTable.IsFull.Should().BeFalse();
        }
    }

    public class Enumerate : MemTableTests {
        [Fact]
        public void EmptyTable_YieldsNothing() =>
            _memTable.Should().BeEmpty();

        [Fact]
        public void SingleEntry_YieldsKeyAndValue() {
            _memTable.ApplyRecord(WalRecord.From(new StringKey("a"), Bytes("1")));

            _memTable.Should().ContainSingle(kv =>
                kv.Key == new StringKey("a") && kv.Value != null && Text(kv.Value.Value) == "1");
        }

        [Fact]
        public void MultipleEntries_YieldsInAscendingKeyOrder() {
            _memTable.ApplyRecord(WalRecord.From(new StringKey("c"), Bytes("3")));
            _memTable.ApplyRecord(WalRecord.From(new StringKey("a"), Bytes("1")));
            _memTable.ApplyRecord(WalRecord.From(new StringKey("b"), Bytes("2")));

            _memTable.Select(kv => kv.Key.Value).Should().BeInAscendingOrder();
        }

        [Fact]
        public void TombstonedKey_YieldsNullValue() {
            _memTable.ApplyRecord(WalRecord.Tombstone(new StringKey("a")));

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
        public void ScanFromExistingKey_IncludesThatKeyAndAfter() {
            _memTable.ApplyRecord(WalRecord.From(new StringKey("a"), Bytes("1")));
            _memTable.ApplyRecord(WalRecord.From(new StringKey("b"), Bytes("2")));
            _memTable.ApplyRecord(WalRecord.From(new StringKey("c"), Bytes("3")));

            _memTable.ScanFrom(new StringKey("b"))
                     .Select(kv => kv.Key.Value)
                     .Should().Equal("b", "c");
        }

        [Fact]
        public void ScanFromBetweenKeys_StartsAtNextKey() {
            _memTable.ApplyRecord(WalRecord.From(new StringKey("a"), Bytes("1")));
            _memTable.ApplyRecord(WalRecord.From(new StringKey("c"), Bytes("3")));

            _memTable.ScanFrom(new StringKey("b"))
                     .Should().ContainSingle(kv => kv.Key == new StringKey("c"));
        }

        [Fact]
        public void ScanFromAfterLast_YieldsNothing() {
            _memTable.ApplyRecord(WalRecord.From(new StringKey("a"), Bytes("1")));

            _memTable.ScanFrom(new StringKey("b")).Should().BeEmpty();
        }

        [Fact]
        public void TombstonedKey_YieldsNullValue() {
            _memTable.ApplyRecord(WalRecord.Tombstone(new StringKey("a")));

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
        public void IncreasesOnWrite() {
            _memTable.ApplyRecord(WalRecord.From(new StringKey("a"), Bytes("1")));
            _memTable.ApplyRecord(WalRecord.From(new StringKey("b"), Bytes("2")));

            _memTable.Count.Should().Be(2);
        }

        [Fact]
        public void UnchangedOnUpdate() {
            _memTable.ApplyRecord(WalRecord.From(new StringKey("a"), Bytes("1")));
            _memTable.ApplyRecord(WalRecord.From(new StringKey("a"), Bytes("2")));

            _memTable.Count.Should().Be(1);
        }

        [Fact]
        public void TombstoneCountsAsEntry() {
            _memTable.ApplyRecord(WalRecord.Tombstone(new StringKey("a")));

            _memTable.Count.Should().Be(1);
        }
    }
}
