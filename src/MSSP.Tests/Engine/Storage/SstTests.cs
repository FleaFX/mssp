using System.Text;
using FluentAssertions;
using MSSP.Storage;

namespace MSSP.Engine.Storage;

public class SstTests {
    static KeyValuePair<StringKey, ReadOnlyMemory<byte>?> Entry(string key, string value) =>
        new(new StringKey(key), Encoding.UTF8.GetBytes(value));

    static KeyValuePair<StringKey, ReadOnlyMemory<byte>?> Tombstone(string key) =>
        new(new StringKey(key), null);

    static string Text(ReadOnlyMemory<byte> bytes) => Encoding.UTF8.GetString(bytes.Span);

    async Task<MemoryStream> WriteAsync(
        IEnumerable<KeyValuePair<StringKey, ReadOnlyMemory<byte>?>> entries,
        int sparseInterval = Sst.DefaultSparseInterval) {
        var stream = new MemoryStream();
        await SstWriter.WriteAsync(entries, stream, sparseInterval);
        stream.Position = 0;
        return stream;
    }

    public class Write : SstTests {
        [Fact]
        public async Task EmptyInput_WritesValidFile() {
            var stream = await WriteAsync([]);

            stream.Length.Should().Be(Sst.FooterSize);
        }

        [Fact]
        public async Task SingleEntry_WritesReadableFile() {
            var stream = await WriteAsync([Entry("k", "v")]);
            using var reader = new SstReader<StringKey>(stream);

            reader.TryGet(new StringKey("k"), out var value).Should().BeTrue();
            value.Should().NotBeNull();
            Text(value!.Value).Should().Be("v");
        }

        [Fact]
        public async Task Tombstone_WritesReadableFile() {
            var stream = await WriteAsync([Tombstone("k")]);
            using var reader = new SstReader<StringKey>(stream);

            reader.TryGet(new StringKey("k"), out var value).Should().BeTrue();
            value.Should().BeNull();
        }
    }

    public class TryGet : SstTests {
        [Fact]
        public async Task PresentKey_ReturnsTrueWithValue() {
            var stream = await WriteAsync([Entry("a", "1"), Entry("b", "2"), Entry("c", "3")]);
            using var reader = new SstReader<StringKey>(stream);

            reader.TryGet(new StringKey("b"), out var value).Should().BeTrue();
            value.Should().NotBeNull();
            Text(value!.Value).Should().Be("2");
        }

        [Fact]
        public async Task AbsentKey_ReturnsFalse() {
            var stream = await WriteAsync([Entry("a", "1"), Entry("c", "3")]);
            using var reader = new SstReader<StringKey>(stream);

            reader.TryGet(new StringKey("b"), out var value).Should().BeFalse();
            value.Should().BeNull();
        }

        [Fact]
        public async Task TombstonedKey_ReturnsTrueWithNullValue() {
            var stream = await WriteAsync([Entry("a", "1"), Tombstone("b"), Entry("c", "3")]);
            using var reader = new SstReader<StringKey>(stream);

            reader.TryGet(new StringKey("b"), out var value).Should().BeTrue();
            value.Should().BeNull();
        }

        [Fact]
        public async Task KeyBeforeFirst_ReturnsFalse() {
            var stream = await WriteAsync([Entry("b", "2"), Entry("c", "3")]);
            using var reader = new SstReader<StringKey>(stream);

            reader.TryGet(new StringKey("a"), out var value).Should().BeFalse();
            value.Should().BeNull();
        }

        [Fact]
        public async Task KeyAfterLast_ReturnsFalse() {
            var stream = await WriteAsync([Entry("a", "1"), Entry("b", "2")]);
            using var reader = new SstReader<StringKey>(stream);

            reader.TryGet(new StringKey("z"), out var value).Should().BeFalse();
            value.Should().BeNull();
        }

        [Fact]
        public async Task KeyInSecondBlock_FoundAcrossSparseIndexBoundary() {
            // sparseInterval=2: index entries at positions 0, 2, 4, ...
            var entries = Enumerable.Range(0, 6)
                .Select(i => Entry(i.ToString("D3"), $"val{i}"))
                .ToList();
            var stream = await WriteAsync(entries, sparseInterval: 2);
            using var reader = new SstReader<StringKey>(stream);

            reader.TryGet(new StringKey("003"), out var value).Should().BeTrue();
            Text(value!.Value).Should().Be("val3");
        }

        [Fact]
        public async Task EmptySst_ReturnsFalse() {
            var stream = await WriteAsync([]);
            using var reader = new SstReader<StringKey>(stream);

            reader.TryGet(new StringKey("k"), out var value).Should().BeFalse();
            value.Should().BeNull();
        }
    }

    public class Scan : SstTests {
        [Fact]
        public async Task EmptySst_YieldsNothing() {
            var stream = await WriteAsync([]);
            using var reader = new SstReader<StringKey>(stream);

            reader.Scan().Should().BeEmpty();
        }

        [Fact]
        public async Task YieldsAllEntriesInAscendingOrder() {
            var entries = new[] {
                Entry("a", "1"),
                Entry("b", "2"),
                Entry("c", "3"),
            };
            var stream = await WriteAsync(entries);
            using var reader = new SstReader<StringKey>(stream);

            var results = reader.Scan().ToList();
            results.Should().HaveCount(3);
            results.Select(kv => kv.Key.Value).Should().BeInAscendingOrder();
        }

        [Fact]
        public async Task TombstoneEntry_YieldsNullValue() {
            var stream = await WriteAsync([Entry("a", "1"), Tombstone("b"), Entry("c", "3")]);
            using var reader = new SstReader<StringKey>(stream);

            var results = reader.Scan().ToList();
            results[1].Key.Should().Be(new StringKey("b"));
            results[1].Value.Should().BeNull();
        }

        [Fact]
        public async Task MultipleScans_EachYieldsAllEntries() {
            var stream = await WriteAsync([Entry("a", "1"), Entry("b", "2")]);
            using var reader = new SstReader<StringKey>(stream);

            reader.Scan().Should().HaveCount(2);
            reader.Scan().Should().HaveCount(2);
        }
    }

    public class ScanFrom : SstTests {
        [Fact]
        public async Task EmptySst_YieldsNothing() {
            var stream = await WriteAsync([]);
            using var reader = new SstReader<StringKey>(stream);

            reader.Scan(new StringKey("a")).Should().BeEmpty();
        }

        [Fact]
        public async Task ScanFromExistingKey_IncludesThatKeyAndAfter() {
            var stream = await WriteAsync([Entry("a", "1"), Entry("b", "2"), Entry("c", "3")]);
            using var reader = new SstReader<StringKey>(stream);

            reader.Scan(new StringKey("b"))
                  .Select(kv => kv.Key.Value)
                  .Should().Equal("b", "c");
        }

        [Fact]
        public async Task ScanFromBetweenKeys_StartsAtNextKey() {
            var stream = await WriteAsync([Entry("a", "1"), Entry("c", "3")]);
            using var reader = new SstReader<StringKey>(stream);

            reader.Scan(new StringKey("b"))
                  .Should().ContainSingle(kv => kv.Key == new StringKey("c"));
        }

        [Fact]
        public async Task ScanFromBeforeFirst_YieldsAll() {
            var stream = await WriteAsync([Entry("b", "2"), Entry("c", "3")]);
            using var reader = new SstReader<StringKey>(stream);

            reader.Scan(new StringKey("a")).Should().HaveCount(2);
        }

        [Fact]
        public async Task ScanFromAfterLast_YieldsNothing() {
            var stream = await WriteAsync([Entry("a", "1"), Entry("b", "2")]);
            using var reader = new SstReader<StringKey>(stream);

            reader.Scan(new StringKey("c")).Should().BeEmpty();
        }

        [Fact]
        public async Task ScanFromAcrossSparseIndexBoundary() {
            // sparseInterval=2: blocks start at entries 0, 2, 4
            var entries = Enumerable.Range(0, 6).Select(i => Entry(i.ToString("D3"), $"val{i}")).ToList();
            var stream = await WriteAsync(entries, sparseInterval: 2);
            using var reader = new SstReader<StringKey>(stream);

            reader.Scan(new StringKey("003"))
                  .Select(kv => kv.Key.Value)
                  .Should().Equal("003", "004", "005");
        }

        [Fact]
        public async Task TombstoneEntry_YieldsNullValue() {
            var stream = await WriteAsync([Entry("a", "1"), Tombstone("b"), Entry("c", "3")]);
            using var reader = new SstReader<StringKey>(stream);

            var results = reader.Scan(new StringKey("b")).ToList();
            results[0].Key.Should().Be(new StringKey("b"));
            results[0].Value.Should().BeNull();
        }
    }

    public class Validation : SstTests {
        [Fact]
        public void InvalidMagic_ThrowsInvalidDataException() {
            var stream = new MemoryStream(new byte[Sst.FooterSize]);

            var act = () => new SstReader<StringKey>(stream);

            act.Should().Throw<InvalidDataException>();
        }
    }
}
