using System.Text;
using FluentAssertions;
using MSSP.BloomFilters;

namespace MSSP.Engine.BloomFilters;

public class BloomFilterTests {
    public class MayContain {
        [Fact]
        public void NeverReturnsFalseNegative() {
            var filter = BloomFilter.Create(100);
            var items = Enumerable.Range(0, 100)
                .Select(i => Encoding.UTF8.GetBytes($"item-{i}"))
                .ToList();

            foreach (var item in items)
                filter.Add(item);

            foreach (var item in items)
                filter.MayContain(item).Should().BeTrue(because: "added items must always be found");
        }

        [Fact]
        public void ReturnsFalseForItemDefinitelyNotAdded() {
            var filter = BloomFilter.Create(1000);
            filter.Add("hello"u8);

            filter.MayContain("world"u8).Should().BeFalse();
        }

        [Fact]
        public void EmptyFilterReturnsFalseForAnyItem() {
            var filter = BloomFilter.Create(100);

            filter.MayContain("anything"u8).Should().BeFalse();
        }
    }

    public class Serialization {
        [Fact]
        public void RoundTripPreservesPositiveLookups() {
            var filter = BloomFilter.Create(100);
            filter.Add("key1"u8);
            filter.Add("key2"u8);

            var stream = new MemoryStream();
            filter.WriteTo(stream);
            stream.Seek(0, SeekOrigin.Begin);
            var loaded = BloomFilter.ReadFrom(stream);

            loaded.MayContain("key1"u8).Should().BeTrue();
            loaded.MayContain("key2"u8).Should().BeTrue();
        }

        [Fact]
        public void RoundTripPreservesNegativeLookups() {
            var filter = BloomFilter.Create(100);
            filter.Add("key1"u8);

            var stream = new MemoryStream();
            filter.WriteTo(stream);
            stream.Seek(0, SeekOrigin.Begin);
            var loaded = BloomFilter.ReadFrom(stream);

            loaded.MayContain("absent"u8).Should().BeFalse();
        }

        [Fact]
        public void ReadFrom_Throws_OnInvalidHeader() {
            var stream = new MemoryStream("\0\0\0\0\0\0\0\0"u8.ToArray());

            var act = () => BloomFilter.ReadFrom(stream);

            act.Should().Throw<InvalidDataException>();
        }
    }

    public class Create {
        [Theory]
        [InlineData(1)]
        [InlineData(100)]
        [InlineData(10000)]
        public void AcceptsVariousExpectedCounts(int n) {
            var act = () => BloomFilter.Create(n);
            act.Should().NotThrow();
        }

        [Fact]
        public void Throws_WhenExpectedItemsIsZero() {
            var act = () => BloomFilter.Create(0);
            act.Should().Throw<ArgumentOutOfRangeException>();
        }
    }
}
