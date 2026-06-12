using FluentAssertions;

namespace MSSP.Engine;

public class EventKeyTests {
    public class Ordering {
        [Fact]
        public void SameStream_LowerRevision_ComesFirst() {
            var a = new EventKey("stream", 0);
            var b = new EventKey("stream", 1);

            a.CompareTo(b).Should().BeNegative();
        }

        [Fact]
        public void LexicographicallyEarlierStream_ComesFirst() {
            var a = new EventKey("a", 99);
            var b = new EventKey("b", 0);

            a.CompareTo(b).Should().BeNegative();
        }

        [Fact]
        public void SameKey_ComparesToZero() {
            var a = new EventKey("stream", 5);
            var b = new EventKey("stream", 5);

            a.CompareTo(b).Should().Be(0);
        }
    }

    public class Serialization {
        [Fact]
        public void RoundTrip_PreservesStreamIdAndRevision() {
            var original = new EventKey("my-stream", 42);

            ReadOnlyMemory<byte> bytes = original;
            EventKey restored = bytes;

            restored.StreamId.Should().Be(original.StreamId);
            restored.Revision.Should().Be(original.Revision);
        }

        [Fact]
        public void RoundTrip_WithUnicodeStreamId() {
            var original = new EventKey("stroom-ë", 0);

            ReadOnlyMemory<byte> bytes = original;
            EventKey restored = bytes;

            restored.StreamId.Should().Be(original.StreamId);
        }
    }
}
