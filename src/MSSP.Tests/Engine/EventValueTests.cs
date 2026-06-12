using FluentAssertions;

namespace MSSP.Engine;

public class EventValueTests {
    static readonly DateTimeOffset Timestamp = new(2024, 6, 15, 12, 0, 0, TimeSpan.Zero);
    static readonly StreamId StreamId = new("test-stream");
    static readonly EventKey Key = new(StreamId.Value, 7);

    public class From_ToRecordedEvent {
        [Fact]
        public void PreservesEventType() {
            var eventData = new EventData("OrderPlaced", "data"u8.ToArray());

            RecordedEvent result = ((EventValue)EventValue.From(eventData, Timestamp)).ToRecordedEvent(Key);

            result.EventType.Should().Be("OrderPlaced");
        }

        [Fact]
        public void PreservesData() {
            var payload = "hello world"u8.ToArray();
            var eventData = new EventData("MyEvent", payload);

            RecordedEvent result = ((EventValue)EventValue.From(eventData, Timestamp)).ToRecordedEvent(Key);

            result.Data.ToArray().Should().Equal(payload);
        }

        [Fact]
        public void PreservesMetadata() {
            var meta = "{ \"userId\": 42 }"u8.ToArray();
            var eventData = new EventData("MyEvent", "data"u8.ToArray(), meta);

            RecordedEvent result = ((EventValue)EventValue.From(eventData, Timestamp)).ToRecordedEvent(Key);

            result.Metadata.ToArray().Should().Equal(meta);
        }

        [Fact]
        public void WithoutMetadata_ReturnsEmptySlice() {
            var eventData = new EventData("MyEvent", "data"u8.ToArray());

            RecordedEvent result = ((EventValue)EventValue.From(eventData, Timestamp)).ToRecordedEvent(Key);

            result.Metadata.IsEmpty.Should().BeTrue();
        }

        [Fact]
        public void PreservesTimestamp() {
            var eventData = new EventData("MyEvent", "data"u8.ToArray());

            RecordedEvent result = ((EventValue)EventValue.From(eventData, Timestamp)).ToRecordedEvent(Key);

            result.Timestamp.Should().Be(Timestamp);
        }

        [Fact]
        public void PreservesStreamIdAndRevision() {
            var eventData = new EventData("MyEvent", "data"u8.ToArray());

            RecordedEvent result = ((EventValue)EventValue.From(eventData, Timestamp)).ToRecordedEvent(Key);

            result.StreamId.Should().Be(StreamId);
            result.Revision.Should().Be(7);
        }

        [Fact]
        public void DataAndMetadataAreIndependent() {
            var payload = "event-payload"u8.ToArray();
            var meta = "meta-payload"u8.ToArray();
            var eventData = new EventData("MyEvent", payload, meta);

            RecordedEvent result = ((EventValue)EventValue.From(eventData, Timestamp)).ToRecordedEvent(Key);

            result.Data.ToArray().Should().Equal(payload);
            result.Metadata.ToArray().Should().Equal(meta);
        }
    }

    public class From_ToSubscriptionEvent {
        [Fact]
        public void PreservesMetadata() {
            var meta = "{ \"correlationId\": \"abc\" }"u8.ToArray();
            var eventData = new EventData("MyEvent", "data"u8.ToArray(), meta);

            SubscriptionEvent result = ((EventValue)EventValue.From(eventData, Timestamp)).ToSubscriptionEvent(Key);

            result.Metadata.ToArray().Should().Equal(meta);
        }

        [Fact]
        public void WithoutMetadata_ReturnsEmptySlice() {
            var eventData = new EventData("MyEvent", "data"u8.ToArray());

            SubscriptionEvent result = ((EventValue)EventValue.From(eventData, Timestamp)).ToSubscriptionEvent(Key);

            result.Metadata.IsEmpty.Should().BeTrue();
        }

        [Fact]
        public void PreservesDataAlongsideMetadata() {
            var payload = "event-payload"u8.ToArray();
            var meta = "meta-payload"u8.ToArray();
            var eventData = new EventData("MyEvent", payload, meta);

            SubscriptionEvent result = ((EventValue)EventValue.From(eventData, Timestamp)).ToSubscriptionEvent(Key);

            result.Data.ToArray().Should().Equal(payload);
            result.Metadata.ToArray().Should().Equal(meta);
        }
    }
}
