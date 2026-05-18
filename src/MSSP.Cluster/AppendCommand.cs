using System.Buffers.Binary;
using System.Text;

namespace MSSP.Cluster;

// Binary layout:
//   [streamIdLen:4LE][streamId:UTF8][expectedRevision:8LE][eventCount:4LE]
//   per event: [typeLen:4LE][type:UTF8][dataLen:4LE][data:bytes]
static class AppendCommand {
    public static ReadOnlyMemory<byte> Serialize(string streamId, long expectedRevision, IEnumerable<EventData> events) {
        var eventList = events as IReadOnlyList<EventData> ?? events.ToArray();
        var streamIdBytes = Encoding.UTF8.GetBytes(streamId);

        var size = 4 + streamIdBytes.Length + 8 + 4;
        var typeBytes = new byte[eventList.Count][];
        for (var i = 0; i < eventList.Count; i++) {
            typeBytes[i] = Encoding.UTF8.GetBytes(eventList[i].EventType);
            size += 4 + typeBytes[i].Length + 4 + eventList[i].Data.Length;
        }

        var buf = new byte[size];
        var span = buf.AsSpan();

        BinaryPrimitives.WriteInt32LittleEndian(span, streamIdBytes.Length);
        span = span[4..];
        streamIdBytes.CopyTo(span);
        span = span[streamIdBytes.Length..];
        BinaryPrimitives.WriteInt64LittleEndian(span, expectedRevision);
        span = span[8..];
        BinaryPrimitives.WriteInt32LittleEndian(span, eventList.Count);
        span = span[4..];

        for (var i = 0; i < eventList.Count; i++) {
            BinaryPrimitives.WriteInt32LittleEndian(span, typeBytes[i].Length);
            span = span[4..];
            typeBytes[i].CopyTo(span);
            span = span[typeBytes[i].Length..];
            BinaryPrimitives.WriteInt32LittleEndian(span, eventList[i].Data.Length);
            span = span[4..];
            eventList[i].Data.Span.CopyTo(span);
            span = span[eventList[i].Data.Length..];
        }

        return buf;
    }

    public static (string StreamId, long ExpectedRevision, EventData[] Events) Deserialize(ReadOnlyMemory<byte> payload) {
        var span = payload.Span;

        var streamIdLen = BinaryPrimitives.ReadInt32LittleEndian(span);
        span = span[4..];
        var streamId = Encoding.UTF8.GetString(span[..streamIdLen]);
        span = span[streamIdLen..];
        var expectedRevision = BinaryPrimitives.ReadInt64LittleEndian(span);
        span = span[8..];
        var eventCount = BinaryPrimitives.ReadInt32LittleEndian(span);
        span = span[4..];

        var events = new EventData[eventCount];
        for (var i = 0; i < eventCount; i++) {
            var typeLen = BinaryPrimitives.ReadInt32LittleEndian(span);
            span = span[4..];
            var type = Encoding.UTF8.GetString(span[..typeLen]);
            span = span[typeLen..];
            var dataLen = BinaryPrimitives.ReadInt32LittleEndian(span);
            span = span[4..];
            var data = span[..dataLen].ToArray();
            span = span[dataLen..];
            events[i] = new EventData(type, data);
        }

        return (streamId, expectedRevision, events);
    }
}
