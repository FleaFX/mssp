using System.Buffers.Binary;
using System.Text;

namespace MSSP;

/// <summary>
/// The binary encoding of a single event's payload as stored in the MemTable and SST files.
/// </summary>
/// <remarks>
/// Binary layout:
/// <code>
/// [typeLen: 4 bytes LE] [type: UTF-8] [timestamp: 8 bytes LE ms]
/// [dataLen: 4 bytes LE] [data bytes]
/// [metaLen: 4 bytes LE] [meta bytes]
/// [reserved: 8 bytes LE]
/// </code>
/// The last 8 bytes are a reserved slot written by the infrastructure layer (e.g. <c>SubscriptionPipeline</c>)
/// to store the <see cref="GlobalPosition"/>. They are zero when the value leaves <see cref="From"/>.
/// </remarks>
public readonly struct EventValue {
    readonly ReadOnlyMemory<byte> _bytes;

    EventValue(ReadOnlyMemory<byte> bytes) => _bytes = bytes;

    /// <summary>
    /// Encodes <paramref name="eventData"/> and <paramref name="timestamp"/> into a binary value.
    /// The last 8 bytes (reserved slot) are left as zero; the caller is responsible for injecting
    /// the <see cref="GlobalPosition"/> before persisting.
    /// </summary>
    public static Memory<byte> From(EventData eventData, DateTimeOffset timestamp) {
        var typeBytes = Encoding.UTF8.GetBytes(eventData.EventType);
        var buffer = new byte[4 + typeBytes.Length + 8 + 4 + eventData.Data.Length + 4 + eventData.Metadata.Length + 8];
        var span = buffer.AsSpan();
        var offset = 0;

        BinaryPrimitives.WriteInt32LittleEndian(span[offset..], typeBytes.Length);
        offset += 4;
        typeBytes.CopyTo(span[offset..]);
        offset += typeBytes.Length;
        BinaryPrimitives.WriteInt64LittleEndian(span[offset..], timestamp.ToUnixTimeMilliseconds());
        offset += 8;
        BinaryPrimitives.WriteInt32LittleEndian(span[offset..], eventData.Data.Length);
        offset += 4;
        eventData.Data.Span.CopyTo(span[offset..]);
        offset += eventData.Data.Length;
        BinaryPrimitives.WriteInt32LittleEndian(span[offset..], eventData.Metadata.Length);
        offset += 4;
        eventData.Metadata.Span.CopyTo(span[offset..]);
        // last 8 bytes default to zero (reserved slot)
        return buffer;
    }

    /// <summary>
    /// Reads the <see cref="GlobalPosition"/> from the reserved slot (last 8 bytes).
    /// </summary>
    public GlobalPosition ReadPosition() =>
        new(BinaryPrimitives.ReadUInt64LittleEndian(_bytes.Span[^8..]));

    /// <summary>
    /// Decodes this value back into a <see cref="RecordedEvent"/> using <paramref name="key"/> for the stream context.
    /// </summary>
    public RecordedEvent ToRecordedEvent(EventKey key) {
        var (eventType, timestamp, data, metadata) = Decode();
        return new RecordedEvent(key.StreamId, key.Revision, eventType, data, timestamp, metadata);
    }

    /// <summary>
    /// Decodes this value into a <see cref="SubscriptionEvent"/> using <paramref name="key"/> for the stream context.
    /// </summary>
    public SubscriptionEvent ToSubscriptionEvent(EventKey key) {
        var (eventType, timestamp, data, metadata) = Decode();
        return new SubscriptionEvent(key.StreamId, key.Revision, eventType, data, timestamp, ReadPosition(), metadata);
    }

    (string EventType, DateTimeOffset Timestamp, ReadOnlyMemory<byte> Data, ReadOnlyMemory<byte> Metadata) Decode() {
        var span = _bytes.Span;
        var offset = 0;

        var typeLen = BinaryPrimitives.ReadInt32LittleEndian(span[offset..]);
        offset += 4;
        var eventType = Encoding.UTF8.GetString(span.Slice(offset, typeLen));
        offset += typeLen;
        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(span[offset..]));
        offset += 8;
        var dataLen = BinaryPrimitives.ReadInt32LittleEndian(span[offset..]);
        offset += 4;
        var data = _bytes[offset..(offset + dataLen)];
        offset += dataLen;
        var metaLen = BinaryPrimitives.ReadInt32LittleEndian(span[offset..]);
        offset += 4;
        var metadata = _bytes[offset..(offset + metaLen)];

        return (eventType, timestamp, data, metadata);
    }

    /// <summary>
    /// Returns the underlying bytes of this value.
    /// </summary>
    public static implicit operator ReadOnlyMemory<byte>(EventValue value) => value._bytes;

    /// <summary>
    /// Wraps <paramref name="bytes"/> as an <see cref="EventValue"/> without copying.
    /// </summary>
    public static implicit operator EventValue(ReadOnlyMemory<byte> bytes) => new(bytes);

    /// <summary>
    /// Wraps <paramref name="bytes"/> as an <see cref="EventValue"/> without copying.
    /// </summary>
    public static implicit operator EventValue(Memory<byte> bytes) => new(bytes);
}
