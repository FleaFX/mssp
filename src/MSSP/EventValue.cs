using System.Buffers.Binary;
using System.Text;

namespace MSSP;

/// <summary>
/// The binary encoding of a single event's payload as stored in the MemTable and SST files.
/// </summary>
/// <remarks>
/// Binary layout: [typeLen: 4 bytes LE] [type: UTF-8] [timestamp: 8 bytes LE ms] [data bytes] [reserved: 8 bytes LE]
///
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
        var buffer = new byte[4 + typeBytes.Length + 8 + eventData.Data.Length + 8];
        var span = buffer.AsSpan();
        BinaryPrimitives.WriteInt32LittleEndian(span, typeBytes.Length);
        typeBytes.CopyTo(span[4..]);
        BinaryPrimitives.WriteInt64LittleEndian(span[(4 + typeBytes.Length)..], timestamp.ToUnixTimeMilliseconds());
        eventData.Data.Span.CopyTo(span[(4 + typeBytes.Length + 8)..]);
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
        var span = _bytes.Span;
        var typeLen = BinaryPrimitives.ReadInt32LittleEndian(span);
        var eventType = Encoding.UTF8.GetString(span.Slice(4, typeLen));
        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(span[(4 + typeLen)..]));
        return new RecordedEvent(key.StreamId, key.Revision, eventType, _bytes[(4 + typeLen + 8)..^8], timestamp);
    }

    /// <summary>
    /// Decodes this value into a <see cref="SubscriptionEvent"/> using <paramref name="key"/> for the stream context.
    /// </summary>
    public SubscriptionEvent ToSubscriptionEvent(EventKey key) {
        var span = _bytes.Span;
        var typeLen = BinaryPrimitives.ReadInt32LittleEndian(span);
        var eventType = Encoding.UTF8.GetString(span.Slice(4, typeLen));
        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(span[(4 + typeLen)..]));
        return new SubscriptionEvent(key.StreamId, key.Revision, eventType, _bytes[(4 + typeLen + 8)..^8], timestamp, ReadPosition());
    }

    /// <inheritdoc/>
    public static implicit operator ReadOnlyMemory<byte>(EventValue value) => value._bytes;

    /// <inheritdoc/>
    public static implicit operator EventValue(ReadOnlyMemory<byte> bytes) => new(bytes);

    /// <inheritdoc/>
    public static implicit operator EventValue(Memory<byte> bytes) => new(bytes);
}
