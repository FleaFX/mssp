using System.Buffers.Binary;
using System.Text;

namespace MSSP;

/// <summary>
/// The binary encoding of a single event's payload as stored in the MemTable and SST files.
/// </summary>
/// <remarks>
/// Binary layout: [typeLen: 4 bytes LE] [type: UTF-8] [timestamp: 8 bytes LE ms] [globalPosition: 8 bytes LE] [data bytes]
/// </remarks>
public readonly struct EventValue {
    readonly ReadOnlyMemory<byte> _bytes;

    EventValue(ReadOnlyMemory<byte> bytes) => _bytes = bytes;

    /// <summary>
    /// Encodes <paramref name="eventData"/>, <paramref name="timestamp"/>, and <paramref name="position"/> into a binary value.
    /// </summary>
    public static EventValue From(EventData eventData, DateTimeOffset timestamp, GlobalPosition position) {
        var typeBytes = Encoding.UTF8.GetBytes(eventData.EventType);
        var buffer = new byte[4 + typeBytes.Length + 8 + 8 + eventData.Data.Length];
        var span = buffer.AsSpan();
        BinaryPrimitives.WriteInt32LittleEndian(span, typeBytes.Length);
        typeBytes.CopyTo(span[4..]);
        BinaryPrimitives.WriteInt64LittleEndian(span[(4 + typeBytes.Length)..], timestamp.ToUnixTimeMilliseconds());
        BinaryPrimitives.WriteUInt64LittleEndian(span[(4 + typeBytes.Length + 8)..], position.Value);
        eventData.Data.Span.CopyTo(span[(4 + typeBytes.Length + 16)..]);
        return new(buffer);
    }

    /// <summary>
    /// Reads the <see cref="GlobalPosition"/> without fully decoding the value.
    /// </summary>
    public GlobalPosition ReadPosition() {
        var span = _bytes.Span;
        var typeLen = BinaryPrimitives.ReadInt32LittleEndian(span);
        return new GlobalPosition(BinaryPrimitives.ReadUInt64LittleEndian(span[(4 + typeLen + 8)..]));
    }

    /// <summary>
    /// Decodes this value back into a <see cref="RecordedEvent"/> using <paramref name="key"/> for the stream context.
    /// </summary>
    public RecordedEvent ToRecordedEvent(EventKey key) {
        var span = _bytes.Span;
        var typeLen = BinaryPrimitives.ReadInt32LittleEndian(span);
        var eventType = Encoding.UTF8.GetString(span.Slice(4, typeLen));
        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(span[(4 + typeLen)..]));
        return new RecordedEvent(key.StreamId, key.Revision, eventType, _bytes[(4 + typeLen + 16)..], timestamp);
    }

    /// <summary>
    /// Decodes this value into a <see cref="SubscriptionEvent"/> using <paramref name="key"/> for the stream context.
    /// </summary>
    public SubscriptionEvent ToSubscriptionEvent(EventKey key) {
        var span = _bytes.Span;
        var typeLen = BinaryPrimitives.ReadInt32LittleEndian(span);
        var eventType = Encoding.UTF8.GetString(span.Slice(4, typeLen));
        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(span[(4 + typeLen)..]));
        var position = new GlobalPosition(BinaryPrimitives.ReadUInt64LittleEndian(span[(4 + typeLen + 8)..]));
        return new SubscriptionEvent(key.StreamId, key.Revision, eventType, _bytes[(4 + typeLen + 16)..], timestamp, position);
    }

    /// <inheritdoc/>
    public static implicit operator ReadOnlyMemory<byte>(EventValue value) => value._bytes;

    /// <inheritdoc/>
    public static implicit operator EventValue(ReadOnlyMemory<byte> bytes) => new(bytes);
}
