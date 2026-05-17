using System.Buffers.Binary;
using System.Text;
using MSSP.LsmTree;

namespace MSSP.Embedded;

/// <summary>
/// Composite key that uniquely identifies a single event by stream and revision.
/// Events are ordered first by stream identifier, then by revision within a stream.
/// </summary>
/// <remarks>
/// Binary layout: [streamIdLen: 4 bytes LE] [streamId: UTF-8] [revision: 8 bytes LE]
/// </remarks>
public readonly struct EventKey(string streamId, ulong revision) : IKey<EventKey> {
    /// <summary>
    /// Gets the stream identifier.
    /// </summary>
    public string StreamId { get; } = streamId;

    /// <summary>
    /// Gets the revision of the event within the stream.
    /// </summary>
    public ulong Revision { get; } = revision;

    /// <inheritdoc/>
    public bool Equals(EventKey other) => StreamId == other.StreamId && Revision == other.Revision;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is EventKey other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => HashCode.Combine(StreamId, Revision);

    /// <inheritdoc/>
    public int CompareTo(EventKey other) {
        var streamCompare = string.Compare(StreamId, other.StreamId, StringComparison.Ordinal);
        return streamCompare != 0 ? streamCompare : Revision.CompareTo(other.Revision);
    }

    /// <inheritdoc/>
    public static implicit operator ReadOnlyMemory<byte>(EventKey key) {
        var streamIdBytes = Encoding.UTF8.GetBytes(key.StreamId);
        var buffer = new byte[4 + streamIdBytes.Length + 8];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, streamIdBytes.Length);
        streamIdBytes.CopyTo(buffer, 4);
        BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(4 + streamIdBytes.Length), key.Revision);
        return buffer;
    }

    /// <inheritdoc/>
    public static implicit operator EventKey(ReadOnlyMemory<byte> memory) {
        var span = memory.Span;
        var streamIdLen = BinaryPrimitives.ReadInt32LittleEndian(span);
        var streamId = Encoding.UTF8.GetString(span.Slice(4, streamIdLen));
        var revision = BinaryPrimitives.ReadUInt64LittleEndian(span[(4 + streamIdLen)..]);
        return new EventKey(streamId, revision);
    }
}
