namespace MSSP;

/// <summary>
/// Represents an event as stored in the event store, including its metadata.
/// </summary>
public readonly struct RecordedEvent(StreamId streamId, ulong revision, string eventType, ReadOnlyMemory<byte> data, DateTimeOffset timestamp, ReadOnlyMemory<byte> metadata = default) {
    /// <summary>
    /// Gets the identifier of the stream this event belongs to.
    /// </summary>
    public StreamId StreamId { get; } = streamId;

    /// <summary>
    /// Gets the revision of this event within the stream.
    /// </summary>
    public ulong Revision { get; } = revision;

    /// <summary>
    /// Gets the event type name.
    /// </summary>
    public string EventType { get; } = eventType;

    /// <summary>
    /// Gets the binary payload of the event.
    /// </summary>
    public ReadOnlyMemory<byte> Data { get; } = data;

    /// <summary>
    /// Gets the timestamp at which the event was recorded.
    /// </summary>
    public DateTimeOffset Timestamp { get; } = timestamp;

    /// <summary>
    /// Gets the metadata associated with the event, or an empty slice when no metadata was stored.
    /// </summary>
    public ReadOnlyMemory<byte> Metadata { get; } = metadata;
}
