namespace MSSP;

/// <summary>
/// Represents an event to be appended to a stream.
/// </summary>
public readonly struct EventData(string eventType, ReadOnlyMemory<byte> data, ReadOnlyMemory<byte> metadata = default) {
    /// <summary>
    /// Gets the event type name.
    /// </summary>
    public string EventType { get; } = eventType;

    /// <summary>
    /// Gets the binary payload of the event.
    /// </summary>
    public ReadOnlyMemory<byte> Data { get; } = data;

    /// <summary>
    /// Gets the metadata associated with the event, or an empty slice when no metadata was provided.
    /// </summary>
    public ReadOnlyMemory<byte> Metadata { get; } = metadata;
}
