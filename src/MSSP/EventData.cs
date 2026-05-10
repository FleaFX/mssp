namespace MSSP;

/// <summary>
/// Represents an event to be appended to a stream.
/// </summary>
public readonly struct EventData(string eventType, ReadOnlyMemory<byte> data) {
    /// <summary>
    /// Gets the event type name.
    /// </summary>
    public string EventType { get; } = eventType;

    /// <summary>
    /// Gets the binary payload of the event.
    /// </summary>
    public ReadOnlyMemory<byte> Data { get; } = data;
}
