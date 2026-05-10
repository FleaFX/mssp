namespace MSSP;

/// <summary>
/// Identifies a stream of events.
/// </summary>
public readonly struct StreamId(string value) {
    /// <summary>
    /// Gets the string value of this stream identifier.
    /// </summary>
    public string Value { get; } = value;

    /// <summary>
    /// Implicitly converts a <see cref="string"/> to a <see cref="StreamId"/>.
    /// </summary>
    /// <param name="value">The string value of the stream identifier.</param>
    public static implicit operator StreamId(string value) => new(value);

    /// <inheritdoc/>
    public override string ToString() => Value;
}
