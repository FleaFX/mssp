namespace MSSP;

/// <summary>
/// Represents a revision within a stream, used both as a read cursor and as an optimistic concurrency expectation when appending.
/// </summary>
public readonly struct StreamRevision {
    readonly long _value;

    StreamRevision(long value) => _value = value;

    /// <summary>
    /// No concurrency check; append regardless of whether the stream exists or its current revision.
    /// </summary>
    public static readonly StreamRevision Any = new(-1);

    /// <summary>
    /// The stream must not yet exist.
    /// </summary>
    public static readonly StreamRevision NoStream = new(-2);

    /// <summary>
    /// The stream must already exist, but the exact revision is not checked.
    /// </summary>
    public static readonly StreamRevision StreamExists = new(-3);

    /// <summary>
    /// Implicitly converts a <see cref="ulong"/> to a <see cref="StreamRevision"/>.
    /// </summary>
    /// <param name="value">The numeric revision value.</param>
    public static implicit operator StreamRevision(ulong value) => new((long)value);
}
