namespace MSSP;

/// <summary>
/// Represents a revision within a stream, used both as a read cursor and as an optimistic concurrency expectation when appending.
/// </summary>
public readonly struct StreamRevision : IEquatable<StreamRevision>, IComparable<StreamRevision> {
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

    /// <summary>
    /// Explicitly converts a <see cref="StreamRevision"/> to its underlying <see cref="long"/> representation.
    /// Sentinel values (<see cref="Any"/>, <see cref="NoStream"/>, <see cref="StreamExists"/>) are preserved as negative numbers.
    /// </summary>
    /// <param name="revision">The revision to convert.</param>
    public static explicit operator long(StreamRevision revision) => revision._value;

    /// <summary>
    /// Explicitly converts a <see cref="StreamRevision"/> to its underlying <see cref="ulong"/> representation.
    /// Sentinel values (<see cref="Any"/>, <see cref="NoStream"/>, <see cref="StreamExists"/>) are preserved as negative numbers.
    /// </summary>
    /// <param name="revision">The revision to convert.</param>
    public static explicit operator ulong(StreamRevision revision) => (ulong)revision._value;

    /// <inheritdoc/>
    public bool Equals(StreamRevision other) => _value == other._value;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is StreamRevision other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => _value.GetHashCode();

    /// <inheritdoc/>
    public int CompareTo(StreamRevision other) => _value.CompareTo(other._value);

    /// <summary>
    /// Returns <see langword="true"/> if <paramref name="left"/> and <paramref name="right"/> represent the same revision.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    public static bool operator ==(StreamRevision left, StreamRevision right) => left.Equals(right);

    /// <summary>
    /// Returns <see langword="true"/> if <paramref name="left"/> and <paramref name="right"/> do not represent the same revision.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    public static bool operator !=(StreamRevision left, StreamRevision right) => !left.Equals(right);

    /// <summary>
    /// Returns <see langword="true"/> if <paramref name="left"/> precedes <paramref name="right"/>.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    public static bool operator <(StreamRevision left, StreamRevision right) => left._value < right._value;

    /// <summary>
    /// Returns <see langword="true"/> if <paramref name="left"/> follows <paramref name="right"/>.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    public static bool operator >(StreamRevision left, StreamRevision right) => left._value > right._value;

    /// <summary>
    /// Returns <see langword="true"/> if <paramref name="left"/> precedes or equals <paramref name="right"/>.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    public static bool operator <=(StreamRevision left, StreamRevision right) => left._value <= right._value;

    /// <summary>
    /// Returns <see langword="true"/> if <paramref name="left"/> follows or equals <paramref name="right"/>.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    public static bool operator >=(StreamRevision left, StreamRevision right) => left._value >= right._value;
}
