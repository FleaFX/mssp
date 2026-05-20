namespace MSSP;

/// <summary>
/// A monotonically increasing counter that uniquely identifies an event's position across all streams.
/// </summary>
public readonly record struct GlobalPosition(ulong Value) : IComparable<GlobalPosition> {
    /// <summary>
    /// The position before any events have been written.
    /// </summary>
    public static readonly GlobalPosition Start = new(0);

    /// <inheritdoc/>
    public int CompareTo(GlobalPosition other) => Value.CompareTo(other.Value);

    /// <summary>
    /// Returns <see langword="true"/> if <paramref name="left"/> precedes <paramref name="right"/>.
    /// </summary>
    public static bool operator <(GlobalPosition left, GlobalPosition right) => left.Value < right.Value;

    /// <summary>
    /// Returns <see langword="true"/> if <paramref name="left"/> follows <paramref name="right"/>.
    /// </summary>
    public static bool operator >(GlobalPosition left, GlobalPosition right) => left.Value > right.Value;

    /// <summary>
    /// Returns <see langword="true"/> if <paramref name="left"/> precedes or equals <paramref name="right"/>.
    /// </summary>
    public static bool operator <=(GlobalPosition left, GlobalPosition right) => left.Value <= right.Value;

    /// <summary>
    /// Returns <see langword="true"/> if <paramref name="left"/> follows or equals <paramref name="right"/>.
    /// </summary>
    public static bool operator >=(GlobalPosition left, GlobalPosition right) => left.Value >= right.Value;
}
