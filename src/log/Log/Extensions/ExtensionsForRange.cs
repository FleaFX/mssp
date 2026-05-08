namespace Log.Extensions;

static class ExtensionsForRange {
    /// <summary>
    /// Returns a <see cref="Range"/> covering all elements of <paramref name="span"/>.
    /// </summary>
    public static Range ToRange<_>(this Span<_> span) => new(0, span.Length);

    /// <summary>
    /// Returns <see langword="true"/> if <paramref name="range"/> contains no elements.
    /// </summary>
    public static bool IsEmpty(this Range range) => range.Start.Value == range.End.Value;

    /// <summary>
    /// Returns <see langword="true"/> if <paramref name="index"/> falls within <paramref name="range"/>.
    /// </summary>
    public static bool Contains(this Range range, Index index) => range.Start.Value <= index.Value && range.End.Value > index.Value;

    /// <summary>
    /// Returns the last element of <paramref name="array"/> within <paramref name="range"/>,
    /// or <paramref name="defaultValue"/> if the range is empty.
    /// </summary>
    public static T LastOrDefault<T>(this Range range, T[] array, T defaultValue = default!) => !range.IsEmpty() ? array[..range.End][^1] : defaultValue;
}
