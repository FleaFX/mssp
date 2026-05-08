namespace Log.Extensions;

static class ExtensionsForRange {
    public static Range ToRange<_>(this Span<_> span) => new(0, span.Length);
    public static bool IsEmpty(this Range range) => range.Start.Value == range.End.Value;
    public static bool Contains(this Range range, Index index) => range.Start.Value <= index.Value && range.End.Value > index.Value;
    public static T LastOrDefault<T>(this Range range, T[] array, T defaultValue = default!) => !range.IsEmpty() ? array[..range.End][^1] : defaultValue;
}