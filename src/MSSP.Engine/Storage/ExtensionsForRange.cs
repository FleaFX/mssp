namespace MSSP.Engine.Storage;

static class ExtensionsForRange {
    /// <summary>
    /// Returns a <see cref="Range"/> covering all elements of <paramref name="span"/>.
    /// </summary>
    public static Range ToRange<_>(this Span<_> span) => new(0, span.Length);

    extension(Range range) {
        /// <summary>
        /// Returns <see langword="true"/> if <paramref name="range"/> contains no elements.
        /// </summary>
        public bool IsEmpty() => range.Start.Value == range.End.Value;

        /// <summary>
        /// Returns <see langword="true"/> if <paramref name="index"/> falls within <paramref name="range"/>.
        /// </summary>
        public bool Contains(Index index) => range.Start.Value <= index.Value && range.End.Value > index.Value;

        /// <summary>
        /// Returns the last element of <paramref name="array"/> within <paramref name="range"/>,
        /// or <paramref name="defaultValue"/> if the range is empty.
        /// </summary>
        public T LastOrDefault<T>(T[] array, T defaultValue = default!) => !range.IsEmpty() ? array[..range.End][^1] : defaultValue;
    }
}
