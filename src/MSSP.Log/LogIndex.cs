using System.Buffers;
using MSSP.Log.Extensions;
using AsyncEnumerable = MSSP.Log.Extensions.AsyncEnumerable;

namespace MSSP.Log;

class LogIndex : IDisposable, IAsyncEnumerable<Index> {
    readonly Index[] _table;

    delegate void IndexAdvancedHandler(object sender, Index newCount);
    event IndexAdvancedHandler? IndexAdvanced;

    Range _range;

    /// <summary>
    /// Initializes a new <see cref="LogIndex"/>.
    /// </summary>
    /// <param name="segmentSize">The maximum size of the segment being indexed.</param>
    public LogIndex(int segmentSize = 0x100_0000) => _table = ArrayPool<Index>.Shared.Rent(segmentSize);

    /// <summary>
    /// Initializes a new <see cref="LogIndex"/> with the values from the given <paramref name="indices"/>.
    /// </summary>
    /// <param name="indices">The indices to fill the index with.</param>
    /// <param name="segmentSize">The maximum size of the segment being indexed.</param>
    public LogIndex(Span<Index> indices, int segmentSize = 0x100_0000) : this(segmentSize) {
        var source = indices.Trim(Index.Start);
        source.CopyTo(_table);
        _range = source.ToRange();
    }

    /// <summary>
    /// Gets the byte offset at the requested <paramref name="index"/> position.
    /// </summary>
    /// <param name="index">The position to retrieve.</param>
    /// <returns>The byte offset stored at <paramref name="index"/>.</returns>
    public Index this[Index index] => _range.Contains(index) ? _table[index] : throw new IndexOutOfRangeException();

    /// <summary>
    /// Gets the current head position.
    /// </summary>
    public Index Head => _range.LastOrDefault(_table);

    /// <summary>
    /// Gets the number of entries in the index.
    /// </summary>
    public int Length => _range.End.Value;

    /// <summary>
    /// Adds the given <paramref name="length"/> to the current head position and adds the result as a new index entry.
    /// </summary>
    /// <param name="length">The number of bytes that the head of the log has advanced.</param>
    public void Advance(int length) {
        if (length <= 0) throw new ArgumentOutOfRangeException(nameof(length));

        var head = _range.LastOrDefault(_table).Value;
        _range = ..(_range.End.Value + 1);
        _table.AsSpan()[_range][^1] = head + length;

        IndexAdvanced?.Invoke(this, _range.End);
    }

    /// <summary>
    /// Removes entries from the index, retaining only the first <paramref name="length"/> entries.
    /// </summary>
    /// <param name="length">The number of entries to keep (exclusive end of the new range). If omitted, the entire table will be emptied.</param>
    public void Truncate(Index length = default) => _range = new Range(0, length);

    /// <inheritdoc/>
    public void Dispose() => ArrayPool<Index>.Shared.Return(_table, true);

    /// <inheritdoc/>
    async IAsyncEnumerator<Index> IAsyncEnumerable<Index>.GetAsyncEnumerator(CancellationToken cancellationToken) {
        for (var i = 0; ; i++) {
            var value = await WaitFor(new Index(i), cancellationToken);
            if (value.Equals(default)) yield break;
            yield return value;
        }
    }

    /// <summary>
    /// Waits for the table head to be advanced to or past the given <paramref name="index"/>; used when asynchronously enumerating the index.
    /// </summary>
    /// <param name="index">The index to wait for.</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> to cancel the operation.</param>
    /// <returns>An awaitable <see cref="ValueTask"/>, which resolves to the value at the requested index.</returns>
    async ValueTask<Index> WaitFor(Index index, CancellationToken cancellationToken) =>
        _range.Contains(index) ?
            // the head might already be past the requested index
            _table[index] :

            // if it isn't, wait until the head of the table has advanced at least up to said index
            await AsyncEnumerable.FromEventPattern<IndexAdvancedHandler, Index>(
                    h => IndexAdvanced += h
                    , h => IndexAdvanced -= h
                    , cancellationToken)
                .Where(newCount => newCount.Value > index.Value)
                .Select(_ => _table[index])
                .FirstOrDefaultAsync(cancellationToken);
}
