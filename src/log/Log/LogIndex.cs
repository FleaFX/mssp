using System.Buffers;
using Log.Extensions;
using AsyncEnumerable = Log.Extensions.AsyncEnumerable;

namespace Log;

class LogIndex : IDisposable, IAsyncEnumerable<Index> {
    readonly Index[] _table;

    delegate void IndexAdvancedHandler(object sender, Index head);
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
    /// Gets the head for the requested <paramref name="index"/>.
    /// </summary>
    /// <param name="index">The index to get the head position of.</param>
    /// <returns>A <see cref="int"/>.</returns>
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
    /// Removes entries from the index.
    /// </summary>
    /// <param name="to">Optional <see cref="Index"/> to truncate to. This will be the last index in the table. If omitted, the entire table will be emptied.</param>
    public void Truncate(Index to = default) => _range = new Range(0, to);

    /// <summary>
    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
    /// </summary>
    public void Dispose() => ArrayPool<Index>.Shared.Return(_table, true);

    /// <summary>Returns an enumerator that iterates asynchronously through the collection.</summary>
    /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> that may be used to cancel the asynchronous iteration.</param>
    /// <returns>An enumerator that can be used to iterate asynchronously through the collection.</returns>
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
                .Where(head => head.Value >= index.Value)
                .Select(ix => _table[ix])
                .FirstOrDefaultAsync(cancellationToken);
}