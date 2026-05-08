using System.Buffers;

namespace Log;

class SegmentedLog<TRecord>(int segmentSize = 0x100_0000) : ILog<TRecord>, IDisposable where TRecord : ILogRecord<TRecord> {
    readonly IMemoryOwner<LogSegment<TRecord>> _segments = MemoryPool<LogSegment<TRecord>>.Shared.Rent();
    readonly LogIndex _index = new LogIndex();
    readonly object _lock = new();

    /// <summary>
    /// Appends the given <paramref name="record"/> to the log.
    /// </summary>
    /// <param name="record">The record to append to the log.</param>
    /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> that may be used to cancel the asynchronous operation.</param>
    public async ValueTask<bool> TryAppendAsync(TRecord record, CancellationToken cancellationToken = new()) {
        // if the record is larger than the maximum size of a segment, we'll never be able to append it
        if (!Validate(record))
            return false;

        Monitor.Enter(_lock);
        try {
            var segment = _index.Length > 0 ? _segments.Memory.Span[_index.Head] : OpenNewSegment();
            if (await segment.TryAppendAsync(record, cancellationToken)) return true;

            // appending to a segment only fails if the segment is full
            // complete the segment, open a new one and try again
            segment.Complete();
            OpenNewSegment();
            return await TryAppendAsync(record, cancellationToken);
        }
        finally {
            Monitor.Exit(_lock); 
        }
    }

    bool Validate(Memory<byte> record) => record.Length <= segmentSize;

    LogSegment<TRecord> OpenNewSegment() {
        _index.Advance(1);
        var segment = new LogSegment<TRecord>(segmentSize);
        _segments.Memory.Span[_index.Head] = segment;
        return segment;
    }

    /// <summary>
    /// Returns an enumerator that iterates asynchronously through the collection.
    /// </summary>
    /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> that may be used to cancel the asynchronous iteration.</param>
    /// <returns>An enumerator that can be used to iterate asynchronously through the collection.</returns>
    IAsyncEnumerator<TRecord> IAsyncEnumerable<TRecord>.GetAsyncEnumerator(CancellationToken cancellationToken) => (
        // aggregate the records from all segments
            from index in _index
            from record in _segments.Memory.Span[index]
            select record
        ).GetAsyncEnumerator(cancellationToken);

    /// <summary>
    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
    /// </summary>
    public void Dispose() {
        _segments.Dispose();
        _index.Dispose();
    }
}