using System.Buffers;

namespace MSSP.Log;

public delegate ILogSegment<TRecord> SegmentFactory<TRecord>(int segmentSize) where TRecord : ILogRecord<TRecord>;

class SegmentedLog<TRecord>(int segmentSize = 0x100_0000, SegmentFactory<TRecord>? segmentFactory = null) : ILog<TRecord>, IDisposable where TRecord : ILogRecord<TRecord> {
    readonly IMemoryOwner<ILogSegment<TRecord>> _segments = MemoryPool<ILogSegment<TRecord>>.Shared.Rent();
    readonly LogIndex _index = new();
    readonly SemaphoreSlim _semaphore = new(1, 1);

    /// <inheritdoc/>
    public async ValueTask<bool> TryAppendAsync(TRecord record, CancellationToken cancellationToken = new()) {
        if (!Validate(record))
            return false;

        await _semaphore.WaitAsync(cancellationToken);
        try {
            var segment = _index.Length > 0 ? _segments.Memory.Span[_index.Head] : OpenNewSegment();
            if (await segment.TryAppendAsync(record, cancellationToken)) return true;

            // appending to a segment only fails if the segment is full
            // complete the segment, open a new one and try again
            segment.Complete();
            segment = OpenNewSegment();
            return await segment.TryAppendAsync(record, cancellationToken);
        } finally {
            _semaphore.Release();
        }
    }

    bool Validate(ReadOnlyMemory<byte> record) => record.Length <= segmentSize;

    static ILogSegment<TRecord> DefaultFactory(int size) => new MemorySegment<TRecord>(size);

    ILogSegment<TRecord> OpenNewSegment() {
        _index.Advance(1);
        var segment = (segmentFactory ?? DefaultFactory)(segmentSize);
        _segments.Memory.Span[_index.Head] = segment;
        return segment;
    }

    /// <inheritdoc/>
    IAsyncEnumerator<TRecord> IAsyncEnumerable<TRecord>.GetAsyncEnumerator(CancellationToken cancellationToken) => (
            from index in _index
            from record in _segments.Memory.Span[index]
            select record
        ).GetAsyncEnumerator(cancellationToken);

    /// <inheritdoc/>
    public void Dispose() {
        foreach (var segment in _segments.Memory.Span[..(_index.Length + 1)])
            segment?.Dispose();
        _segments.Dispose();
        _index.Dispose();
        _semaphore.Dispose();
    }
}
