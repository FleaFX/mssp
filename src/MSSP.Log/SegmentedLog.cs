namespace MSSP.Log;

delegate ILogSegment<TRecord> SegmentFactory<TRecord>(int segmentSize) where TRecord : ILogRecord<TRecord>;

/// <summary>
/// An <see cref="ILog{TRecord}"/> backed by a growing list of fixed-size <see cref="ILogSegment{TRecord}"/> instances.
/// When the active segment is full a new one is opened automatically.
/// </summary>
class SegmentedLog<TRecord>(int segmentSize = 0x100_0000, SegmentFactory<TRecord>? segmentFactory = null) : ILog<TRecord>, IDisposable where TRecord : ILogRecord<TRecord> {
    readonly List<ILogSegment<TRecord>> _segments = [];
    readonly SemaphoreSlim _semaphore = new(1, 1);

    /// <inheritdoc/>
    public async ValueTask<bool> TryAppendAsync(TRecord record, CancellationToken cancellationToken = default) {
        if (!Validate(record))
            return false;

        await _semaphore.WaitAsync(cancellationToken);
        try {
            var segment = _segments.Count > 0 ? _segments[^1] : OpenNewSegment();
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
        var segment = (segmentFactory ?? DefaultFactory)(segmentSize);
        _segments.Add(segment);
        return segment;
    }

    /// <inheritdoc/>
    async IAsyncEnumerator<TRecord> IAsyncEnumerable<TRecord>.GetAsyncEnumerator(CancellationToken cancellationToken) {
        ILogSegment<TRecord>[] snapshot;
        await _semaphore.WaitAsync();
        try {
            snapshot = _segments.ToArray();
        } finally {
            _semaphore.Release();
        }

        foreach (var segment in snapshot)
            await foreach (var record in segment.WithCancellation(cancellationToken))
                yield return record;
    }

    /// <inheritdoc/>
    public void Dispose() {
        foreach (var segment in _segments)
            segment.Dispose();
        _semaphore.Dispose();
    }
}
