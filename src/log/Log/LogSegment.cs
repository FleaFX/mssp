using System.Buffers;

namespace Log;

class LogSegment<TRecord> : ILog<TRecord>, IDisposable where TRecord : ILogRecord<TRecord> {
    readonly int _segmentSize;
    readonly IMemoryOwner<byte> _table;
    readonly LogIndex _index;
    readonly object _lock;
    bool _completed;

    /// <summary>
    /// Initializes a new <see cref="LogSegment{TRecord}"/>.
    /// </summary>
    /// <param name="segmentSize">The maximum size in bytes of the segment.</param>
    public LogSegment(int segmentSize = 0x100_0000) {
        _segmentSize = segmentSize;
        _table = MemoryPool<byte>.Shared.Rent(_segmentSize);
        _index = new LogIndex(_segmentSize);
        _lock = new object();
    }

    /// <inheritdoc/>
    public ValueTask<bool> TryAppendAsync(TRecord record, CancellationToken cancellationToken = new()) =>
      ValueTask.FromResult(TryAppendCore(record));

    bool TryAppendCore(Memory<byte> record) {
        if (_completed) return false;

        lock (_lock) {
            // the only reason to reject a record is if it doesn't fit the table anymore
            // a rejected write should be reissued to a new log segment
            if (!record.TryCopyTo(_table.Memory[_index.Head.._segmentSize]))
                return false;

            _index.Advance(record.Length);

            return true;
        }
    }

    /// <summary>
    /// Marks the segment as complete. You will not be able to append any more records after calling this method.
    /// </summary>
    public void Complete() => _completed = true;

    /// <inheritdoc/>
    async IAsyncEnumerator<TRecord> IAsyncEnumerable<TRecord>.GetAsyncEnumerator(CancellationToken cancellationToken) {
        var previous = Index.Start;
        await foreach (var index in _index.WithCancellation(!_completed ? cancellationToken : new CancellationToken(true))) {
            yield return _table.Memory[previous..index];
            previous = index;
        }
    }

    /// <inheritdoc/>
    public void Dispose() {
        _table.Dispose();
        _index.Dispose();
    }
}
