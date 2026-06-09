namespace MSSP.Embedded;

public sealed partial class EmbeddedMsspClient {
    /// <inheritdoc/>
    public async ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken cancellationToken = default) {
        var timer = OperationTimer.Start();
        var eventCount = 0L;

        await _writeLock.WaitAsync(cancellationToken);
        try {
            if (!_revisions.Contains(streamId.Value)) {
                var (exists, revision) = LookupCurrentRevision(streamId.Value);
                if (exists) _revisions.Set(streamId.Value, revision);
            }

            if (!_revisions.CheckConcurrency(streamId.Value, expectedRevision)) {
                _metrics?.RecordConflict();
                throw new OptimisticConcurrencyException(streamId, expectedRevision);
            }

            var baseRevision = _revisions.TryGet(streamId.Value, out var current) ? current + 1 : 0UL;
            var timestamp = DateTimeOffset.UtcNow;
            var offset = 0UL;

            foreach (var eventData in events) {
                var key = new EventKey(streamId.Value, baseRevision + offset++);
                await store.WriteAsync(key, EventValue.From(eventData, timestamp), cancellationToken);
                _revisions.Set(streamId.Value, key.Revision);
                eventCount++;
            }
        } finally {
            _writeLock.Release();
            if (_metrics is not null && eventCount > 0)
                _metrics.RecordAppend(eventCount, timer.ElapsedMs);
        }
    }
}
