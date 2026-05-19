using System.Runtime.CompilerServices;
using MSSP.LsmTree;
using MSSP.Raft;

namespace MSSP.Cluster;

/// <summary>
/// <see cref="IMsspClient"/> implementation that routes writes through the Raft leader.
/// OCC is validated on the leader before proposing; the write lock combined with the
/// TCS-based read-after-write guarantee ensures the revision index is always current.
/// </summary>
sealed class ClusteredMsspClient(RaftNode node, LsmStore<EventKey> store) : IMsspClient, IDisposable {
    readonly SemaphoreSlim _writeLock = new(1, 1);
    readonly RevisionIndex _revisions = new();

    /// <inheritdoc/>
    public async ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken ct = default) {
        if (!node.IsLeader)
            throw new NotLeaderException(node.LeaderHint);

        await _writeLock.WaitAsync(ct);
        try {
            if (!_revisions.Contains(streamId.Value)) {
                var (exists, revision) = LookupCurrentRevision(streamId.Value);
                if (exists) _revisions.Set(streamId.Value, revision);
            }

            if (!_revisions.CheckConcurrency(streamId.Value, expectedRevision))
                throw new OptimisticConcurrencyException(streamId, expectedRevision);

            var baseRevision = _revisions.TryGet(streamId.Value, out var current) ? current + 1 : 0UL;
            var timestamp = DateTimeOffset.UtcNow;
            var offset = 0UL;

            foreach (var eventData in events) {
                var key = new EventKey(streamId.Value, baseRevision + offset++);
                ReadOnlyMemory<byte> value = EventValue.From(eventData, timestamp);
                await store.WriteAsync(key, value, ct);
                _revisions.Set(streamId.Value, key.Revision);
            }
        } finally {
            _writeLock.Release();
        }
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<RecordedEvent> ReadAsync(StreamId streamId, StreamRevision from = default, [EnumeratorCancellation] CancellationToken ct = default) {
        if (!node.IsLeader)
            throw new NotLeaderException(node.LeaderHint);

        IEnumerable<KeyValuePair<EventKey, ReadOnlyMemory<byte>?>> scan;
        var startKey = new EventKey(streamId.Value, 0UL);

        await _writeLock.WaitAsync(ct);
        try {
            scan = store.ScanSnapshotFrom(startKey);
        } finally {
            _writeLock.Release();
        }

        foreach (var (key, value) in scan) {
            if (ct.IsCancellationRequested) yield break;
            if (key.StreamId != streamId.Value) break;
            if (key.Revision < from || value is null) continue;
            yield return ((EventValue)value.Value).ToRecordedEvent(key);
        }
    }

    (bool exists, ulong revision) LookupCurrentRevision(string streamId) {
        ulong? max = null;
        foreach (var (key, _) in store.ScanAllFrom(new EventKey(streamId, 0UL))) {
            if (key.StreamId != streamId) break;
            max = key.Revision;
        }
        return (max.HasValue, max ?? 0UL);
    }

    /// <inheritdoc/>
    public void Dispose() => _writeLock.Dispose();
}
