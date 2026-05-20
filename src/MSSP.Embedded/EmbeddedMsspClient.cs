using System.Runtime.CompilerServices;
using System.Threading.Channels;
using MSSP.Storage;

namespace MSSP.Embedded;

/// <summary>
/// An embedded, single-process implementation of <see cref="IMsspClient"/> that stores events on the local filesystem.
/// </summary>
public sealed class EmbeddedMsspClient(
    ILsmStore<EventKey> store,
    ISubscriptionProvider subscriptions
): IMsspClient, IDisposable {
    readonly SemaphoreSlim _writeLock = new(1, 1);
    readonly RevisionIndex _revisions = new();

    /// <summary>
    /// The <see cref="GlobalPosition"/> of the most recently applied event on this node.
    /// On a follower, this reflects entries received via Raft replication.
    /// </summary>
    public GlobalPosition CurrentPosition => subscriptions.CurrentPosition;

    /// <summary>
    /// Opens or creates an embedded event store at the given <paramref name="dataDirectory"/>,
    /// recovering any unflushed writes from the WAL.
    /// </summary>
    /// <param name="dataDirectory">The directory in which to store WAL and SST files.</param>
    /// <param name="memTableCapacityBytes">The maximum size of the in-memory write buffer before it is flushed to an SST file.</param>
    /// <param name="sst">Optional SST access decorator (e.g. bloom filter layer).</param>
    /// <param name="subscriptionLogFormat">The format of the subscription log entries.</param>
    /// <param name="subscriptionLogSegmentSizeBytes">Maximum size of a single subscription log segment.</param>
    /// <param name="ct">Token to cancel the open operation.</param>
    /// <returns>An <see cref="EmbeddedMsspClient"/> ready for use.</returns>
    public static async ValueTask<EmbeddedMsspClient> OpenAsync(
        string dataDirectory,
        int memTableCapacityBytes = 64 * 1024 * 1024,
        ISstAccess<EventKey>? sst = null,
        SubscriptionLogFormat subscriptionLogFormat = SubscriptionLogFormat.FullPayload,
        long subscriptionLogSegmentSizeBytes = 64 * 1024 * 1024,
        CancellationToken ct = default) {

        Directory.CreateDirectory(dataDirectory);
        var wal = WalManager.Open(dataDirectory);
        var log = new EmbeddedLog(wal);
        var lsmOptions = new LsmStoreOptions<EventKey>(dataDirectory, memTableCapacityBytes, _ => ValueTask.CompletedTask, SstAccess: sst);
        var lsmStore = await LsmStore<EventKey>.OpenAsync(lsmOptions, wal.ReadAllAsync(ct), ct);

        var subscriptionLog = SubscriptionLog.Open(dataDirectory, subscriptionLogFormat, subscriptionLogSegmentSizeBytes);
        var pipeline = new SubscriptionPipeline(lsmStore, subscriptionLog);
        var logDriven = LogDrivenStore<EventKey>.Create(log, pipeline, memTableCapacityBytes);

        return new EmbeddedMsspClient(store: new GlobalPositionDecorator(logDriven, pipeline), subscriptions: pipeline);
    }

    /// <inheritdoc/>
    public async ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken ct = default) {
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
                await store.WriteAsync(key, EventValue.From(eventData, timestamp), ct);
                _revisions.Set(streamId.Value, key.Revision);
            }
        } finally {
            _writeLock.Release();
        }
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<RecordedEvent> ReadAsync(StreamId streamId, StreamRevision from = default, [EnumeratorCancellation] CancellationToken ct = default) {
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

    /// <inheritdoc/>
    public async IAsyncEnumerable<SubscriptionEvent> SubscribeAsync(
        SubscriptionFilter filter,
        GlobalPosition fromPosition = default,
        [EnumeratorCancellation] CancellationToken ct = default) {

        ChannelReader<SubscriptionEvent>? liveChannel = null;
        IEnumerable<SubscriptionEvent> catchUpScan;
        GlobalPosition catchUpPosition;

        await _writeLock.WaitAsync(ct);
        try {
            catchUpPosition = subscriptions.CurrentPosition;
            liveChannel = subscriptions.Register(filter);
            catchUpScan = subscriptions.ScanFrom(fromPosition, BuildResolver());
        } finally {
            _writeLock.Release();
        }

        try {
            // CATCH-UP: replay historical events from the subscription log.
            // The log is ordered by GlobalPosition, so we can break on first entry past the snapshot.
            foreach (var evt in catchUpScan) {
                if (ct.IsCancellationRequested) yield break;
                if (evt.Position > catchUpPosition) break;
                if (filter.Matches(evt)) yield return evt;
            }

            // LIVE: deliver events written after the catch-up snapshot.
            // The overlap guard skips any events already delivered in catch-up.
            await foreach (var evt in liveChannel!.ReadAllAsync(ct)) {
                if (evt.Position <= catchUpPosition) continue;
                yield return evt;
            }
        } finally {
            if (liveChannel != null) {
                await _writeLock.WaitAsync(CancellationToken.None);
                try {
                    subscriptions.Unregister(liveChannel);
                } finally {
                    _writeLock.Release();
                }
            }
        }
    }

    // For FullPayload format the log contains full event data; no resolver needed.
    // For ReferenceOnly the log stores only EventKey pointers, resolved here via SST scan.
    Func<EventKey, SubscriptionEvent>? BuildResolver() {
        if (subscriptions.LogFormat == SubscriptionLogFormat.FullPayload) return null;
        return key => {
            foreach (var (k, v) in store.ScanSnapshotFrom(key)) {
                if (!k.Equals(key)) break;
                if (v is null) break;
                return ((EventValue)v.Value).ToSubscriptionEvent(k);
            }
            throw new InvalidOperationException($"Event {key.StreamId}@{key.Revision} not found in store.");
        };
    }

    (bool exists, ulong revision) LookupCurrentRevision(string streamId) {
        ulong? max = null;
        var startKey = new EventKey(streamId, 0UL);

        foreach (var (key, _) in store.ScanAllFrom(startKey)) {
            if (key.StreamId != streamId) break;
            max = key.Revision;
        }

        return (max.HasValue, max ?? 0UL);
    }

    /// <inheritdoc/>
    public void Dispose() {
        store.Dispose();
        _writeLock.Dispose();
    }
}
