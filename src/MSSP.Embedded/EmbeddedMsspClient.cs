using System.Runtime.CompilerServices;
using MSSP.LsmTree;

namespace MSSP.Embedded;

/// <summary>
/// An embedded, single-process implementation of <see cref="IMsspClient"/> that stores events on the local filesystem.
/// </summary>
public sealed class EmbeddedMsspClient : IMsspClient, IDisposable {
    readonly SemaphoreSlim _writeLock = new(1, 1);
    readonly RevisionIndex _revisions = new();
    readonly LsmStore<EventKey> _store;
    readonly WalManager _wal;

    EmbeddedMsspClient(LsmStore<EventKey> store, WalManager wal) {
        _store = store;
        _wal = wal;
    }

    /// <summary>
    /// Opens or creates an embedded event store at the given <paramref name="dataDirectory"/>,
    /// recovering any unflushed writes from the WAL.
    /// </summary>
    /// <param name="dataDirectory">The directory in which to store WAL and SST files.</param>
    /// <param name="memTableCapacityBytes">The maximum size of the in-memory write buffer before it is flushed to an SST file.</param>
    /// <param name="ct">Token to cancel the open operation.</param>
    /// <returns>An <see cref="EmbeddedMsspClient"/> ready for use.</returns>
    public static async ValueTask<EmbeddedMsspClient> OpenAsync(string dataDirectory, int memTableCapacityBytes = 64 * 1024 * 1024, ISstAccess<EventKey>? sst = null, CancellationToken ct = default) {
        Directory.CreateDirectory(dataDirectory);
        var wal = WalManager.Open(dataDirectory);
        var options = new LsmStoreOptions<EventKey>(dataDirectory, memTableCapacityBytes, wal.AppendAsync, wal.RotateAsync, SstAccess: sst);
        var store = await LsmStore<EventKey>.OpenAsync(options, wal.ReadAllAsync(ct), ct);
        return new EmbeddedMsspClient(store, wal);
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
                ReadOnlyMemory<byte> value = EventValue.From(eventData, timestamp);
                await _store.WriteAsync(key, value, ct);
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
            scan = _store.ScanSnapshotFrom(startKey);
        } finally {
            _writeLock.Release();
        }

        foreach (var (key, value) in scan) {
            if (ct.IsCancellationRequested) yield break;
            if (key.StreamId != streamId.Value || key.Revision < from || value is null) continue;
            yield return ((EventValue)value.Value).ToRecordedEvent(key);
        }
    }

    (bool exists, ulong revision) LookupCurrentRevision(string streamId) {
        ulong? max = null;
        var startKey = new EventKey(streamId, 0UL);

        foreach (var (key, _) in _store.ScanAllFrom(startKey)) {
            if (key.StreamId != streamId) break;
            max = key.Revision;
        }

        return (max.HasValue, max ?? 0UL);
    }

    /// <inheritdoc/>
    public void Dispose() {
        _writeLock.Dispose();
        _store.Dispose();
        _wal.Dispose();
    }
}
