using System.Runtime.CompilerServices;
using MSSP.LsmTree;

namespace MSSP.Embedded;

/// <summary>
/// An embedded, single-process implementation of <see cref="IMsspClient"/> that stores events on the local filesystem.
/// </summary>
public sealed class EmbeddedMsspClient : IMsspClient, IDisposable {
    readonly SemaphoreSlim _writeLock = new(1, 1);
    readonly LsmStore _store;
    readonly RevisionIndex _revisions = new();

    EmbeddedMsspClient(LsmStore store) =>
        _store = store;

    /// <summary>
    /// Opens or creates an embedded event store at the given <paramref name="dataDirectory"/>,
    /// recovering any unflushed writes from the WAL.
    /// </summary>
    /// <param name="dataDirectory">The directory in which to store WAL and SST files.</param>
    /// <param name="memTableCapacityBytes">The maximum size of the in-memory write buffer before it is flushed to an SST file.</param>
    /// <param name="ct">Token to cancel the open operation.</param>
    /// <returns>An <see cref="EmbeddedMsspClient"/> ready for use.</returns>
    public static async ValueTask<EmbeddedMsspClient> OpenAsync(string dataDirectory, int memTableCapacityBytes = 64 * 1024 * 1024, CancellationToken ct = default) =>
        new(await LsmStore.OpenAsync(dataDirectory, memTableCapacityBytes, ct));

    /// <inheritdoc/>
    public async ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken ct = default) {
        await _writeLock.WaitAsync(ct);
        try {
            if (!_revisions.Contains(streamId.Value)) {
                var (exists, revision) = _store.LookupCurrentRevision(streamId.Value);
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
        string[] sstFiles;
        MemTable<EventKey> memTable;

        await _writeLock.WaitAsync(ct);
        try {
            (sstFiles, memTable) = _store.TakeSnapshot();
        } finally {
            _writeLock.Release();
        }

        foreach (var sstPath in sstFiles) {
            if (ct.IsCancellationRequested) yield break;
            using var stream = new FileStream(sstPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);
            foreach (var (key, value) in new SstReader<EventKey>(stream).Scan()) {
                if (key.StreamId != streamId.Value || key.Revision < from || value is null) continue;
                yield return ((EventValue)value.Value).ToRecordedEvent(key);
            }
        }

        foreach (var (key, value) in memTable) {
            if (ct.IsCancellationRequested) yield break;
            if (key.StreamId != streamId.Value || key.Revision < from || value is null) continue;
            yield return ((EventValue)value.Value).ToRecordedEvent(key);
        }
    }

    /// <inheritdoc/>
    public void Dispose() {
        _writeLock.Dispose();
        _store.Dispose();
    }
}
