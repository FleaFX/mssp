using System.Diagnostics.Metrics;
using MSSP.Storage;

namespace MSSP.Engine;

/// <summary>
/// An embedded, single-process implementation of <see cref="IMsspClient"/> that stores events on the local filesystem.
/// </summary>
public sealed partial class EmbeddedMsspClient(
    ILsmStore<EventKey> store,
    ISubscriptionProvider subscriptions,
    IMeterFactory? meterFactory = null
): IMsspClient, IDisposable, IAsyncDisposable {
    readonly SemaphoreSlim _writeLock = new(1, 1);
    readonly RevisionIndex _revisions = new();
    readonly EmbeddedMetrics? _metrics = meterFactory is not null ? new EmbeddedMetrics(meterFactory) : null;
    string? _dataDirectory;
    LsmStore<EventKey>? _lsmStore;
    StoreEngine? _engine;
    EmbeddedLog? _embeddedLog;

    /// <summary>
    /// Wires up the backup source. Called from <see cref="OpenAsync"/> after construction.
    /// </summary>
    internal EmbeddedMsspClient WithBackupSource(string dataDirectory, LsmStore<EventKey> lsmStore) {
        _dataDirectory = dataDirectory;
        _lsmStore = lsmStore;
        return this;
    }

    internal EmbeddedMsspClient WithEngine(StoreEngine engine, EmbeddedLog log) {
        _engine = engine;
        _embeddedLog = log;
        return this;
    }

    /// <summary>
    /// The <see cref="GlobalPosition"/> of the most recently applied event on this node.
    /// On a follower, this reflects entries received via Raft replication.
    /// </summary>
    public GlobalPosition CurrentPosition => _engine?.CurrentPosition ?? subscriptions.CurrentPosition;

    /// <summary>
    /// Opens or creates an embedded event store at the given <paramref name="dataDirectory"/>,
    /// recovering any unflushed writes from the WAL.
    /// </summary>
    /// <param name="dataDirectory">The directory in which to store WAL and SST files.</param>
    /// <param name="memTableCapacityBytes">The maximum size of the in-memory write buffer before it is flushed to an SST file.</param>
    /// <param name="sst">Optional SST access decorator (e.g. bloom filter layer).</param>
    /// <param name="subscriptionLogFormat">The format of the subscription log entries.</param>
    /// <param name="subscriptionLogSegmentSizeBytes">Maximum size of a single subscription log segment.</param>
    /// <param name="cancellationToken">Token to cancel the open operation.</param>
    /// <returns>An <see cref="EmbeddedMsspClient"/> ready for use.</returns>
    public static async ValueTask<EmbeddedMsspClient> OpenAsync(
        string dataDirectory,
        int memTableCapacityBytes = 64 * 1024 * 1024,
        ISstAccess<EventKey>? sst = null,
        SubscriptionLogFormat subscriptionLogFormat = SubscriptionLogFormat.FullPayload,
        long subscriptionLogSegmentSizeBytes = 64 * 1024 * 1024,
        IMeterFactory? meterFactory = null,
        CancellationToken cancellationToken = default) {

        Directory.CreateDirectory(dataDirectory);
        var wal = WalManager.Open(dataDirectory);
        var log = new EmbeddedLog(wal);
        var lsmMetrics = meterFactory is not null ? new LsmStoreMetrics(meterFactory, memTableCapacityBytes) : null;
        var lsmOptions = new LsmStoreOptions<EventKey>(dataDirectory, memTableCapacityBytes,
            _ => { log.RequestRotation(); return ValueTask.CompletedTask; },
            BaseLevelSizeBytes: -1, LevelSizeMultiplier: 10, SstAccess: sst, Metrics: lsmMetrics);
        var lsmStore = await LsmStore<EventKey>.OpenAsync(lsmOptions, wal.ReadAllForRecoveryAsync(cancellationToken), cancellationToken);
        wal.DeletePrevWalIfExists();

        var subscriptionLog = SubscriptionLog.Open(dataDirectory, subscriptionLogFormat, subscriptionLogSegmentSizeBytes);
        var engine = new StoreEngine(log, lsmStore, subscriptionLog, (long)subscriptionLog.GetLastPosition().Value);
        engine.Start();

        var pipeline = new SubscriptionPipeline(lsmStore, subscriptionLog);

        return new EmbeddedMsspClient(store: pipeline, subscriptions: pipeline, meterFactory: meterFactory)
            .WithBackupSource(dataDirectory, lsmStore)
            .WithEngine(engine, log);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() {
        if (_engine is not null) await _engine.DisposeAsync();
        _embeddedLog?.Dispose();
        store.Dispose();
        _writeLock.Dispose();
        _metrics?.Dispose();
    }

    /// <inheritdoc/>
    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    (bool exists, ulong revision) LookupCurrentRevision(string streamId) {
        ulong? max = null;
        var startKey = new EventKey(streamId, 0UL);

        foreach (var (key, _) in store.ScanAllFrom(startKey)) {
            if (key.StreamId != streamId) break;
            max = key.Revision;
        }

        return (max.HasValue, max ?? 0UL);
    }
}
