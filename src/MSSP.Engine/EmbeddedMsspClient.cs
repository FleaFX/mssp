using System.Diagnostics.Metrics;
using MSSP.Engine.Storage;

namespace MSSP.Engine;

/// <summary>
/// An embedded, single-process implementation of <see cref="IMsspClient"/> that stores events on the local filesystem.
/// </summary>
/// <param name="log">Write-ahead log; owned and disposed by this instance if it implements <see cref="IDisposable"/>.</param>
/// <param name="store">The key-value store; owned and disposed via <see cref="ILsmStore{TKey}"/>.</param>
/// <param name="subscriptionLog">Subscription position log; owned by the engine.</param>
/// <param name="dataDirectory">Root directory used for backups. <see langword="null"/> disables backup support.</param>
/// <param name="meterFactory">Optional meter factory for diagnostics.</param>
public sealed partial class EmbeddedMsspClient(ILog<WalRecord> log, ILsmStore<EventKey> store, SubscriptionLog subscriptionLog, string? dataDirectory = null, IMeterFactory? meterFactory = null) : IMsspClient, IAsyncDisposable {

    readonly IDisposable? _logOwner = log as IDisposable;
    readonly EmbeddedMetrics? _metrics = meterFactory is not null ? new EmbeddedMetrics(meterFactory) : null;
    readonly StoreEngine _engine = new StoreEngine(log, store, subscriptionLog, (long)subscriptionLog.GetLastPosition().Value).Start();

    internal ValueTask ReloadSnapshotAsync(string stagingDirectory, CancellationToken cancellationToken) =>
        _engine.ReloadSnapshotAsync(stagingDirectory, cancellationToken);

    /// <summary>
    /// The <see cref="GlobalPosition"/> of the most recently applied event on this node.
    /// On a follower, this reflects entries received via Raft replication.
    /// </summary>
    public GlobalPosition CurrentPosition => _engine.CurrentPosition;

    /// <summary>
    /// Opens or creates an embedded event store at the given <paramref name="dataDirectory"/>,
    /// recovering any unflushed writes from the WAL.
    /// </summary>
    /// <param name="dataDirectory">The directory in which to store WAL and SST files.</param>
    /// <param name="memTableCapacityBytes">The maximum size of the in-memory write buffer before it is flushed to an SST file.</param>
    /// <param name="sst">Optional SST access decorator (e.g. bloom filter layer).</param>
    /// <param name="subscriptionLogFormat">The format of the subscription log entries.</param>
    /// <param name="subscriptionLogSegmentSizeBytes">Maximum size of a single subscription log segment.</param>
    /// <param name="meterFactory">Optional meter factory for diagnostics.</param>
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
        return new EmbeddedMsspClient(log, lsmStore, subscriptionLog, dataDirectory, meterFactory);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() {
        await _engine.DisposeAsync();
        _logOwner?.Dispose();
        _metrics?.Dispose();
    }
}
