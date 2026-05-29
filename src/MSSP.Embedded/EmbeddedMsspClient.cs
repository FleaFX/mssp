using System.Diagnostics.Metrics;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using MSSP;
using MSSP.Storage;

namespace MSSP.Embedded;

/// <summary>
/// An embedded, single-process implementation of <see cref="IMsspClient"/> that stores events on the local filesystem.
/// </summary>
public sealed class EmbeddedMsspClient(
    ILsmStore<EventKey> store,
    ISubscriptionProvider subscriptions,
    IMeterFactory? meterFactory = null
): IMsspClient, IDisposable {
    readonly SemaphoreSlim _writeLock = new(1, 1);
    readonly RevisionIndex _revisions = new();
    readonly EmbeddedMetrics? _metrics = meterFactory is not null ? new EmbeddedMetrics(meterFactory) : null;

    string? _dataDirectory;
    LsmStore<EventKey>? _lsmStore;

    /// <summary>
    /// Wires up the backup source. Called from <see cref="OpenAsync"/> after construction.
    /// </summary>
    internal EmbeddedMsspClient WithBackupSource(string dataDirectory, LsmStore<EventKey> lsmStore) {
        _dataDirectory = dataDirectory;
        _lsmStore = lsmStore;
        return this;
    }

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
        var lsmOptions = new LsmStoreOptions<EventKey>(dataDirectory, memTableCapacityBytes, _ => ValueTask.CompletedTask, BaseLevelSizeBytes: -1, LevelSizeMultiplier: 10, SstAccess: sst, Metrics: lsmMetrics);
        var lsmStore = await LsmStore<EventKey>.OpenAsync(lsmOptions, wal.ReadAllAsync(cancellationToken), cancellationToken);

        var subscriptionLog = SubscriptionLog.Open(dataDirectory, subscriptionLogFormat, subscriptionLogSegmentSizeBytes);
        var pipeline = new SubscriptionPipeline(lsmStore, subscriptionLog);
        var logDriven = LogDrivenStore<EventKey>.Create(log, pipeline, memTableCapacityBytes);

        return new EmbeddedMsspClient(store: new GlobalPositionDecorator(logDriven, pipeline), subscriptions: pipeline, meterFactory: meterFactory)
            .WithBackupSource(dataDirectory, lsmStore);
    }

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

    /// <inheritdoc/>
    public async IAsyncEnumerable<RecordedEvent> ReadAsync(StreamId streamId, StreamRevision from = default, ReadDirection direction = ReadDirection.Forwards, long maxCount = long.MaxValue, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        IEnumerable<KeyValuePair<EventKey, ReadOnlyMemory<byte>?>> scan;
        var startKey = new EventKey(streamId.Value, 0UL);

        await _writeLock.WaitAsync(cancellationToken);
        try {
            scan = store.ScanSnapshotFrom(startKey);
        } finally {
            _writeLock.Release();
        }

        // First pass: collect all events for the stream
        var allEvents = new List<RecordedEvent>();
        foreach (var (key, value) in scan) {
            if (cancellationToken.IsCancellationRequested) yield break;
            if (key.StreamId != streamId.Value) break;
            if (value is null) continue;
            allEvents.Add(((EventValue)value.Value).ToRecordedEvent(key));
        }

        // Determine the effective from revision for filtering
        // For Backwards with default from (0), we want to read from the end (max revision)
        var effectiveFrom = direction == ReadDirection.Backwards && from == default && allEvents.Count > 0 ? allEvents.Max(e => e.Revision) : from;

        // Apply direction and from filter
        var count = 0L;
        foreach (var evt in direction switch {
                     ReadDirection.Forwards => allEvents.Where(e => e.Revision >= effectiveFrom),
                     ReadDirection.Backwards => allEvents.Where(e => e.Revision <= effectiveFrom).Reverse(),
                     _ => throw new ArgumentOutOfRangeException(nameof(direction), direction, null)
                 }) {
            if (count++ >= maxCount)
                yield break;
            _metrics?.RecordRead(1);
            yield return evt;
        }
    }
    
    /// <inheritdoc/>
    public async IAsyncEnumerable<SubscriptionEvent> SubscribeAsync(
        SubscriptionFilter filter,
        GlobalPosition fromPosition = default,
        [EnumeratorCancellation] CancellationToken cancellationToken = default) {

        ChannelReader<SubscriptionEvent>? liveChannel;
        IEnumerable<SubscriptionEvent> catchUpScan;
        GlobalPosition catchUpPosition;

        _metrics?.SubscriptionStarted();
        await _writeLock.WaitAsync(cancellationToken);
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
                if (cancellationToken.IsCancellationRequested) yield break;
                if (evt.Position > catchUpPosition) break;
                if (filter.Matches(evt)) yield return evt;
            }

            // LIVE: deliver events written after the catch-up snapshot.
            // The overlap guard skips any events already delivered in catch-up.
            await foreach (var evt in liveChannel!.ReadAllAsync(cancellationToken)) {
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
            _metrics?.SubscriptionStopped();
        }
    }

    /// <summary>
    /// Creates a compressed backup of the store as a ZIP archive at <paramref name="backupPath"/>.
    /// Archives all active SST files, their bloom filter sidecars, the subscription log, and the WAL.
    /// Writes that started before this call are guaranteed to be included;
    /// writes that start after may or may not be included (fuzzy backup).
    /// </summary>
    /// <remarks>
    /// Only available on instances created via <see cref="OpenAsync"/>.
    /// The store remains fully operational during the backup.
    /// </remarks>
    public async ValueTask CreateBackupAsync(string backupPath, CancellationToken cancellationToken = default) {
        if (_dataDirectory is null || _lsmStore is null)
            throw new InvalidOperationException($"{nameof(CreateBackupAsync)} is only available on instances created via {nameof(OpenAsync)}.");

        var parentDir = Path.GetDirectoryName(backupPath);
        if (!string.IsNullOrEmpty(parentDir))
            Directory.CreateDirectory(parentDir);

        // Open SST file handles while holding _writeLock. Opening under the lock
        // guarantees no file is deleted by a concurrent compaction between the
        // snapshot and the open. FileShare.Delete allows compaction to remove the
        // directory entry while we still hold the handle — the file data remains
        // readable until we close it.
        var sstHandles = new List<(string FileName, FileStream Handle)>();
        await _writeLock.WaitAsync(cancellationToken);
        try {
            foreach (var filePath in _lsmStore.GetActiveFilePaths())
                sstHandles.Add((Path.GetFileName(filePath),
                    new FileStream(filePath, FileMode.Open, FileAccess.Read,
                        FileShare.ReadWrite | FileShare.Delete,
                        bufferSize: 81920, FileOptions.Asynchronous | FileOptions.SequentialScan)));
        } catch {
            foreach (var (_, h) in sstHandles) h.Dispose();
            _writeLock.Release();
            throw;
        }
        _writeLock.Release();

        // Write compressed archive. On any failure the partial archive is deleted
        // so callers are never left with a file that looks complete but is corrupt.
        var backupCreated = false;
        try {
            await using var zipStream = new FileStream(backupPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 81920, FileOptions.Asynchronous);
            await using var archive = new ZipArchive(zipStream, ZipArchiveMode.Create, leaveOpen: false);

            foreach (var (fileName, handle) in sstHandles)
                await AddStreamToArchiveAsync(archive, fileName, handle, cancellationToken);

            // Subscription log must be included so GlobalPosition continuity is preserved on restore.
            // SubscriptionPipeline initialises _globalSequence from the log's last position; without it
            // the sequence restarts at 0 and new events collide with pre-backup positions.
            foreach (var logFile in Directory.EnumerateFiles(_dataDirectory, "subscriptions-*.log").OrderBy(f => f))
                await AddToArchiveAsync(archive, logFile, cancellationToken);

            var walPath = Path.Combine(_dataDirectory, "wal.log");
            if (File.Exists(walPath))
                await AddToArchiveAsync(archive, walPath, cancellationToken);

            backupCreated = true;
        } finally {
            foreach (var (_, h) in sstHandles) h.Dispose();
            if (!backupCreated)
                try { File.Delete(backupPath); } catch { /* best-effort cleanup of partial archive */ }
        }
    }

    /// <summary>
    /// Extracts a backup archive created by <see cref="CreateBackupAsync"/> into
    /// <paramref name="targetDirectory"/>, replacing any existing SST files and WAL.
    /// After this call, open the store at <paramref name="targetDirectory"/> with
    /// <see cref="OpenAsync"/> to resume from the backup state.
    /// </summary>
    /// <remarks>
    /// This is an offline operation. The store at <paramref name="targetDirectory"/> must not be
    /// open while this method runs.
    /// </remarks>
    public static async ValueTask RestoreBackupAsync(
        string backupPath,
        string targetDirectory,
        CancellationToken cancellationToken = default) {

        Directory.CreateDirectory(targetDirectory);

        // Remove existing SST, .bf, subscription log, and WAL files from targetDirectory.
        foreach (var file in Directory.EnumerateFiles(targetDirectory, "*.sst")
                     .Concat(Directory.EnumerateFiles(targetDirectory, "*.bf"))
                     .Concat(Directory.EnumerateFiles(targetDirectory, "subscriptions-*.log")))
            File.Delete(file);

        var existingWal = Path.Combine(targetDirectory, "wal.log");
        if (File.Exists(existingWal))
            File.Delete(existingWal);

        // Extract archive entries to targetDirectory.
        // Validate each entry's resolved path to prevent ZipSlip: an entry with a
        // name like "../evil" or an absolute path must not escape targetDirectory.
        var resolvedTarget = Path.GetFullPath(targetDirectory).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;

        await using var zipStream = new FileStream(backupPath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 81920, FileOptions.Asynchronous);
        await using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read, leaveOpen: false);

        foreach (var entry in archive.Entries) {
            var destPath = Path.GetFullPath(Path.Combine(targetDirectory, entry.FullName));
            if (!destPath.StartsWith(resolvedTarget, StringComparison.OrdinalIgnoreCase))
                throw new InvalidDataException($"Backup entry '{entry.Name}' resolves outside the target directory.");
            await using var entryStream = entry.Open();
            await using var destStream = new FileStream(destPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 81920, FileOptions.Asynchronous);
            await entryStream.CopyToAsync(destStream, cancellationToken);
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

    static async Task AddToArchiveAsync(ZipArchive archive, string filePath, CancellationToken cancellationToken) {
        await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);
        await AddStreamToArchiveAsync(archive, Path.GetFileName(filePath), fileStream, cancellationToken);
    }

    static async Task AddStreamToArchiveAsync(ZipArchive archive, string entryName, Stream source, CancellationToken cancellationToken) {
        var entry = archive.CreateEntry(entryName, CompressionLevel.Optimal);
        await using var entryStream = entry.Open();
        await source.CopyToAsync(entryStream, cancellationToken);
    }

    /// <inheritdoc/>
    public void Dispose() {
        store.Dispose();
        _writeLock.Dispose();
        _metrics?.Dispose();
    }
}
