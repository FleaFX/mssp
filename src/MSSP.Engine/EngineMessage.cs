using System.Threading.Channels;
using MSSP.Engine.Storage;

namespace MSSP.Engine;

/// <summary>
/// Base type for messages processed by the <see cref="StoreEngine"/> actor loop.
/// </summary>
abstract record EngineMessage;

/// <summary>
/// Requests an append of one or more events to a stream.
/// Posted by <see cref="StoreEngine.AppendAsync"/> and processed by the actor loop.
/// </summary>
sealed record AppendCommand(
    StreamId StreamId,
    StreamRevision ExpectedRevision,
    IEnumerable<EventData> Events,
    DateTimeOffset Timestamp,
    TaskCompletionSource<bool> Reply
) : EngineMessage;

/// <summary>
/// Carries a batch of WAL records that were durably committed by the log.
/// Posted by <c>RunCommittedBatchReaderAsync</c> and processed by the actor loop.
/// </summary>
sealed record CommittedBatch(WalRecord[] Records) : EngineMessage;

/// <summary>
/// Requests a handle-owning snapshot of the LSM store for off-thread iteration.
/// The caller receives a <see cref="LsmStoreSnapshot{TKey}"/> and is responsible for disposing it.
/// </summary>
sealed record CaptureSnapshotCommand(
    TaskCompletionSource<LsmStoreSnapshot<EventKey>> Reply
) : EngineMessage;

/// <summary>
/// Registers a live subscription and captures the catch-up scan position on the actor thread.
/// </summary>
sealed record RegisterSubscriptionCommand(
    SubscriptionFilter Filter,
    GlobalPosition FromPosition,
    TaskCompletionSource<SubscriptionRegistration> Reply
) : EngineMessage;

/// <summary>
/// Unregisters and completes the live channel identified by <paramref name="Channel"/>.
/// Fire-and-forget: no reply required.
/// </summary>
sealed record UnregisterSubscriptionCommand(
    ChannelReader<SubscriptionEvent> Channel
) : EngineMessage;

/// <summary>
/// Opens raw file streams for all active SST files, suitable for streaming into a backup archive.
/// The caller is responsible for disposing each returned stream.
/// </summary>
sealed record OpenBackupStreamsCommand(
    TaskCompletionSource<IReadOnlyList<FileStream>> Reply
) : EngineMessage;

/// <summary>
/// Replaces the store's SST files with a snapshot from <see cref="StagingDirectory"/>
/// and resets in-memory state. Called after a Raft snapshot install.
/// </summary>
sealed record ReloadSnapshotCommand(
    string StagingDirectory,
    TaskCompletionSource<bool> Reply
) : EngineMessage;

/// <summary>
/// Posted by the background flush task when <see cref="Storage.LsmStore{TKey}.FlushJob.RunAsync"/> finishes.
/// The actor loop calls <see cref="Storage.LsmStore{TKey}.FlushJob.CompleteAsync"/> on receipt.
/// </summary>
sealed record FlushCompleted(
    LsmStore<EventKey>.FlushJob Job,
    Exception? Error = null
) : EngineMessage;

/// <summary>
/// Returned by <see cref="RegisterSubscriptionCommand"/>. Contains everything the subscriber
/// needs to perform catch-up and then switch to the live channel.
/// </summary>
sealed record SubscriptionRegistration(
    ChannelReader<SubscriptionEvent> LiveChannel,
    IEnumerable<SubscriptionEvent> CatchUpScan,
    GlobalPosition CatchUpPosition,
    LsmStoreSnapshot<EventKey>? ResolverSnapshot
);
