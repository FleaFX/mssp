using MSSP.Storage;

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
