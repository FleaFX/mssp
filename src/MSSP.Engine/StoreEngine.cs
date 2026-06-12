using System.Threading.Channels;
using MSSP.Engine.Storage;

namespace MSSP.Engine;

/// <summary>
/// Actor-based store engine that serialises all mutations through a single background loop.
/// <para>
/// Append requests are posted as <see cref="AppendCommand"/> messages. Committed WAL batches
/// arrive as <see cref="CommittedBatch"/> messages forwarded by a separate reader task.
/// The actor loop processes both in order, so no external locking is required for writes.
/// </para>
/// </summary>
sealed partial class StoreEngine(ILog<WalRecord> log, LsmStore<EventKey> store, SubscriptionLog subscriptionLog, long startPosition) : IAsyncDisposable {

    /// <summary>
    /// Tracks an in-flight append while its WAL records are being applied to the store.
    /// Dequeued and resolved by <c>HandleCommittedBatchAsync</c> when the committed position matches <see cref="LastPosition"/>.
    /// </summary>
    /// <param name="LastPosition">The global position of the last event in the append batch; used to match the committed WAL record.</param>
    /// <param name="Reply">The completion source to signal once the batch has been durably applied.</param>
    readonly record struct PendingAppend(ulong LastPosition, TaskCompletionSource<bool> Reply);

    readonly Channel<EngineMessage> _mailbox = Channel.CreateUnbounded<EngineMessage>(new UnboundedChannelOptions { SingleReader = true, AllowSynchronousContinuations = false });
    readonly RevisionIndex _revisions = new();
    readonly Queue<PendingAppend> _pending = new();
    readonly CancellationTokenSource _cts = new();
    readonly SubscriptionBus _subscriptionBus = new();

    JobPipeline<LsmStore<EventKey>.FlushJob>? _flush;
    CompactionWorker? _compaction;
    ulong _currentPosition = (ulong)startPosition;
    long _nextPosition = startPosition;
    Task? _actorTask;
    Task? _batchReaderTask;

    /// <summary>
    /// The <see cref="GlobalPosition"/> of the most recently applied event.
    /// </summary>
    public GlobalPosition CurrentPosition => new(_currentPosition);

    /// <summary>
    /// Starts the actor loop and the committed-batch reader task.
    /// Must be called once after construction and before any <see cref="AppendAsync"/> calls.
    /// </summary>
    public StoreEngine Start() {
        _flush = new JobPipeline<LsmStore<EventKey>.FlushJob>((job, error) => _mailbox.Writer.TryWrite(new FlushCompleted(job, error)), _cts.Token).Start();
        _compaction = new CompactionWorker(_mailbox.Writer, _cts.Token).Start();
        (_actorTask, _batchReaderTask) = (RunActorAsync(_cts.Token), RunCommittedBatchReaderAsync(_cts.Token));
        return this;
    }

    /// <summary>
    /// Posts an append command to the actor mailbox and returns a task that completes
    /// once the records have been durably committed and applied to the pipeline.
    /// </summary>
    public ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken cancellationToken) {
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_mailbox.Writer.TryWrite(new AppendCommand(streamId, expectedRevision, events, DateTimeOffset.UtcNow, tcs)))
            tcs.TrySetException(new ObjectDisposedException(nameof(StoreEngine)));
        return new ValueTask(tcs.Task.WaitAsync(cancellationToken));
    }

    async Task RunActorAsync(CancellationToken cancellationToken) {
        try {
            await foreach (var message in _mailbox.Reader.ReadAllAsync(cancellationToken)) {
                await (message switch {
                    AppendCommand append => HandleAppendAsync(append, cancellationToken),
                    CommittedBatch batch => HandleCommittedBatchAsync(batch, cancellationToken),
                    FlushCompleted fc => HandleFlushCompletedAsync(fc, cancellationToken),
                    CompactionCompleted cc => HandleCompactionCompletedAsync(cc, cancellationToken),
                    CompactionPlanRequest req => HandleCompactionPlanRequest(req),
                    CaptureSnapshotCommand snap => HandleCaptureSnapshot(snap),
                    OpenBackupStreamsCommand backup => HandleOpenBackupStreams(backup),
                    ReloadSnapshotCommand reload => HandleReloadSnapshot(reload, cancellationToken),
                    RegisterSubscriptionCommand reg => HandleRegisterSubscription(reg),
                    UnregisterSubscriptionCommand u => HandleUnregisterSubscription(u),
                    _ => ValueTask.CompletedTask
                });
            }
        } catch (OperationCanceledException) {
            // normal shutdown
        } catch (Exception ex) {
            while (_pending.TryDequeue(out var entry))
                entry.Reply.TrySetException(ex);
        }
    }

    async Task RunCommittedBatchReaderAsync(CancellationToken cancellationToken) {
        try {
            await foreach (var batch in log.WithCancellation(cancellationToken))
                _mailbox.Writer.TryWrite(new CommittedBatch(batch));
        } catch (OperationCanceledException) {
            // normal shutdown
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() {
        await _cts.CancelAsync();
        _mailbox.Writer.TryComplete();
        try { await (_actorTask ?? Task.CompletedTask); } catch { }
        try { await (_batchReaderTask ?? Task.CompletedTask); } catch { }
        while (_pending.TryDequeue(out var entry))
            entry.Reply.TrySetCanceled();
        _subscriptionBus.CompleteAll();
        if (_flush is not null) await _flush.DisposeAsync();
        if (_compaction is not null) await _compaction.DisposeAsync();
        subscriptionLog.Dispose();
        store.Dispose();
        _cts.Dispose();
    }
}
