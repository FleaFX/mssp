using System.Threading.Channels;
using MSSP.Engine.Storage;

namespace MSSP.Engine;

/// <summary>
/// Runs background jobs off the engine actor thread, one at a time.
/// <para>
/// Jobs are posted with <see cref="Enqueue"/> and drained by a single-reader consumer loop, so their
/// <see cref="IMaintenanceJob.RunAsync"/> calls are serialised structurally — without locks or
/// in-flight bookkeeping. When a job's work finishes, <paramref name="onCompleted"/> is invoked so the
/// owner can marshal the actor-thread completion back onto its own loop.
/// </para>
/// </summary>
/// <param name="onCompleted">
/// Invoked off-thread once a job's <see cref="IMaintenanceJob.RunAsync"/> completes, with the
/// faulting exception or <see langword="null"/> on success. Must not block.
/// </param>
/// <param name="cancellationToken">Cancels the consumer loop and any in-flight job on shutdown.</param>
sealed class JobPipeline<TJob>(Action<TJob, Exception?> onCompleted, CancellationToken cancellationToken) : IAsyncDisposable where TJob : IMaintenanceJob {
    readonly Channel<TJob> _channel = Channel.CreateUnbounded<TJob>(new UnboundedChannelOptions { SingleReader = true });

    Task? _loop;

    /// <summary>
    /// Starts the consumer loop. Must be called once before any <see cref="Enqueue"/>.
    /// </summary>
    public JobPipeline<TJob> Start() {
        _loop = RunAsync();
        return this;
    }

    /// <summary>
    /// Queues a job for serialised off-thread execution.
    /// </summary>
    public void Enqueue(TJob job) => _channel.Writer.TryWrite(job);

    async Task RunAsync() {
        try {
            await foreach (var job in _channel.Reader.ReadAllAsync(cancellationToken)) {
                Exception? error = null;
                try {
                    await job.RunAsync(cancellationToken);
                } catch (Exception ex) {
                    error = ex;
                }
                onCompleted(job, error);
            }
        } catch (OperationCanceledException) {
            // normal shutdown
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() {
        _channel.Writer.TryComplete();
        try {
            await (_loop ?? Task.CompletedTask);
        } catch {
            // swallow
        }
    }
}

/// <summary>
/// Runs compaction jobs off the actor thread using a pull model.
/// <para>
/// The worker keeps one <see cref="CompactionPlanRequest"/> outstanding in the actor mailbox at all times.
/// The actor holds the request until work becomes available, then calls <see cref="Respond"/> with a ready job.
/// Because plans are always made after the previous <c>CompleteAsync</c> has updated <c>_sstLevels</c>,
/// stale-plan races are structurally impossible.
/// </para>
/// </summary>
sealed class CompactionWorker(ChannelWriter<EngineMessage> actorMailbox, CancellationToken cancellationToken) : IAsyncDisposable {
    readonly Channel<LsmStore<EventKey>.CompactionJob> _response =
        Channel.CreateBounded<LsmStore<EventKey>.CompactionJob>(new BoundedChannelOptions(1) { SingleReader = true, SingleWriter = true });

    Task? _loop;

    /// <summary>
    /// Starts the worker loop. Must be called once before any use.
    /// </summary>
    public CompactionWorker Start() {
        _loop = RunAsync();
        return this;
    }

    /// <summary>
    /// Delivers a ready compaction job to the waiting worker loop.
    /// Called on the actor thread from <c>TryFulfillPendingPlanRequest</c>.
    /// </summary>
    public void Respond(LsmStore<EventKey>.CompactionJob job) => _response.Writer.TryWrite(job);

    async Task RunAsync() {
        try {
            while (true) {
                actorMailbox.TryWrite(new CompactionPlanRequest());
                var job = await _response.Reader.ReadAsync(cancellationToken);
                Exception? error = null;
                try {
                    await job.RunAsync(cancellationToken);
                } catch (Exception ex) {
                    error = ex;
                }
                actorMailbox.TryWrite(new CompactionCompleted(job, error));
            }
        } catch (OperationCanceledException) {
            // normal shutdown
        } catch (ChannelClosedException) {
            // normal shutdown — _response completed during disposal
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() {
        _response.Writer.TryComplete();
        try {
            await (_loop ?? Task.CompletedTask);
        } catch {
            // swallow
        }
    }
}
