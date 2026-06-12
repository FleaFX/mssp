using System.Threading.Channels;
using MSSP.Engine.Storage;

namespace MSSP.Engine;

/// <summary>
/// Runs LSM flush I/O off the engine actor thread, one flush at a time.
/// <para>
/// Jobs are posted with <see cref="Enqueue"/> and drained by a single-reader consumer loop, so their
/// <see cref="LsmStore{TKey}.FlushJob.RunAsync"/> calls are serialised structurally — without locks or
/// in-flight bookkeeping. When a job's I/O finishes, <paramref name="onCompleted"/> is invoked so the
/// owner can marshal the actor-thread completion (<see cref="LsmStore{TKey}.FlushJob.CompleteAsync"/>)
/// back onto its own loop.
/// </para>
/// </summary>
/// <param name="onCompleted">
/// Invoked off-thread once a job's <see cref="LsmStore{TKey}.FlushJob.RunAsync"/> completes, with the
/// faulting exception or <see langword="null"/> on success. Must not block.
/// </param>
/// <param name="cancellationToken">Cancels the consumer loop and any in-flight flush on shutdown.</param>
sealed class FlushPipeline(Action<LsmStore<EventKey>.FlushJob, Exception?> onCompleted, CancellationToken cancellationToken) : IAsyncDisposable {

    readonly Channel<LsmStore<EventKey>.FlushJob> _channel = Channel.CreateUnbounded<LsmStore<EventKey>.FlushJob>(new UnboundedChannelOptions { SingleReader = true });

    Task? _loop;

    /// <summary>
    /// Starts the consumer loop. Must be called once before any <see cref="Enqueue"/>.
    /// </summary>
    public FlushPipeline Start() {
        _loop = RunAsync();
        return this;
    }

    /// <summary>
    /// Queues a flush job for serialised off-thread execution.
    /// </summary>
    public void Enqueue(LsmStore<EventKey>.FlushJob job) => _channel.Writer.TryWrite(job);

    async Task RunAsync() {
        try {
            await foreach (var job in _channel.Reader.ReadAllAsync(cancellationToken)) {
                Exception? error = null;
                try { await job.RunAsync(cancellationToken); }
                catch (Exception ex) { error = ex; }
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
