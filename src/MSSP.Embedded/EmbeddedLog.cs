using System.Collections.Concurrent;
using System.Threading.Channels;
using MSSP.Storage;

namespace MSSP.Embedded;

/// <summary>
/// <see cref="ILog{WalRecord}"/> implementation that drives a drain-to-empty flush loop for
/// group commit.
/// <para>
/// <see cref="TryAppendAsync"/> enqueues the record and signals the flush loop — no WAL write
/// happens on the caller's thread. The flush loop wakes on the first pending record, drains
/// everything available, writes the batch to the WAL, issues a single <c>fsync</c>, then posts
/// the records to the apply channel. Records only become visible to the apply loop — and their
/// callers' awaiting tasks only complete — after they are durably on disk.
/// </para>
/// <para>
/// This adaptive batching means low-load writes get their own flush (minimal latency) while
/// high-load writes share a flush (maximum throughput), with no fixed timer or tuning knob.
/// </para>
/// <para>
/// WAL rotation is requested via <see cref="RequestRotation"/>. The flush loop performs the
/// rotation between batches so the WAL is never written and rotated concurrently.
/// </para>
/// </summary>
sealed class EmbeddedLog : ILog<WalRecord>, IDisposable {
    readonly WalManager _wal;
    readonly Channel<WalRecord[]> _channel = Channel.CreateUnbounded<WalRecord[]>(new UnboundedChannelOptions {
        SingleReader = true,
        SingleWriter = true,
        AllowSynchronousContinuations = false
    });
    readonly ConcurrentQueue<WalRecord> _pendingFlush = new();
    readonly SemaphoreSlim _flushReady = new(0, 1);
    int _flushPending;  // Interlocked: 0 = idle, 1 = signalled
    volatile bool _rotationRequested;
    readonly CancellationTokenSource _flushCts = new();
    readonly Task _flushTask;

    internal EmbeddedLog(WalManager wal) {
        _wal = wal;
        _flushTask = RunFlushLoopAsync(_flushCts.Token);
    }

    /// <inheritdoc/>
    public ValueTask<bool> TryAppendAsync(WalRecord record, CancellationToken cancellationToken = default) {
        _pendingFlush.Enqueue(record);

        // Signal the flush loop only on the 0→1 transition to avoid spurious semaphore releases.
        if (Interlocked.Exchange(ref _flushPending, 1) == 0)
            _flushReady.Release();

        return ValueTask.FromResult(true);
    }

    /// <summary>
    /// Signals the flush loop to rotate the WAL after the current batch is written.
    /// Safe to call from any thread; the actual rotation is deferred to the flush loop.
    /// </summary>
    internal void RequestRotation() => _rotationRequested = true;

    async Task RunFlushLoopAsync(CancellationToken cancellationToken) {
        var batch = new List<WalRecord>();
        try {
            while (!cancellationToken.IsCancellationRequested) {
                await _flushReady.WaitAsync(cancellationToken);
                // Reset before draining: a writer arriving after the drain but before the next
                // WaitAsync will see 0 and correctly signal again.
                Interlocked.Exchange(ref _flushPending, 0);

                batch.Clear();
                while (_pendingFlush.TryDequeue(out var r))
                    batch.Add(r);

                if (batch.Count == 0) continue;

                // Rotate before writing the batch: the flush loop exclusively owns the WAL,
                // so rotation here is safe and the new batch lands in the fresh WAL.
                if (_rotationRequested) {
                    await _wal.RotateAsync(cancellationToken);
                    _rotationRequested = false;
                }

                foreach (var record in batch) {
                    ReadOnlyMemory<byte> bytes = record;
                    if (!await _wal.AppendAsync(bytes, cancellationToken))
                        throw new InvalidOperationException("WAL append failed.");
                }

                await _wal.FlushAsync(cancellationToken);
                _channel.Writer.TryWrite(batch.ToArray());
            }
        } catch (OperationCanceledException) {
            // Final drain: write any records enqueued after the last flush so they survive
            // orderly shutdown and can be recovered from WAL on the next startup.
            batch.Clear();
            while (_pendingFlush.TryDequeue(out var r))
                batch.Add(r);
            if (batch.Count > 0) {
                try {
                    foreach (var record in batch) {
                        ReadOnlyMemory<byte> bytes = record;
                        await _wal.AppendAsync(bytes, CancellationToken.None);
                    }
                    await _wal.FlushAsync(CancellationToken.None);
                } catch {
                    // Best-effort: callers will be cancelled regardless via the apply loop's finally.
                }
            }
        } catch (Exception ex) {
            _channel.Writer.Complete(ex);
        }
    }

    /// <inheritdoc/>
    public IAsyncEnumerator<WalRecord[]> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
        _channel.Reader.ReadAllAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);

    /// <inheritdoc/>
    public void Dispose() {
        _flushCts.Cancel();
        try { _flushTask.GetAwaiter().GetResult(); } catch { }
        _channel.Writer.Complete();
        _flushCts.Dispose();
        _wal.Dispose();
    }
}
