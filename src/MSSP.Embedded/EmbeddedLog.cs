using System.Collections.Concurrent;
using System.Threading.Channels;
using MSSP.Storage;

namespace MSSP.Embedded;

/// <summary>
/// <see cref="ILog{WalRecord}"/> implementation that writes to the WAL and drives a
/// drain-to-empty flush loop for group commit.
/// <para>
/// <see cref="TryAppendAsync"/> writes to the OS kernel buffer (fast, no fsync) and signals
/// the flush loop. The flush loop wakes on the first pending record, drains everything
/// available, issues a single <c>fsync</c> for the entire batch, then posts the records to
/// the apply channel. Records only become visible to the apply loop — and their callers'
/// awaiting tasks only complete — after they are durably on disk.
/// </para>
/// <para>
/// This adaptive batching means low-load writes get their own flush (minimal latency) while
/// high-load writes share a flush (maximum throughput), with no fixed timer or tuning knob.
/// </para>
/// </summary>
sealed class EmbeddedLog : ILog<WalRecord>, IDisposable {
    readonly WalManager _wal;
    readonly Channel<WalRecord> _channel = Channel.CreateUnbounded<WalRecord>(new UnboundedChannelOptions {
        SingleReader = true,
        SingleWriter = false,
        AllowSynchronousContinuations = false
    });
    readonly ConcurrentQueue<WalRecord> _pendingFlush = new();
    readonly SemaphoreSlim _flushReady = new(0, 1);
    int _flushPending;  // Interlocked: 0 = idle, 1 = signalled
    readonly CancellationTokenSource _flushCts = new();
    readonly Task _flushTask;

    internal EmbeddedLog(WalManager wal) {
        _wal = wal;
        _flushTask = RunFlushLoopAsync(_flushCts.Token);
    }

    /// <inheritdoc/>
    public async ValueTask<bool> TryAppendAsync(WalRecord record, CancellationToken cancellationToken = default) {
        ReadOnlyMemory<byte> bytes = record;

        if (!await _wal.AppendAsync(bytes, cancellationToken))
            return false;
        _pendingFlush.Enqueue(record);

        // Signal the flush loop only on the 0→1 transition to avoid spurious semaphore releases.
        if (Interlocked.Exchange(ref _flushPending, 1) == 0)
            _flushReady.Release();

        return true;
    }

    async Task RunFlushLoopAsync(CancellationToken cancellationToken) {
        try {
            while (!cancellationToken.IsCancellationRequested) {
                await _flushReady.WaitAsync(cancellationToken);
                // Reset before draining: a writer arriving after the drain but before the next
                // WaitAsync will see 0 and correctly signal again.
                Interlocked.Exchange(ref _flushPending, 0);

                var batch = new List<WalRecord>();
                while (_pendingFlush.TryDequeue(out var r))
                    batch.Add(r);

                if (batch.Count == 0) continue;

                await _wal.FlushAsync(cancellationToken);

                foreach (var r in batch)
                    _channel.Writer.TryWrite(r);
            }
        } catch (OperationCanceledException) {
            // swallow
        } catch (Exception ex) {
            _channel.Writer.Complete(ex);
        }
    }

    /// <inheritdoc/>
    public IAsyncEnumerator<WalRecord> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
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
