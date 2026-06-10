using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Threading;

namespace MSSP.Storage;

/// <summary>
/// <see cref="ILsmStore{TKey}"/> decorator that owns a write-ahead log and its apply loop.
/// <para>
/// On <see cref="WriteAsync"/>, the record is appended to the log and the caller waits until
/// the apply loop has forwarded it to the inner store. On shutdown or when follower nodes
/// receive committed entries from the Raft leader, the apply loop calls the inner store's
/// <see cref="ILsmStore{TKey}.WriteAsync"/> directly — feeding every decorator in the inner
/// chain on all nodes, not just the leader.
/// </para>
/// </summary>
public sealed class LogDrivenStore<TKey> : ILsmStore<TKey> where TKey : IKey<TKey> {
    readonly ILog<WalRecord> _log;
    readonly ILsmStore<TKey> _inner;
    readonly int _capacityBytes;
    readonly ConcurrentQueue<TaskCompletionSource<bool>> _pending = new();
    readonly SemaphoreSlim _enqueueGate = new(1, 1);
    CancellationTokenSource? _loopCts;
    Task? _loopTask;

    LogDrivenStore(ILog<WalRecord> log, ILsmStore<TKey> inner, int capacityBytes) {
        _log = log;
        _inner = inner;
        _capacityBytes = capacityBytes;
    }

    /// <summary>
    /// Creates a <see cref="LogDrivenStore{TKey}"/> wrapping <paramref name="inner"/> and
    /// immediately starts the apply loop.
    /// </summary>
    /// <param name="log">The write-ahead log that durably persists records before they are applied.</param>
    /// <param name="inner">The inner store that receives applied records; typically a decorator chain ending in <see cref="LsmStore{TKey}"/>.</param>
    /// <param name="capacityBytes">Maximum combined key+value size for a single record; matches the MemTable capacity of the inner store.</param>
    public static LogDrivenStore<TKey> Create(ILog<WalRecord> log, ILsmStore<TKey> inner, int capacityBytes) {
        var store = new LogDrivenStore<TKey>(log, inner, capacityBytes);
        store.StartApplyLoop();
        return store;
    }

    /// <summary>
    /// Serialises <paramref name="key"/> and <paramref name="value"/> as a WAL record, appends it
    /// to the log, then waits until the apply loop has forwarded the record to the inner store.
    /// </summary>
    public async ValueTask WriteAsync(TKey key, Memory<byte> value, CancellationToken cancellationToken) {
        ReadOnlyMemory<byte> keyBytes = key;
        var entrySize = keyBytes.Length + value.Length;

        if (entrySize > _capacityBytes)
            throw new InvalidOperationException("Single event exceeds MemTable capacity.");

        var record = WalRecord.From(key, value);
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        // The gate serialises the pair (Enqueue + log channel write) so _pending and the apply
        // channel always agree on record order across concurrent callers. It is held only for the
        // synchronous portion of TryAppendAsync; the async durability wait (quorum or flush)
        // happens outside it, so group commit is preserved.
        // The linked token converts a concurrent Dispose() into OperationCanceledException at
        // WaitAsync rather than ObjectDisposedException from a disposed semaphore.
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _loopCts!.Token);
        await _enqueueGate.WaitAsync(linked.Token);
        ValueTask<bool> appendTask;
        try {
            _pending.Enqueue(tcs);
            appendTask = _log.TryAppendAsync(record, cancellationToken);
        } finally {
            _enqueueGate.Release();
        }

        // If the append fails the TCS is already in _pending but no record will ever reach the
        // apply channel for it. Mark it as failed so the apply loop can drain the orphaned slot.
        try {
            if (!await appendTask)
                throw new InvalidOperationException("WAL append failed.");
        } catch (OperationCanceledException) {
            tcs.TrySetCanceled(cancellationToken);
            throw;
        } catch (Exception ex) {
            tcs.TrySetException(ex);
            throw;
        }

        await tcs.Task.WaitAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> ScanAllFrom(TKey from)
        => _inner.ScanAllFrom(from);

    /// <inheritdoc/>
    public IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> ScanSnapshotFrom(TKey from)
        => _inner.ScanSnapshotFrom(from);

    /// <summary>
    /// Applies a raw WAL record directly to the inner store, bypassing the log and the
    /// <see cref="_pending"/> TCS queue. Used during startup replay, where committed Raft
    /// log entries must be forwarded to the inner store synchronously — before the Raft
    /// node starts — so that replay entries never mix with real-write TCS slots in
    /// <see cref="_pending"/>.
    /// </summary>
    internal async ValueTask ReplayAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken) {
        var span = payload.Span;
        if (span.Length < 5) return;

        var keyLen = BinaryPrimitives.ReadInt32LittleEndian(span[1..]);
        if (keyLen < 0 || 5 + keyLen > span.Length) return;

        TKey key = payload.Slice(5, keyLen);
        Memory<byte> value = payload[(5 + keyLen)..].ToArray();
        await _inner.WriteAsync(key, value, cancellationToken);
    }

    void StartApplyLoop() {
        _loopCts = new CancellationTokenSource();
        _loopTask = RunApplyLoopAsync(_loopCts.Token);
    }

    async Task RunApplyLoopAsync(CancellationToken cancellationToken) {
        var batchTcss = new List<TaskCompletionSource<bool>>();
        try {
            await foreach (var batch in _log.WithCancellation(cancellationToken)) {
                batchTcss.Clear();
                foreach (var record in batch)
                    await ApplyRecordAsync(record, batchTcss, cancellationToken);
                await _inner.FlushAsync(cancellationToken);
                foreach (var tcs in batchTcss)
                    tcs.SetResult(true);
            }
        } catch (OperationCanceledException) {
            // normal shutdown
        } catch (Exception ex) {
            foreach (var tcs in batchTcss)
                tcs.TrySetException(ex);
            batchTcss.Clear();
            throw;
        } finally {
            foreach (var tcs in batchTcss) tcs.TrySetCanceled();
            while (_pending.TryDequeue(out var tcs))
                tcs.TrySetCanceled();
        }
    }

    async ValueTask ApplyRecordAsync(WalRecord record, List<TaskCompletionSource<bool>> batch, CancellationToken cancellationToken) {
        // Drain TCS entries that WriteAsync already marked as failed. Those writes produced no
        // record in the apply channel, so their slots must be skipped before matching this record.
        while (_pending.TryPeek(out var slot) && slot.Task.IsCompleted)
            _pending.TryDequeue(out _);

        ReadOnlyMemory<byte> bytes = record;
        var span = bytes.Span;

        if (span.Length < 5) {
            if (_pending.TryDequeue(out var badTcs))
                badTcs.TrySetException(new InvalidDataException("Malformed WAL record: too short."));
            return;
        }

        var keyLen = BinaryPrimitives.ReadInt32LittleEndian(span[1..]);
        if (keyLen < 0 || 5 + keyLen > span.Length) {
            if (_pending.TryDequeue(out var badTcs))
                badTcs.TrySetException(new InvalidDataException("Malformed WAL record: invalid key length."));
            return;
        }

        TKey key = bytes.Slice(5, keyLen);
        // Copy value slice to a mutable Memory<byte> so inner decorators can read/write it.
        var value = bytes[(5 + keyLen)..].ToArray();

        try {
            await _inner.WriteAsync(key, value, cancellationToken);
        } catch (Exception ex) when (ex is not OperationCanceledException) {
            if (_pending.TryDequeue(out var failedTcs))
                failedTcs.TrySetException(ex);
            throw;
        }

        if (_pending.TryDequeue(out var tcs))
            batch.Add(tcs);
    }

    /// <inheritdoc/>
    public void Dispose() {
        _loopCts?.Cancel();
        
        try { _loopTask?.GetAwaiter().GetResult(); } catch {
            // Swallow: the loop exits cleanly on cancellation; any unexpected exception was
            // // already propagated to the caller via the TCS before the loop exited.
        }

        while (_pending.TryDequeue(out var tcs))
            tcs.TrySetCanceled();
        _enqueueGate.Dispose();
        _loopCts?.Dispose();
        _inner.Dispose();
        (_log as IDisposable)?.Dispose();
    }
}
