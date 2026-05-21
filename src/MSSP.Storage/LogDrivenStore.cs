using System.Buffers.Binary;
using System.Collections.Concurrent;

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

        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending.Enqueue(tcs);

        bool appended;
        try {
            appended = await _log.TryAppendAsync(WalRecord.From(key, value), cancellationToken);
        } catch {
            _pending.TryDequeue(out _);
            throw;
        }

        if (!appended) {
            _pending.TryDequeue(out _);
            throw new InvalidOperationException("WAL append failed.");
        }

        await tcs.Task.WaitAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> ScanAllFrom(TKey from)
        => _inner.ScanAllFrom(from);

    /// <inheritdoc/>
    public IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> ScanSnapshotFrom(TKey from)
        => _inner.ScanSnapshotFrom(from);

    void StartApplyLoop() {
        _loopCts = new CancellationTokenSource();
        _loopTask = RunApplyLoopAsync(_loopCts.Token);
    }

    async Task RunApplyLoopAsync(CancellationToken cancellationToken) {
        try {
            await foreach (var record in _log.WithCancellation(cancellationToken)) {
                ReadOnlyMemory<byte> bytes = record;
                var span = bytes.Span;

                if (span.Length < 5) {
                    if (_pending.TryDequeue(out var badTcs))
                        badTcs.TrySetException(new InvalidDataException("Malformed WAL record: too short."));
                    continue;
                }

                var keyLen = BinaryPrimitives.ReadInt32LittleEndian(span[1..]);
                if (keyLen < 0 || 5 + keyLen > span.Length) {
                    if (_pending.TryDequeue(out var badTcs))
                        badTcs.TrySetException(new InvalidDataException("Malformed WAL record: invalid key length."));
                    continue;
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
                    tcs.SetResult(true);
            }
        } catch (OperationCanceledException) {
            // normal shutdown
        } finally {
            while (_pending.TryDequeue(out var tcs))
                tcs.TrySetCanceled();
        }
    }

    /// <inheritdoc/>
    public void Dispose() {
        _loopCts?.Cancel();
        // Swallow: the loop exits cleanly on cancellation; any unexpected exception was
        // already propagated to the caller via the TCS before the loop exited.
        try { _loopTask?.GetAwaiter().GetResult(); } catch { }
        while (_pending.TryDequeue(out var tcs))
            tcs.TrySetCanceled();
        _loopCts?.Dispose();
        _inner.Dispose();
        (_log as IDisposable)?.Dispose();
    }
}
