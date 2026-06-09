using System.Runtime.CompilerServices;
using MSSP.Storage;

namespace MSSP.Embedded;

sealed class WalManager : IDisposable {
    readonly string _walPath;
    StreamSegment<WalRecord> _wal;

    WalManager(string walPath, StreamSegment<WalRecord> wal) {
        _walPath = walPath;
        _wal = wal;
    }

    /// <summary>
    /// Opens or creates the WAL file in <paramref name="dataDirectory"/> and returns a ready <see cref="WalManager"/>.
    /// </summary>
    internal static WalManager Open(string dataDirectory) {
        var walPath = Path.Combine(dataDirectory, "wal.log");
        var walStream = new FileStream(walPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, bufferSize: 4096, FileOptions.Asynchronous);
        return new WalManager(walPath, new StreamSegment<WalRecord>(walStream));
    }

    /// <summary>
    /// Appends a record to the WAL without flushing. Returns <see langword="false"/> if the append fails.
    /// Call <see cref="FlushAsync"/> to commit the batch to durable storage.
    /// </summary>
    internal ValueTask<bool> AppendAsync(ReadOnlyMemory<byte> record, CancellationToken cancellationToken) =>
        _wal.TryAppendAsync(record, flush: false, cancellationToken);

    /// <summary>
    /// Flushes all pending WAL writes to durable storage.
    /// </summary>
    internal ValueTask FlushAsync(CancellationToken cancellationToken) =>
        _wal.FlushAsync(cancellationToken);

    /// <summary>
    /// Truncates the WAL by replacing the current file with a new empty one.
    /// Called after a MemTable flush so replayed records on next open don't include already-flushed data.
    /// </summary>
    internal ValueTask RotateAsync(CancellationToken cancellationToken) {
        // Dispose the current stream before opening the new one; FileMode.Create
        // truncates/recreates the file. On failure to reopen, _wal is left as the
        // disposed segment so subsequent appends fail with ObjectDisposedException,
        // signalling the broken state.
        _wal.Dispose();
        var walStream = new FileStream(_walPath, FileMode.Create, FileAccess.ReadWrite, FileShare.Read,
            bufferSize: 4096, FileOptions.Asynchronous);
        _wal = new StreamSegment<WalRecord>(walStream);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Reads all records from the WAL as raw bytes.
    /// </summary>
    internal async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadAllAsync([EnumeratorCancellation] CancellationToken cancellationToken = default) {
        await foreach (var record in _wal.WithCancellation(cancellationToken))
            yield return record;
    }

    public void Dispose() => _wal.Dispose();
}
