using System.Runtime.CompilerServices;
using MSSP.Storage;

namespace MSSP.Embedded;

sealed class WalManager : IDisposable {
    readonly string _walPath;
    readonly string _walPrevPath;
    StreamSegment<WalRecord> _wal;

    WalManager(string walPath, string walPrevPath, StreamSegment<WalRecord> wal) {
        _walPath = walPath;
        _walPrevPath = walPrevPath;
        _wal = wal;
    }

    /// <summary>
    /// Opens or creates the WAL file in <paramref name="dataDirectory"/> and returns a ready <see cref="WalManager"/>.
    /// </summary>
    internal static WalManager Open(string dataDirectory) {
        var walPath     = Path.Combine(dataDirectory, "wal.log");
        var walPrevPath = Path.Combine(dataDirectory, "wal_prev.log");
        var walStream   = new FileStream(walPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, bufferSize: 4096, FileOptions.Asynchronous);
        return new WalManager(walPath, walPrevPath, new StreamSegment<WalRecord>(walStream));
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
    /// Archives the current WAL as <c>wal_prev.log</c> and opens a new empty <c>wal.log</c>.
    /// Any existing <c>wal_prev.log</c> is deleted first — by the two-generation invariant its
    /// records are guaranteed to be in SST at this point.
    /// </summary>
    internal ValueTask RotateAsync(CancellationToken cancellationToken) {
        _wal.Dispose();
        if (File.Exists(_walPrevPath))
            File.Delete(_walPrevPath);
        File.Move(_walPath, _walPrevPath);
        var walStream = new FileStream(_walPath, FileMode.Create, FileAccess.ReadWrite, FileShare.Read,
            bufferSize: 4096, FileOptions.Asynchronous);
        _wal = new StreamSegment<WalRecord>(walStream);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Reads all WAL records for startup recovery: <c>wal_prev.log</c> (if present) first,
    /// then <c>wal.log</c>.
    /// </summary>
    internal async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadAllForRecoveryAsync([EnumeratorCancellation] CancellationToken cancellationToken = default) {
        if (File.Exists(_walPrevPath)) {
            using var prevSeg = new StreamSegment<WalRecord>(
                new FileStream(_walPrevPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite,
                    bufferSize: 4096, FileOptions.Asynchronous | FileOptions.SequentialScan));
            await foreach (var record in prevSeg.WithCancellation(cancellationToken))
                yield return record;
        }
        await foreach (var record in _wal.WithCancellation(cancellationToken))
            yield return record;
    }

    /// <summary>
    /// Removes <c>wal_prev.log</c> if it exists. Called after recovery completes so that
    /// a subsequent startup does not re-replay records already applied to the MemTable.
    /// </summary>
    internal void DeletePrevWalIfExists() {
        if (File.Exists(_walPrevPath))
            File.Delete(_walPrevPath);
    }

    public void Dispose() => _wal.Dispose();
}
