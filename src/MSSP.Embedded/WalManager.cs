using System.Runtime.CompilerServices;
using MSSP.Log;

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
        var walStream = new FileStream(walPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, bufferSize: 4096, FileOptions.WriteThrough | FileOptions.Asynchronous);
        return new WalManager(walPath, new StreamSegment<WalRecord>(walStream));
    }

    /// <summary>
    /// Appends a record to the WAL. Returns <see langword="false"/> if the append fails.
    /// </summary>
    internal ValueTask<bool> AppendAsync(ReadOnlyMemory<byte> record, CancellationToken ct) =>
        _wal.TryAppendAsync(record, ct);

    /// <summary>
    /// Truncates the WAL by replacing the current file with a new empty one.
    /// Called after a MemTable flush so replayed records on next open don't include already-flushed data.
    /// </summary>
    internal ValueTask RotateAsync(CancellationToken ct) {
        _wal.Dispose();
        var walStream = new FileStream(_walPath, FileMode.Create, FileAccess.ReadWrite, FileShare.None,
            bufferSize: 4096, FileOptions.WriteThrough | FileOptions.Asynchronous);
        _wal = new StreamSegment<WalRecord>(walStream);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Reads all records from the WAL as raw bytes.
    /// </summary>
    internal async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadAllAsync([EnumeratorCancellation] CancellationToken ct = default) {
        await foreach (var record in _wal.WithCancellation(ct))
            yield return (ReadOnlyMemory<byte>)record;
    }

    public void Dispose() => _wal.Dispose();
}
