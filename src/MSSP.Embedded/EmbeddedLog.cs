using System.Threading.Channels;
using MSSP.Storage;

namespace MSSP.Embedded;

/// <summary>
/// <see cref="ILog{TRecord}"/> implementation backed by a local WAL file.
/// Records are immediately committed (single node — no consensus required), so every
/// successful <see cref="TryAppendAsync"/> call publishes the record to the
/// <see cref="IAsyncEnumerable{T}"/> side without delay.
/// </summary>
sealed class EmbeddedLog : ILog<WalRecord>, IDisposable {
    readonly WalManager _wal;
    readonly Channel<WalRecord> _channel = Channel.CreateUnbounded<WalRecord>(new UnboundedChannelOptions {
        SingleReader = true,
        SingleWriter = false,
        AllowSynchronousContinuations = false
    });

    internal EmbeddedLog(WalManager wal) => _wal = wal;

    /// <inheritdoc/>
    public async ValueTask<bool> TryAppendAsync(WalRecord record, CancellationToken cancellationToken = default) {
        ReadOnlyMemory<byte> bytes = record;
        if (!await _wal.AppendAsync(bytes, cancellationToken))
            return false;
        _channel.Writer.TryWrite(record);
        return true;
    }

    /// <inheritdoc/>
    public IAsyncEnumerator<WalRecord> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
        _channel.Reader.ReadAllAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);

    /// <inheritdoc/>
    public void Dispose() => _wal.Dispose();
}
