using System.Text.Json;
using System.Threading.Channels;
using MSSP.Engine.Storage;
using MSSP.Raft;

namespace MSSP.Cluster;

/// <summary>
/// <see cref="IRaftStateMachine"/> that forwards committed log entries to the <see cref="RaftLog"/> channel.
/// The actual storage apply happens in the <see cref="LsmStore{TKey}"/> apply loop.
/// </summary>
sealed class RaftLogStateMachine : IRaftStateMachine {
    readonly Channel<WalRecord> _channel = Channel.CreateUnbounded<WalRecord>(new UnboundedChannelOptions {
        SingleReader = true,
        SingleWriter = false,
        AllowSynchronousContinuations = false
    });
    ulong _lastAppliedIndex;

    /// <inheritdoc/>
    public ulong LastAppliedIndex => Volatile.Read(ref _lastAppliedIndex);

    /// <summary>
    /// The channel reader for committed WAL records, consumed by <see cref="RaftLog"/>.
    /// Exposing the reader directly allows <see cref="RaftLog"/> to drain all buffered records
    /// in one pass via <see cref="ChannelReader{T}.TryRead"/>, enabling group commit.
    /// </summary>
    internal ChannelReader<WalRecord> CommittedRecords => _channel.Reader;

    /// <inheritdoc/>
    public ValueTask<bool> ApplyAsync(RaftLogEntry entry, CancellationToken cancellationToken = default) {
        if (entry.Type == RaftLogEntryType.Command)
            _channel.Writer.TryWrite(entry.Payload);
        Volatile.Write(ref _lastAppliedIndex, entry.Index);
        return ValueTask.FromResult(true);
    }

    /// <summary>
    /// Advances <see cref="LastAppliedIndex"/> to <paramref name="index"/> without writing
    /// a WAL record to the committed-records channel. Used during startup replay, where WAL
    /// records are applied directly to the store during startup replay.
    /// </summary>
    internal void MarkApplied(ulong index) =>
        Volatile.Write(ref _lastAppliedIndex, index);

    /// <summary>
    /// Invoked by <see cref="RaftHostedService"/> to provide the physical snapshot bytes.
    /// When <see langword="null"/>, <see cref="CreateSnapshotAsync"/> returns an empty archive.
    /// </summary>
    internal Func<CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? SnapshotProvider { get; set; }

    /// <summary>
    /// Invoked by <see cref="RaftHostedService"/> to install the physical snapshot bytes.
    /// When <see langword="null"/>, <see cref="InstallSnapshotAsync"/> only advances the index.
    /// </summary>
    internal Func<ulong, ulong, ReadOnlyMemory<byte>, CancellationToken, ValueTask>? SnapshotInstaller { get; set; }

    /// <inheritdoc/>
    public ValueTask<ReadOnlyMemory<byte>> CreateSnapshotAsync(CancellationToken cancellationToken = default) =>
        SnapshotProvider?.Invoke(cancellationToken) ?? ValueTask.FromResult(ReadOnlyMemory<byte>.Empty);

    /// <inheritdoc/>
    public async ValueTask InstallSnapshotAsync(ulong lastIncludedIndex, ulong lastIncludedTerm, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) {
        if (SnapshotInstaller is not null)
            await SnapshotInstaller(lastIncludedIndex, lastIncludedTerm, data, cancellationToken);

        var current = Volatile.Read(ref _lastAppliedIndex);
        if (lastIncludedIndex > current)
            Volatile.Write(ref _lastAppliedIndex, lastIncludedIndex);
    }

    /// <summary>
    /// Reads the last-applied log index from <c>raft-checkpoint.json</c>, or returns zero if absent.
    /// </summary>
    public static async ValueTask<ulong> ReadCheckpointIndexAsync(string dataDirectory, CancellationToken cancellationToken = default) {
        var path = CheckpointPath(dataDirectory);
        if (!File.Exists(path)) return 0;
        var json = await File.ReadAllTextAsync(path, cancellationToken);
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("lastApplied").GetUInt64();
    }

    /// <summary>
    /// Writes the current <see cref="LastAppliedIndex"/> to <c>raft-checkpoint.json</c>.
    /// </summary>
    public static async ValueTask WriteCheckpointAsync(string dataDirectory, ulong lastApplied, CancellationToken cancellationToken) {
        var path = CheckpointPath(dataDirectory);
        var tmp = path + ".tmp";
        await File.WriteAllTextAsync(tmp, $"{{\"lastApplied\":{lastApplied}}}", cancellationToken);
        File.Move(tmp, path, overwrite: true);
    }

    static string CheckpointPath(string dataDirectory) => Path.Combine(dataDirectory, "raft-checkpoint.json");
}
