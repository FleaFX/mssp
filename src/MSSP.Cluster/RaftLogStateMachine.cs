using System.Text.Json;
using System.Threading.Channels;
using MSSP.LsmTree;
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
    /// The stream of committed WAL records, consumed by <see cref="RaftLog"/>.
    /// </summary>
    internal IAsyncEnumerable<WalRecord> CommittedRecords => _channel.Reader.ReadAllAsync();

    /// <inheritdoc/>
    public ValueTask<bool> ApplyAsync(RaftLogEntry entry, CancellationToken ct = default) {
        if (entry.Type == RaftLogEntryType.Command)
            _channel.Writer.TryWrite((WalRecord)entry.Payload);
        Volatile.Write(ref _lastAppliedIndex, entry.Index);
        return ValueTask.FromResult(true);
    }

    /// <summary>
    /// Reads the last-applied log index from <c>raft-checkpoint.json</c>, or returns zero if absent.
    /// </summary>
    public static async ValueTask<ulong> ReadCheckpointIndexAsync(string dataDirectory, CancellationToken ct = default) {
        var path = CheckpointPath(dataDirectory);
        if (!File.Exists(path)) return 0;
        var json = await File.ReadAllTextAsync(path, ct);
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("lastApplied").GetUInt64();
    }

    /// <summary>
    /// Writes the current <see cref="LastAppliedIndex"/> to <c>raft-checkpoint.json</c>.
    /// </summary>
    public static async ValueTask WriteCheckpointAsync(string dataDirectory, ulong lastApplied, CancellationToken ct) {
        var path = CheckpointPath(dataDirectory);
        var tmp = path + ".tmp";
        await File.WriteAllTextAsync(tmp, $"{{\"lastApplied\":{lastApplied}}}", ct);
        File.Move(tmp, path, overwrite: true);
    }

    static string CheckpointPath(string dataDirectory) => Path.Combine(dataDirectory, "raft-checkpoint.json");
}
