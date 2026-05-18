using System.Buffers.Binary;
using System.Text.Json;
using MSSP.Embedded;
using MSSP.LsmTree;
using MSSP.Raft;

namespace MSSP.Cluster;

sealed class MsspStateMachine : IRaftStateMachine, IDisposable {
    readonly SemaphoreSlim _writeLock = new(1, 1);
    readonly LsmStore<EventKey> _store;
    readonly RevisionIndex _revisions = new();
    ulong _lastAppliedIndex;

    MsspStateMachine(LsmStore<EventKey> store, ulong lastAppliedIndex) {
        _store = store;
        _lastAppliedIndex = lastAppliedIndex;
    }

    public ulong LastAppliedIndex => _lastAppliedIndex;

    internal LsmStore<EventKey> Store => _store;

    public static async ValueTask<MsspStateMachine> OpenAsync(
        string dataDirectory,
        int memTableCapacityBytes,
        ulong checkpointIndex,
        CancellationToken ct = default) {

        Directory.CreateDirectory(dataDirectory);

        // Box so the delegate can capture the machine's live LastAppliedIndex once it's created.
        MsspStateMachine? machineRef = null;
        MemTableFlushedDelegate onFlushed = async fCt =>
            await WriteCheckpointAsync(dataDirectory, machineRef?.LastAppliedIndex ?? checkpointIndex, fCt);

        // Raft log IS the WAL — no separate WAL needed
        static ValueTask<bool> noOpWal(ReadOnlyMemory<byte> _, CancellationToken __) => ValueTask.FromResult(true);

        var options = new LsmStoreOptions<EventKey>(
            dataDirectory,
            memTableCapacityBytes,
            noOpWal,
            onFlushed);

        var store = await LsmStore<EventKey>.OpenAsync(options, AsyncEnumerable.Empty<ReadOnlyMemory<byte>>(), ct);
        var machine = new MsspStateMachine(store, checkpointIndex);
        machineRef = machine;

        // rebuild RevisionIndex from SST
        await machine.RebuildRevisionIndexAsync(ct);

        return machine;
    }

    public async ValueTask<bool> ApplyAsync(RaftLogEntry entry, CancellationToken ct = default) {
        if (entry.Type == RaftLogEntryType.NoOp) {
            _lastAppliedIndex = entry.Index;
            return true;
        }

        await _writeLock.WaitAsync(ct);
        try {
            var (streamId, expectedRevision, events) = AppendCommand.Deserialize(entry.Payload);
            var streamRevision = ToStreamRevision(expectedRevision);

            if (!_revisions.Contains(streamId)) {
                var (exists, revision) = LookupCurrentRevision(streamId);
                if (exists) _revisions.Set(streamId, revision);
            }

            if (!_revisions.CheckConcurrency(streamId, streamRevision)) {
                _lastAppliedIndex = entry.Index;
                return false; // OCC conflict — entry accepted by consensus, no events written
            }

            var baseRevision = _revisions.TryGet(streamId, out var current) ? current + 1 : 0UL;
            var timestamp = DateTimeOffset.UtcNow;
            var offset = 0UL;

            foreach (var eventData in events) {
                var key = new EventKey(streamId, baseRevision + offset++);
                ReadOnlyMemory<byte> value = EventValue.From(eventData, timestamp);
                await _store.WriteAsync(key, value, ct);
                _revisions.Set(streamId, key.Revision);
            }

            _lastAppliedIndex = entry.Index;
            return true;
        } finally {
            _writeLock.Release();
        }
    }

    internal IEnumerable<KeyValuePair<EventKey, ReadOnlyMemory<byte>?>> ScanSnapshotFrom(EventKey from) {
        _writeLock.Wait();
        try {
            return _store.ScanSnapshotFrom(from);
        } finally {
            _writeLock.Release();
        }
    }

    public static async ValueTask<ulong> ReadCheckpointIndexAsync(string dataDirectory, CancellationToken ct = default) {
        var path = CheckpointPath(dataDirectory);
        if (!File.Exists(path)) return 0;
        var json = await File.ReadAllTextAsync(path, ct);
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("lastApplied").GetUInt64();
    }

    static async ValueTask WriteCheckpointAsync(string dataDirectory, ulong lastApplied, CancellationToken ct) {
        var path = CheckpointPath(dataDirectory);
        var tmp = path + ".tmp";
        await File.WriteAllTextAsync(tmp, $"{{\"lastApplied\":{lastApplied}}}", ct);
        File.Move(tmp, path, overwrite: true);
    }

    async Task RebuildRevisionIndexAsync(CancellationToken ct) {
        // scan all keys to populate revision index
        await Task.CompletedTask; // store is available synchronously after open
        var allKeys = _store.ScanAllFrom(new EventKey(string.Empty, 0));
        foreach (var (key, _) in allKeys) {
            if (ct.IsCancellationRequested) return;
            _revisions.Set(key.StreamId, key.Revision);
        }
    }

    bool EnsureRevisionLoaded(string streamId) => _revisions.Contains(streamId);

    (bool Exists, ulong Revision) LookupCurrentRevision(string streamId) {
        ulong? max = null;
        foreach (var (key, _) in _store.ScanAllFrom(new EventKey(streamId, 0))) {
            if (key.StreamId != streamId) break;
            max = key.Revision;
        }
        return (max.HasValue, max ?? 0);
    }

    static StreamRevision ToStreamRevision(long value) => value switch {
        -1 => StreamRevision.Any,
        -2 => StreamRevision.NoStream,
        -3 => StreamRevision.StreamExists,
        >= 0 => (StreamRevision)(ulong)value,
        _ => StreamRevision.Any
    };

    static string CheckpointPath(string dataDirectory) => Path.Combine(dataDirectory, "raft-checkpoint.json");

    public void Dispose() {
        _writeLock.Dispose();
        _store.Dispose();
    }
}
