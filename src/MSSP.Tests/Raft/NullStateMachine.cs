namespace MSSP.Raft;

sealed class NullStateMachine : IRaftStateMachine {
    ulong _lastApplied;

    public ulong LastAppliedIndex => _lastApplied;

    /// <summary>The snapshot data passed to the most recent <see cref="InstallSnapshotAsync"/> call.</summary>
    public ReadOnlyMemory<byte>? InstalledData { get; private set; }

    public ValueTask<bool> ApplyAsync(RaftLogEntry entry, CancellationToken cancellationToken = default) {
        _lastApplied = entry.Index;
        return ValueTask.FromResult(true);
    }

    public ValueTask<ReadOnlyMemory<byte>> CreateSnapshotAsync(CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(ReadOnlyMemory<byte>.Empty);

    public ValueTask InstallSnapshotAsync(ulong lastIncludedIndex, ulong lastIncludedTerm, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) {
        InstalledData = data;
        if (lastIncludedIndex > _lastApplied) _lastApplied = lastIncludedIndex;
        return ValueTask.CompletedTask;
    }
}
