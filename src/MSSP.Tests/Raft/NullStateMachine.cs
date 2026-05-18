using MSSP.Raft;

namespace MSSP.Raft;

sealed class NullStateMachine : IRaftStateMachine {
    ulong _lastApplied;

    public ulong LastAppliedIndex => _lastApplied;

    public ValueTask<bool> ApplyAsync(RaftLogEntry entry, CancellationToken ct = default) {
        _lastApplied = entry.Index;
        return ValueTask.FromResult(true);
    }
}
