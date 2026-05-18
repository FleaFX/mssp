namespace MSSP.Raft;

public interface IRaftStateMachine {
    ulong LastAppliedIndex { get; }
    ValueTask<bool> ApplyAsync(RaftLogEntry entry, CancellationToken ct = default);
}
