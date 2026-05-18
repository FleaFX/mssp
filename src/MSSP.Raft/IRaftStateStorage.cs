namespace MSSP.Raft;

public interface IRaftStateStorage {
    ValueTask<RaftPersistentState> LoadAsync(CancellationToken ct = default);
    ValueTask SaveAsync(RaftPersistentState state, CancellationToken ct = default);
}
