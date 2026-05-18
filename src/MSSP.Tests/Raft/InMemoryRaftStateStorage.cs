namespace MSSP.Raft;

sealed class InMemoryRaftStateStorage : IRaftStateStorage {
    RaftPersistentState _state = new(0, null);

    public ValueTask<RaftPersistentState> LoadAsync(CancellationToken ct = default)
        => ValueTask.FromResult(_state);

    public ValueTask SaveAsync(RaftPersistentState state, CancellationToken ct = default) {
        _state = state;
        return ValueTask.CompletedTask;
    }
}
