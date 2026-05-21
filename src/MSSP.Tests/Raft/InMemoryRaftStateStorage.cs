namespace MSSP.Raft;

sealed class InMemoryRaftStateStorage : IRaftStateStorage {
    RaftPersistentState _state = new(0, null);

    public ValueTask<RaftPersistentState> LoadAsync(CancellationToken cancellationToken = default)
        => ValueTask.FromResult(_state);

    public ValueTask SaveAsync(RaftPersistentState state, CancellationToken cancellationToken = default) {
        _state = state;
        return ValueTask.CompletedTask;
    }
}
