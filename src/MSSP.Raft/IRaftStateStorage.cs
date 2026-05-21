namespace MSSP.Raft;

/// <summary>
/// Durable storage for the <see cref="RaftPersistentState"/> that must survive crashes.
/// </summary>
/// <remarks>
/// The Raft paper requires that <see cref="RaftPersistentState.CurrentTerm"/> and
/// <see cref="RaftPersistentState.VotedFor"/> be persisted before any RPC response is sent.
/// Implementations must guarantee that <see cref="SaveAsync"/> is crash-safe (e.g. atomic rename).
/// </remarks>
public interface IRaftStateStorage {
    /// <summary>
    /// Loads the previously saved persistent state, or returns a default
    /// <see cref="RaftPersistentState"/> with term 0 and no vote if nothing has been saved yet.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    ValueTask<RaftPersistentState> LoadAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Durably persists the given <paramref name="state"/> before returning.
    /// </summary>
    /// <param name="state">The state to persist.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    ValueTask SaveAsync(RaftPersistentState state, CancellationToken cancellationToken = default);
}
