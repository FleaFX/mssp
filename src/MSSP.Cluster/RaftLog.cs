using MSSP.Storage;
using MSSP.Raft;

namespace MSSP.Cluster;

/// <summary>
/// <see cref="ILog{TRecord}"/> implementation backed by Raft consensus.
/// Records are committed once a quorum accepts them; only committed records appear
/// on the <see cref="IAsyncEnumerable{T}"/> side.
/// </summary>
sealed class RaftLog(RaftNode node, RaftLogStateMachine stateMachine) : ILog<WalRecord> {
    /// <inheritdoc/>
    /// <exception cref="NotLeaderException">
    /// Thrown when this node is not the Raft leader. Callers should forward the request to the
    /// current leader rather than treating this as a fatal error.
    /// </exception>
    public async ValueTask<bool> TryAppendAsync(WalRecord record, CancellationToken cancellationToken = default) {
        await node.ProposeAsync((ReadOnlyMemory<byte>)record, cancellationToken);
        return true;
    }

    /// <inheritdoc/>
    public IAsyncEnumerator<WalRecord> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
        stateMachine.CommittedRecords.GetAsyncEnumerator(cancellationToken);
}
