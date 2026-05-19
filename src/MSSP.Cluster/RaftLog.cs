using MSSP.Log;
using MSSP.LsmTree;
using MSSP.Raft;

namespace MSSP.Cluster;

/// <summary>
/// <see cref="ILog{TRecord}"/> implementation backed by Raft consensus.
/// Records are committed once a quorum accepts them; only committed records appear
/// on the <see cref="IAsyncEnumerable{T}"/> side.
/// </summary>
sealed class RaftLog(RaftNode node, RaftLogStateMachine stateMachine) : ILog<WalRecord> {
    /// <inheritdoc/>
    public async ValueTask<bool> TryAppendAsync(WalRecord record, CancellationToken cancellationToken = default) {
        ReadOnlyMemory<byte> payload = record;
        await node.ProposeAsync(payload, cancellationToken);
        return true;
    }

    /// <inheritdoc/>
    public IAsyncEnumerator<WalRecord> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
        stateMachine.CommittedRecords.GetAsyncEnumerator(cancellationToken);
}
