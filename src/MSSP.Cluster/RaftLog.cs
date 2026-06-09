using System.Runtime.CompilerServices;
using System.Threading.Channels;
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
        await node.ProposeAsync(record, cancellationToken);
        return true;
    }

    /// <inheritdoc/>
    public IAsyncEnumerator<WalRecord[]> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
        AsBatches(stateMachine.CommittedRecords, cancellationToken).GetAsyncEnumerator(cancellationToken);

    static async IAsyncEnumerable<WalRecord[]> AsBatches(ChannelReader<WalRecord> reader, [EnumeratorCancellation] CancellationToken cancellationToken) {
        var batch = new List<WalRecord>();
        while (await reader.WaitToReadAsync(cancellationToken)) {
            batch.Clear();
            while (reader.TryRead(out var record))
                batch.Add(record);
            if (batch.Count > 0)
                yield return batch.ToArray();
        }
    }
}
