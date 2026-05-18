using System.Runtime.CompilerServices;
using MSSP.Embedded;
using MSSP.Raft;

namespace MSSP.Cluster;

sealed class ClusteredMsspClient(RaftHostedService raftService) : IMsspClient {
    public async ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken ct = default) {
        var node = raftService.Node;
        if (!node.IsLeader)
            throw new NotLeaderException(node.LeaderHint);

        var payload = AppendCommand.Serialize(streamId.Value, (long)expectedRevision, events);
        var result = await node.ProposeAsync(payload, ct);
        if (result.IsOccConflict)
            throw new OptimisticConcurrencyException(streamId, expectedRevision);
    }

    public async IAsyncEnumerable<RecordedEvent> ReadAsync(StreamId streamId, StreamRevision from = default, [EnumeratorCancellation] CancellationToken ct = default) {
        var node = raftService.Node;
        if (!node.IsLeader)
            throw new NotLeaderException(node.LeaderHint);

        var startKey = new EventKey(streamId.Value, 0UL);
        var scan = raftService.StateMachine.ScanSnapshotFrom(startKey);

        foreach (var (key, value) in scan) {
            if (ct.IsCancellationRequested) yield break;
            if (key.StreamId != streamId.Value) break;
            if (key.Revision < from || value is null) continue;
            yield return ((EventValue)value.Value).ToRecordedEvent(key);
        }
    }
}
