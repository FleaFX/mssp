using System.Runtime.CompilerServices;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using MSSP.Embedded;
using MSSP.Raft;
using AppendRequest = MSSP.Grpc.AppendRequest;
using GrpcEventData = MSSP.Grpc.EventData;
using GrpcMsspClient = MSSP.Grpc.Mssp.MsspClient;

namespace MSSP.Cluster;

/// <summary>
/// <see cref="IMsspClient"/> implementation for cluster nodes.
/// When this node is the leader, all requests are handled locally via <see cref="EmbeddedMsspClient"/>.
/// When it is a follower, writes are transparently forwarded to the leader over gRPC.
/// Reads and subscriptions are always served from the local node — the follower's LSM store contains
/// only committed entries, so data is always durable, and the local copy avoids adding read load to the leader.
/// </summary>
sealed class ClusteredMsspClient(
    RaftNode node,
    EmbeddedMsspClient local,
    RaftClusterMember[] peers
) : IMsspClient, IDisposable {

    readonly Lock _leaderClientLock = new();
    GrpcChannel? _leaderChannel;
    GrpcMsspClient? _leaderGrpcClient;
    string? _cachedLeaderNodeId;

    /// <inheritdoc/>
    public async ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken ct = default) {
        if (!node.IsLeader) {
            var leaderHint = await WaitForLeaderHintAsync(ct);
            var grpcClient = GetOrCreateLeaderClient(leaderHint);
            var request = new AppendRequest { StreamId = streamId.Value, ExpectedRevision = (long)expectedRevision };
            foreach (var e in events)
                request.Events.Add(new GrpcEventData { EventType = e.EventType, Data = ByteString.CopyFrom(e.Data.Span) });
            try {
                await grpcClient.AppendAsync(request, cancellationToken: ct);
            } catch (RpcException ex) when (ex.StatusCode == StatusCode.FailedPrecondition) {
                throw new OptimisticConcurrencyException(streamId, expectedRevision);
            }
            return;
        }

        await local.AppendAsync(streamId, expectedRevision, events, ct);
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<RecordedEvent> ReadAsync(StreamId streamId, StreamRevision from = default, [EnumeratorCancellation] CancellationToken ct = default) {
        await foreach (var e in local.ReadAsync(streamId, from, ct))
            yield return e;
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<SubscriptionEvent> SubscribeAsync(
        SubscriptionFilter filter,
        GlobalPosition fromPosition = default,
        [EnumeratorCancellation] CancellationToken ct = default) {

        await foreach (var e in local.SubscribeAsync(filter, fromPosition, ct))
            yield return e;
    }

    async ValueTask<string> WaitForLeaderHintAsync(CancellationToken ct) {
        if (peers.Length == 0)
            throw new TimeoutException("No peers configured; cannot forward to leader.");
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (DateTime.UtcNow < deadline) {
            var hint = node.LeaderHint;
            if (hint is not null && peers.Any(p => p.NodeId == hint))
                return hint;
            await Task.Delay(50, ct);
        }
        throw new TimeoutException("Could not determine the cluster leader within the timeout period.");
    }

    GrpcMsspClient GetOrCreateLeaderClient(string leaderNodeId) {
        lock (_leaderClientLock) {
            if (_cachedLeaderNodeId != leaderNodeId) {
                _leaderChannel?.Dispose();
                var peer = peers.First(p => p.NodeId == leaderNodeId);
                _leaderChannel = GrpcChannel.ForAddress(peer.Address);
                _leaderGrpcClient = new GrpcMsspClient(_leaderChannel);
                _cachedLeaderNodeId = leaderNodeId;
            }
            return _leaderGrpcClient!;
        }
    }

    /// <inheritdoc/>
    public void Dispose() {
        lock (_leaderClientLock) {
            _leaderChannel?.Dispose();
        }
    }
}
