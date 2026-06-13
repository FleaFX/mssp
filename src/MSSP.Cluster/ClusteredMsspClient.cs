using System.Runtime.CompilerServices;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using MSSP.Engine;
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
sealed class ClusteredMsspClient(RaftNode node, EmbeddedMsspClient local, RaftClusterMember[] peers) : IMsspClient, IDisposable {

    readonly Lock _leaderClientLock = new();
    GrpcChannel? _leaderChannel;
    GrpcMsspClient? _leaderGrpcClient;
    string? _cachedLeaderNodeId;

    /// <inheritdoc/>
    public async ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken cancellationToken = default) {
        if (!node.IsLeader) {
            await ForwardAppendAsync(streamId, expectedRevision, events, cancellationToken);
            return;
        }

        try {
            await local.AppendAsync(streamId, expectedRevision, events, cancellationToken);
        } catch (NotLeaderException) {
            // Lost leadership between the IsLeader check and ProposeAsync — forward to the new leader.
            await ForwardAppendAsync(streamId, expectedRevision, events, cancellationToken);
        }
    }

    async ValueTask ForwardAppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken cancellationToken) {
        var leaderHint = await WaitForLeaderHintAsync(cancellationToken);
        if (leaderHint is null) {
            // We became leader (no-op committed) while waiting — serve locally.
            try {
                await local.AppendAsync(streamId, expectedRevision, events, cancellationToken);
                return;
            } catch (NotLeaderException) {
                // Lost leadership again between WaitForLeaderHintAsync returning and ProposeAsync
                // completing — restart the forwarding loop to discover the new leader.
                await ForwardAppendAsync(streamId, expectedRevision, events, cancellationToken);
                return;
            }
        }
        var grpcClient = GetOrCreateLeaderClient(leaderHint);
        var request = new AppendRequest { StreamId = streamId.Value, ExpectedRevision = (long)expectedRevision };
        foreach (var e in events)
            request.Events.Add(new GrpcEventData { EventType = e.EventType, Data = UnsafeByteOperations.UnsafeWrap(e.Data), Metadata = UnsafeByteOperations.UnsafeWrap(e.Metadata) });
        try {
            await grpcClient.AppendAsync(request, cancellationToken: cancellationToken);
        } catch (RpcException ex) when (ex.StatusCode == StatusCode.FailedPrecondition) {
            throw new OptimisticConcurrencyException(streamId, expectedRevision);
        }
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<RecordedEvent> ReadAsync(StreamId streamId, StreamRevision from = default, ReadDirection direction = ReadDirection.Forwards, long maxCount = long.MaxValue, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        await foreach (var e in local.ReadAsync(streamId, from, direction, maxCount, cancellationToken))
            yield return e;
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<SubscriptionEvent> SubscribeAsync(SubscriptionFilter filter, GlobalPosition fromPosition = default, [EnumeratorCancellation] CancellationToken cancellationToken = default) {

        await foreach (var e in local.SubscribeAsync(filter, fromPosition, cancellationToken))
            yield return e;
    }

    /// <summary>
    /// Waits until a leader is known and reachable via a peer address.
    /// </summary>
    /// <returns>
    /// The node ID of the peer to forward to, or <see langword="null"/> if this node itself
    /// became leader (no-op committed) while waiting — in which case the caller should
    /// handle the request locally.
    /// </returns>
    async ValueTask<string?> WaitForLeaderHintAsync(CancellationToken cancellationToken) {
        if (peers.Length == 0)
            throw new TimeoutException("No peers configured; cannot forward to leader.");
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (DateTime.UtcNow < deadline) {
            // This node may have just been elected leader but not yet committed its initial
            // no-op entry (so IsLeader was false when AppendAsync was entered).  Once the
            // no-op commits, IsLeader flips to true and we can serve the request locally.
            if (node.IsLeader)
                return null;
            if (node.LeaderHint is { }hint && hint != node.NodeId && peers.Any(p => p.NodeId == hint))
                return hint;
            await Task.Delay(50, cancellationToken);
        }
        throw new TimeoutException("Could not determine the cluster leader within the timeout period.");
    }

    GrpcMsspClient GetOrCreateLeaderClient(string leaderNodeId) {
        lock (_leaderClientLock) {
            if (_cachedLeaderNodeId != leaderNodeId) {
                _leaderChannel?.Dispose();
                var peer = peers.First(p => p.NodeId == leaderNodeId);
                // Use an opaque HttpMessageHandler wrapper so grpc-dotnet uses PassiveSubchannelTransport
                // instead of SocketConnectivitySubchannelTransport. The connectivity transport opens a
                // raw TCP monitoring socket that Kestrel (HttpProtocols.Http2) closes after ~1 second
                // (no HTTP/2 preface received). When Kestrel closes this socket, grpc-dotnet resets the
                // transport and disposes any in-flight gRPC calls with "gRPC call disposed."
                var socketsHandler = new System.Net.Http.SocketsHttpHandler();
                var invoker = new System.Net.Http.HttpMessageInvoker(socketsHandler, disposeHandler: false);
                var opaqueHandler = new OpaqueInvokerHandler(invoker, socketsHandler);
                _leaderChannel = GrpcChannel.ForAddress(peer.Address, new GrpcChannelOptions {
                    HttpHandler = opaqueHandler,
                    DisposeHttpClient = true,
                });
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
