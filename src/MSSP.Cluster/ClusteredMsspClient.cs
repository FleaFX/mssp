using System.Runtime.CompilerServices;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using MSSP.LsmTree;
using MSSP.Raft;
using AppendRequest = MSSP.Grpc.AppendRequest;
using ReadRequest = MSSP.Grpc.ReadRequest;
using GrpcEventData = MSSP.Grpc.EventData;
using GrpcMsspClient = MSSP.Grpc.Mssp.MsspClient;

namespace MSSP.Cluster;

/// <summary>
/// <see cref="IMsspClient"/> implementation that routes writes through the Raft leader.
/// When this node is the leader, requests are handled locally. When it is a follower,
/// requests are transparently forwarded to the leader over gRPC.
/// </summary>
sealed class ClusteredMsspClient(RaftNode node, LsmStore<EventKey> store, RaftClusterMember[] peers) : IMsspClient, IDisposable {
    readonly SemaphoreSlim _writeLock = new(1, 1);
    readonly RevisionIndex _revisions = new();
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

        await _writeLock.WaitAsync(ct);
        try {
            if (!_revisions.Contains(streamId.Value)) {
                var (exists, revision) = LookupCurrentRevision(streamId.Value);
                if (exists) _revisions.Set(streamId.Value, revision);
            }

            if (!_revisions.CheckConcurrency(streamId.Value, expectedRevision))
                throw new OptimisticConcurrencyException(streamId, expectedRevision);

            var baseRevision = _revisions.TryGet(streamId.Value, out var current) ? current + 1 : 0UL;
            var timestamp = DateTimeOffset.UtcNow;
            var offset = 0UL;

            foreach (var eventData in events) {
                var key = new EventKey(streamId.Value, baseRevision + offset++);
                ReadOnlyMemory<byte> value = EventValue.From(eventData, timestamp);
                await store.WriteAsync(key, value, ct);
                _revisions.Set(streamId.Value, key.Revision);
            }
        } finally {
            _writeLock.Release();
        }
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<RecordedEvent> ReadAsync(StreamId streamId, StreamRevision from = default, [EnumeratorCancellation] CancellationToken ct = default) {
        if (!node.IsLeader) {
            var leaderHint = await WaitForLeaderHintAsync(ct);
            var grpcClient = GetOrCreateLeaderClient(leaderHint);
            var request = new ReadRequest { StreamId = streamId.Value, FromRevision = (ulong)(long)from };
            using var call = grpcClient.Read(request, cancellationToken: ct);
            while (await call.ResponseStream.MoveNext(ct)) {
                var e = call.ResponseStream.Current;
                yield return new RecordedEvent(
                    new StreamId(e.StreamId),
                    e.Revision,
                    e.EventType,
                    e.Data.Memory,
                    new DateTimeOffset(DateTimeOffset.UnixEpoch.Ticks + e.TimestampNs / 100L, TimeSpan.Zero));
            }
            yield break;
        }

        IEnumerable<KeyValuePair<EventKey, ReadOnlyMemory<byte>?>> scan;
        var startKey = new EventKey(streamId.Value, 0UL);

        await _writeLock.WaitAsync(ct);
        try {
            scan = store.ScanSnapshotFrom(startKey);
        } finally {
            _writeLock.Release();
        }

        foreach (var (key, value) in scan) {
            if (ct.IsCancellationRequested) yield break;
            if (key.StreamId != streamId.Value) break;
            if (key.Revision < from || value is null) continue;
            yield return ((EventValue)value.Value).ToRecordedEvent(key);
        }
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

    (bool exists, ulong revision) LookupCurrentRevision(string streamId) {
        ulong? max = null;
        foreach (var (key, _) in store.ScanAllFrom(new EventKey(streamId, 0UL))) {
            if (key.StreamId != streamId) break;
            max = key.Revision;
        }
        return (max.HasValue, max ?? 0UL);
    }

    /// <inheritdoc/>
    public void Dispose() {
        _writeLock.Dispose();
        lock (_leaderClientLock) {
            _leaderChannel?.Dispose();
        }
    }
}
