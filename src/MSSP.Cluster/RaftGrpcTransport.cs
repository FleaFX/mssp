using Grpc.Net.Client;
using MSSP.Cluster.Grpc;
using MSSP.Raft;
using AppendEntriesRequest = MSSP.Raft.AppendEntriesRequest;
using AppendEntriesResponse = MSSP.Raft.AppendEntriesResponse;
using InstallSnapshotRequest = MSSP.Raft.InstallSnapshotRequest;
using InstallSnapshotResponse = MSSP.Raft.InstallSnapshotResponse;
using VoteRequest = MSSP.Raft.VoteRequest;
using VoteResponse = MSSP.Raft.VoteResponse;
using GrpcLogEntry = MSSP.Cluster.Grpc.LogEntry;

namespace MSSP.Cluster;

/// <summary>
/// <see cref="IRaftTransport"/> implementation that routes Raft RPCs over gRPC.
/// One <see cref="GrpcChannel"/> is created per peer and kept open for the lifetime of the node.
/// </summary>
sealed class RaftGrpcTransport : IRaftTransport, IDisposable {
    readonly Dictionary<string, (GrpcChannel Channel, RaftConsensus.RaftConsensusClient Client)> _peers = new();
    readonly TimeSpan _rpcTimeout;

    /// <summary>
    /// Creates a transport and opens a gRPC channel to each peer in <paramref name="peers"/>.
    /// </summary>
    /// <param name="peers">The cluster members this node can send RPCs to.</param>
    /// <param name="rpcTimeout">
    /// Deadline applied to every Raft RPC call. If a call exceeds this duration the remote peer
    /// is treated as temporarily unavailable and the call is retried on the next heartbeat.
    /// A sensible default is five times the heartbeat interval.
    /// </param>
    public RaftGrpcTransport(IEnumerable<RaftClusterMember> peers, TimeSpan rpcTimeout) {
        _rpcTimeout = rpcTimeout;
        foreach (var peer in peers) {
            var socketsHandler = new System.Net.Http.SocketsHttpHandler {
                // HTTP/2 keep-alive pings prevent idle connections from being silently
                // closed by the OS or by Kestrel's idle-connection timeout.
                KeepAlivePingDelay = TimeSpan.FromSeconds(15),
                KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
            };
            var invoker = new System.Net.Http.HttpMessageInvoker(socketsHandler, disposeHandler: false);
            var opaqueHandler = new OpaqueInvokerHandler(invoker, socketsHandler);
            var channel = GrpcChannel.ForAddress(peer.Address, new GrpcChannelOptions {
                HttpHandler = opaqueHandler,
                DisposeHttpClient = true,
            });
            _peers[peer.NodeId] = (channel, new RaftConsensus.RaftConsensusClient(channel));
        }
    }

    /// <inheritdoc/>
    public async ValueTask<VoteResponse> RequestVoteAsync(string peerId, VoteRequest request, CancellationToken cancellationToken = default) {
        var client = GetClient(peerId);
        var grpcRequest = new Grpc.VoteRequest {
            Term = request.Term,
            CandidateId = request.CandidateId,
            LastLogIndex = request.LastLogIndex,
            LastLogTerm = request.LastLogTerm
        };
        var response = await client.RequestVoteAsync(grpcRequest,
            deadline: DateTime.UtcNow.Add(_rpcTimeout),
            cancellationToken: cancellationToken);
        return new VoteResponse(response.Term, response.VoteGranted);
    }

    /// <inheritdoc/>
    public async ValueTask<AppendEntriesResponse> AppendEntriesAsync(string peerId, AppendEntriesRequest request, CancellationToken cancellationToken = default) {
        var client = GetClient(peerId);
        var grpcRequest = new Grpc.AppendEntriesRequest {
            Term = request.Term,
            LeaderId = request.LeaderId,
            PrevLogIndex = request.PrevLogIndex,
            PrevLogTerm = request.PrevLogTerm,
            LeaderCommit = request.LeaderCommit
        };
        grpcRequest.Entries.AddRange(request.Entries.Select(e => new GrpcLogEntry {
            Term = e.Term,
            Index = e.Index,
            Type = (uint)e.Type,
            Payload = Google.Protobuf.ByteString.CopyFrom(e.Payload.Span)
        }));
        var response = await client.AppendEntriesAsync(grpcRequest,
            deadline: DateTime.UtcNow.Add(_rpcTimeout),
            cancellationToken: cancellationToken);
        return new AppendEntriesResponse(response.Term, response.Success, response.ConflictIndex, response.ConflictTerm);
    }

    /// <inheritdoc/>
    public async ValueTask<InstallSnapshotResponse> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request, CancellationToken cancellationToken = default) {
        var client = GetClient(peerId);
        var grpcRequest = new Grpc.InstallSnapshotRequest {
            Term = request.Term,
            LeaderId = request.LeaderId,
            LastIncludedIndex = request.LastIncludedIndex,
            LastIncludedTerm = request.LastIncludedTerm,
            Offset = request.Offset,
            Data = Google.Protobuf.ByteString.CopyFrom(request.Data.Span),
            Done = request.Done
        };
        var response = await client.InstallSnapshotAsync(grpcRequest,
            deadline: DateTime.UtcNow.Add(_rpcTimeout),
            cancellationToken: cancellationToken);
        return new InstallSnapshotResponse(response.Term);
    }

    RaftConsensus.RaftConsensusClient GetClient(string peerId) =>
        _peers.TryGetValue(peerId, out var peer) ? peer.Client
        : throw new InvalidOperationException($"Unknown peer: {peerId}");

    /// <inheritdoc/>
    public void Dispose() {
        foreach (var (channel, _) in _peers.Values)
            channel.Dispose();
    }
}
