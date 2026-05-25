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

    /// <summary>
    /// Creates a transport and opens a gRPC channel to each peer in <paramref name="peers"/>.
    /// </summary>
    /// <param name="peers">The cluster members this node can send RPCs to.</param>
    public RaftGrpcTransport(IEnumerable<RaftClusterMember> peers) {
        foreach (var peer in peers) {
            var channel = GrpcChannel.ForAddress(peer.Address);
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
        var response = await client.RequestVoteAsync(grpcRequest, cancellationToken: cancellationToken);
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
        var response = await client.AppendEntriesAsync(grpcRequest, cancellationToken: cancellationToken);
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
        var response = await client.InstallSnapshotAsync(grpcRequest, cancellationToken: cancellationToken);
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
