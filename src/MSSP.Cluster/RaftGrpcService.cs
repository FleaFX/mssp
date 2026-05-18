using Grpc.Core;
using MSSP.Cluster.Grpc;
using MSSP.Raft;
using GrpcVoteRequest = MSSP.Cluster.Grpc.VoteRequest;
using GrpcVoteResponse = MSSP.Cluster.Grpc.VoteResponse;
using GrpcAppendEntriesRequest = MSSP.Cluster.Grpc.AppendEntriesRequest;
using GrpcAppendEntriesResponse = MSSP.Cluster.Grpc.AppendEntriesResponse;

namespace MSSP.Cluster;

/// <summary>
/// gRPC service that receives inbound Raft RPCs from peer nodes and forwards them
/// to the local <see cref="RaftNode"/> via the mailbox.
/// </summary>
sealed class RaftGrpcService(RaftHostedService raftService) : RaftConsensus.RaftConsensusBase {
    /// <summary>
    /// Handles a <c>RequestVote</c> RPC from a candidate peer.
    /// Translates the protobuf message to the Raft domain model, delegates to
    /// <see cref="RaftNode.ReceiveVoteRequestAsync"/>, and maps the result back.
    /// </summary>
    public override async Task<GrpcVoteResponse> RequestVote(GrpcVoteRequest request, ServerCallContext context) {
        var node = raftService.Node;
        var raftRequest = new MSSP.Raft.VoteRequest(
            request.Term, request.CandidateId,
            request.LastLogIndex, request.LastLogTerm);
        var response = await node.ReceiveVoteRequestAsync(raftRequest, context.CancellationToken);
        return new GrpcVoteResponse { Term = response.Term, VoteGranted = response.VoteGranted };
    }

    /// <summary>
    /// Handles an <c>AppendEntries</c> RPC from the current leader.
    /// Translates the protobuf message to the Raft domain model, delegates to
    /// <see cref="RaftNode.ReceiveAppendEntriesAsync"/>, and maps the result back.
    /// </summary>
    public override async Task<GrpcAppendEntriesResponse> AppendEntries(GrpcAppendEntriesRequest request, ServerCallContext context) {
        var node = raftService.Node;
        var entries = request.Entries.Select(e => new RaftLogEntry(
            e.Term, e.Index, (RaftLogEntryType)e.Type,
            e.Payload.Memory)).ToArray();
        var raftRequest = new MSSP.Raft.AppendEntriesRequest(
            request.Term, request.LeaderId,
            request.PrevLogIndex, request.PrevLogTerm,
            entries, request.LeaderCommit);
        var response = await node.ReceiveAppendEntriesAsync(raftRequest, context.CancellationToken);
        return new GrpcAppendEntriesResponse {
            Term = response.Term,
            Success = response.Success,
            ConflictIndex = response.ConflictIndex,
            ConflictTerm = response.ConflictTerm
        };
    }
}
