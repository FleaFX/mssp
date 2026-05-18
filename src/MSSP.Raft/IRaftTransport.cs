namespace MSSP.Raft;

public interface IRaftTransport {
    ValueTask<VoteResponse> RequestVoteAsync(string peerId, VoteRequest request, CancellationToken ct = default);
    ValueTask<AppendEntriesResponse> AppendEntriesAsync(string peerId, AppendEntriesRequest request, CancellationToken ct = default);
}
