namespace MSSP.Raft;

/// <summary>
/// Abstracts the network layer used by <see cref="RaftNode"/> to communicate with peer nodes.
/// </summary>
public interface IRaftTransport {
    /// <summary>
    /// Sends a <see cref="VoteRequest"/> to the specified peer and awaits the <see cref="VoteResponse"/>.
    /// </summary>
    /// <param name="peerId">The node ID of the peer to contact.</param>
    /// <param name="request">The vote request to send.</param>
    /// <param name="ct">Token to cancel the RPC.</param>
    ValueTask<VoteResponse> RequestVoteAsync(string peerId, VoteRequest request, CancellationToken ct = default);

    /// <summary>
    /// Sends an <see cref="AppendEntriesRequest"/> to the specified peer and awaits the <see cref="AppendEntriesResponse"/>.
    /// </summary>
    /// <param name="peerId">The node ID of the peer to contact.</param>
    /// <param name="request">The append-entries request (or heartbeat) to send.</param>
    /// <param name="ct">Token to cancel the RPC.</param>
    ValueTask<AppendEntriesResponse> AppendEntriesAsync(string peerId, AppendEntriesRequest request, CancellationToken ct = default);
}
