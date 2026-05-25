namespace MSSP.Raft;

sealed class InMemoryRaftTransport : IRaftTransport {
    readonly Dictionary<string, RaftNode> _nodes = new();

    public void Register(RaftNode node) => _nodes[node.NodeId] = node;

    public async ValueTask<VoteResponse> RequestVoteAsync(string peerId, VoteRequest request, CancellationToken cancellationToken = default) {
        if (!_nodes.TryGetValue(peerId, out var node))
            throw new InvalidOperationException($"Unknown peer: {peerId}");
        return await node.ReceiveVoteRequestAsync(request, cancellationToken);
    }

    public async ValueTask<AppendEntriesResponse> AppendEntriesAsync(string peerId, AppendEntriesRequest request, CancellationToken cancellationToken = default) {
        if (!_nodes.TryGetValue(peerId, out var node))
            throw new InvalidOperationException($"Unknown peer: {peerId}");
        return await node.ReceiveAppendEntriesAsync(request, cancellationToken);
    }

    public async ValueTask<InstallSnapshotResponse> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request, CancellationToken cancellationToken = default) {
        if (!_nodes.TryGetValue(peerId, out var node))
            throw new InvalidOperationException($"Unknown peer: {peerId}");
        return await node.ReceiveInstallSnapshotAsync(request, cancellationToken);
    }
}
