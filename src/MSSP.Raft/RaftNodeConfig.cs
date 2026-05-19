namespace MSSP.Raft;

/// <summary>
/// Identifies a peer node in a Raft cluster.
/// </summary>
/// <param name="NodeId">The unique string identifier for this node.</param>
/// <param name="Address">The URI at which this node's Raft gRPC service is reachable.</param>
public sealed record RaftClusterMember(string NodeId, Uri Address);

/// <summary>
/// Immutable runtime configuration for a <see cref="RaftNode"/>.
/// </summary>
/// <param name="NodeId">The unique string identifier for this node within the cluster.</param>
/// <param name="PeerIds">The node IDs of all other cluster members; empty for a single-node cluster.</param>
/// <param name="ElectionTimeoutMinMs">
/// The lower bound of the randomized election timeout in milliseconds.
/// Must be significantly larger than <paramref name="HeartbeatIntervalMs"/> to avoid spurious elections.
/// </param>
/// <param name="ElectionTimeoutMaxMs">
/// The upper bound of the randomized election timeout in milliseconds.
/// Spreading candidates across this range reduces split-vote probability.
/// </param>
/// <param name="HeartbeatIntervalMs">
/// How often the leader sends <see cref="AppendEntriesRequest"/> heartbeats in milliseconds.
/// </param>
public sealed record RaftNodeConfig(
    string NodeId,
    string[] PeerIds,
    int ElectionTimeoutMinMs = 150,
    int ElectionTimeoutMaxMs = 300,
    int HeartbeatIntervalMs = 50);
