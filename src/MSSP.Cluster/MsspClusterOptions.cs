using MSSP.Raft;

namespace MSSP.Cluster;

/// <summary>
/// Configuration options for the MSSP Raft cluster layer, supplied to
/// <see cref="MsspClusterBuilderExtensions.AddCluster"/>.
/// </summary>
public sealed class MsspClusterOptions {
    /// <summary>
    /// Gets or sets the unique identifier for this node within the cluster.
    /// Must be distinct from all <see cref="Peers"/> node IDs.
    /// </summary>
    public string NodeId { get; set; } = "node-1";

    /// <summary>
    /// Gets or sets the other cluster members. Each member provides its node ID and
    /// the address of its Raft gRPC endpoint.
    /// </summary>
    public RaftClusterMember[] Peers { get; set; } = [];

    /// <summary>
    /// Gets or sets the lower bound of the randomised election timeout in milliseconds.
    /// Must be significantly greater than <see cref="HeartbeatIntervalMs"/>.
    /// </summary>
    public int ElectionTimeoutMinMs { get; set; } = 150;

    /// <summary>
    /// Gets or sets the upper bound of the randomised election timeout in milliseconds.
    /// </summary>
    public int ElectionTimeoutMaxMs { get; set; } = 300;

    /// <summary>
    /// Gets or sets how often the leader sends heartbeats in milliseconds.
    /// </summary>
    public int HeartbeatIntervalMs { get; set; } = 50;
}
