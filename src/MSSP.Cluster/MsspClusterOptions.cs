using MSSP.Raft;

namespace MSSP.Cluster;

public sealed class MsspClusterOptions {
    public string NodeId { get; set; } = "node-1";
    public RaftClusterMember[] Peers { get; set; } = [];
    public int ElectionTimeoutMinMs { get; set; } = 150;
    public int ElectionTimeoutMaxMs { get; set; } = 300;
    public int HeartbeatIntervalMs { get; set; } = 50;
}
