namespace MSSP.Raft;

public sealed record RaftClusterMember(string NodeId, Uri Address);

public sealed record RaftNodeConfig(
    string NodeId,
    string[] PeerIds,
    int ElectionTimeoutMinMs = 150,
    int ElectionTimeoutMaxMs = 300,
    int HeartbeatIntervalMs = 50);
