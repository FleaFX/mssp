using Microsoft.AspNetCore.Server.Kestrel.Core;
using MSSP.Cluster;
using MSSP.Engine;
using MSSP.Raft;    // RaftClusterMember is defined here; bundled inside the MSSP.Cluster package
using MSSP.Server;

// Required for gRPC over plain HTTP/2 — applies to both Kestrel (server) and
// the Raft transport channels (inter-node gRPC client).
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

// Usage: ClusterSample <nodeId> <port>
//   e.g. ClusterSample node-1 6001
if (args.Length < 2 || !int.TryParse(args[1], out var port)) {
    Console.Error.WriteLine("Usage: ClusterSample <nodeId> <port>");
    Console.Error.WriteLine("  node-1  6001");
    Console.Error.WriteLine("  node-2  6002");
    Console.Error.WriteLine("  node-3  6003");
    return;
}
var nodeId = args[0];

// Peer addresses default to localhost for running directly with 'dotnet run'.
// In Docker, set MSSP_PEERS to use container hostnames instead, e.g.:
//   node-1=http://node-1:6000,node-2=http://node-2:6000,node-3=http://node-3:6000
var peersEnv = Environment.GetEnvironmentVariable("MSSP_PEERS");
var peers = peersEnv is not null
    ? peersEnv.Split(',')
               .Select(p => p.Split('=', 2))
               .Select(p => new RaftClusterMember(p[0], new Uri(p[1])))
               .ToArray()
    : new RaftClusterMember[]
      {
          new("node-1", new Uri("http://localhost:6001")),
          new("node-2", new Uri("http://localhost:6002")),
          new("node-3", new Uri("http://localhost:6003")),
      };

var builder = WebApplication.CreateBuilder(args[2..]);

// ListenAnyIP binds to 0.0.0.0 so the node is reachable from peer containers.
// When running locally it still accepts connections on localhost:<port>.
// ListenAnyIP binds to 0.0.0.0 so the node is reachable from peer containers.
// When running locally it still accepts connections on localhost:<port>.
builder.WebHost.ConfigureKestrel(options => {
    options.ListenAnyIP(port, o => o.Protocols = HttpProtocols.Http2);
});

builder.Services
    .AddMssp(o => o.DataDirectory = $"./mssp-data-{nodeId}")
    .AddCluster(o => {
        o.NodeId = nodeId;
        o.Peers = peers;
        // Use conservative timeouts: 10× heartbeat gives ample margin for gRPC latency on localhost.
        o.HeartbeatIntervalMs = 100;
        o.ElectionTimeoutMinMs = 1000;
        o.ElectionTimeoutMaxMs = 2000;
    })
    .AddServer();

var app = builder.Build();
app.UseMssp();      // MSSP gRPC endpoint (for clients)
app.UseCluster();   // Raft gRPC endpoint (for inter-node communication)

Console.WriteLine($"Cluster node '{nodeId}' listening on port {port}");
Console.WriteLine($"Peers: {string.Join(", ", peers.Select(p => $"{p.NodeId}@{p.Address}"))}");
Console.WriteLine("Waiting for leader election...");
await app.RunAsync();
