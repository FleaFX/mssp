# MSSP Samples

Sample projects that demonstrate how to use the MSSP NuGet packages.
## Samples

### EmbeddedSample

Single-process store: no network, everything in one process.

```powershell
cd src/Samples/EmbeddedSample
dotnet run
```

### ServerSample + ClientSample

Demonstrates the gRPC server/client split. Start the server first, then the client in a
separate terminal.

```powershell
# Terminal 1
cd src/Samples/ServerSample
dotnet run

# Terminal 2
cd src/Samples/ClientSample
dotnet run
```

Both use plain HTTP/2 (no TLS) on port 5000, so no certificate setup is needed.

### ClusterSample

A three-node Raft cluster. Choose between running it locally or in Docker.

#### Option A — Docker Compose (recommended)

```powershell
cd src/Samples/ClusterSample
docker compose up --build
```

This starts all three nodes, wires up the inter-node network automatically, and persists
each node's data in a named Docker volume. Once a leader is elected, run `ClientSample`
against any node port (6001, 6002, or 6003):

```powershell
# In a separate terminal:
cd src/Samples/ClientSample
dotnet run          # connects to http://localhost:6001 by default
```

To stop and remove containers (data volumes are preserved):

```powershell
docker compose down
```

To also remove the persisted data volumes:

```powershell
docker compose down --volumes
```

#### Option B — three terminals

```powershell
# Terminal 1
cd src/Samples/ClusterSample
dotnet run -- node-1 6001

# Terminal 2
cd src/Samples/ClusterSample
dotnet run -- node-2 6002

# Terminal 3
cd src/Samples/ClusterSample
dotnet run -- node-3 6003
```

Followers automatically forward writes to the current leader, so you can connect
a client to any node.
