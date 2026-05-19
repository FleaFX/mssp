# MSSP

MSSP (pronounced *Mississippi*) is a purpose-built event store for .NET 10. It scales from an embedded library in a single process all the way to a high-availability cluster.

> **Status:** early development — not production ready.

## Deployment modes

| Mode | Packages | Description |
|------|---------|-------------|
| Embedded | `MSSP.Embedded` | Runs in-process; no network required |
| Client-Server | `MSSP.Server` + `MSSP.Client` | Standalone server over gRPC |
| HA Cluster | `MSSP.Cluster` | Multi-node Raft consensus cluster |

All modes share the same `IMsspClient` interface:

```csharp
// Append events
await client.AppendAsync("orders-42", StreamRevision.NoStream, [
    new EventData("OrderPlaced", JsonSerializer.SerializeToUtf8Bytes(payload))
]);

// Read events
await foreach (var e in client.ReadAsync("orders-42"))
    Console.WriteLine($"{e.Revision}: {e.EventType}");
```

See the [wiki](../../wiki) for full setup instructions, architecture details, and operations guidance.

## Building

Requires .NET 10 SDK.

```bash
dotnet build src/MSSP.slnx
dotnet test src/MSSP.slnx
```
