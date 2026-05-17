# MSSP

MSSP (pronounced *Mississippi*) is a purpose-built event store database for .NET. It is designed to scale from embedded use in a single process all the way to a high-availability cluster.

> **Status:** early development — not production ready.

## Getting started (embedded)

For single-process use, install the embedded package:

```
dotnet add package MSSP.Embedded
```

Register with the .NET Generic Host:

```csharp
builder.Services.AddMssp(options => {
    options.DataDirectory = "./data";
});
```

`DataDirectory` is the only required option. `UseBloomFilters` defaults to `true` and `MemTableCapacityBytes` defaults to 64 MiB.

Inject `IMsspClient` where needed:

```csharp
// Append events — stream must not yet exist
await client.AppendAsync(
    streamId: new StreamId("order-123"),
    expectedRevision: StreamRevision.NoStream,
    events: [new EventData("OrderPlaced", JsonSerializer.SerializeToUtf8Bytes(payload))]);

// Append to an existing stream without a concurrency check
await client.AppendAsync(
    streamId: new StreamId("order-123"),
    expectedRevision: StreamRevision.Any,
    events: [new EventData("OrderShipped", JsonSerializer.SerializeToUtf8Bytes(payload))]);

// Read all events from the beginning
await foreach (var e in client.ReadAsync(new StreamId("order-123")))
    Console.WriteLine($"{e.Revision}: {e.EventType} at {e.Timestamp}");
```

`AppendAsync` throws `OptimisticConcurrencyException` when the actual stream revision does not match `expectedRevision`. Pass a specific `ulong` revision to implement optimistic locking.

## Architecture

MSSP is built on two foundational components:

**Write Ahead Log (WAL)**
All writes are persisted to the WAL before being acknowledged. In embedded mode this is a single append-only file. In cluster mode the WAL is replicated across nodes via a consensus algorithm (Raft), and a write is only durable once the cluster agrees.

**LSM Tree**
Events are stored in a log-structured merge tree:
- Level 0: an in-memory SkipList (MemTable)
- Levels 1–n: Sorted String Tables (SST) on disk, searched efficiently via bloom filters

## Roadmap

- [x] Log module (WAL foundation)
- [x] LSM tree (MemTable, SST format, sparse index, compaction)
- [x] Embedded event store (WAL + LSM, optimistic concurrency, recovery)
- [x] Bloom filters (`.bf` sidecar per SST file, opt-in via `BloomFilteredSstAccess<TKey>`)
- [x] Range queries in SkipList (`Scan(TKey from)` positions in O(log n) via skip list levels)
- [ ] Cluster mode (Raft consensus)

## Building

Requires .NET 10 SDK.

```bash
dotnet build src/MSSP.slnx
dotnet test src/MSSP.slnx
```