# MSSP

MSSP (pronounced *Mississippi*) is a purpose-built event store database for .NET. It is designed to scale from embedded use in a single process all the way to a high-availability cluster.

> **Status:** early development — not production ready.

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
- [ ] Range queries in SkipList (currently O(n) linear scan)
- [ ] Cluster mode (Raft consensus)

## Building

Requires .NET 10 SDK.

```bash
dotnet build src/MSSP.slnx
dotnet test src/MSSP.slnx
```