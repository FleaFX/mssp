namespace MSSP.Raft;

public interface IRaftLog {
    ulong LastIndex { get; }
    ulong LastTerm { get; }
    ValueTask<RaftLogEntry> GetEntryAsync(ulong index, CancellationToken ct = default);
    IAsyncEnumerable<RaftLogEntry> GetEntriesFromAsync(ulong fromIndex, CancellationToken ct = default);
    ValueTask AppendAsync(IEnumerable<RaftLogEntry> entries, CancellationToken ct = default);
    ValueTask TruncateFromAsync(ulong fromIndex, CancellationToken ct = default);
    ValueTask<ulong> GetTermAtAsync(ulong index, CancellationToken ct = default);
}
