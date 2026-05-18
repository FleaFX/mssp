using MSSP.Raft;

namespace MSSP.Raft;

sealed class InMemoryRaftLog : IRaftLog {
    readonly List<RaftLogEntry> _entries = new();

    public ulong LastIndex => (ulong)_entries.Count;
    public ulong LastTerm => _entries.Count > 0 ? _entries[^1].Term : 0;

    public ValueTask<RaftLogEntry> GetEntryAsync(ulong index, CancellationToken ct = default) {
        if (index == 0 || index > LastIndex)
            throw new ArgumentOutOfRangeException(nameof(index));
        return ValueTask.FromResult(_entries[(int)(index - 1)]);
    }

    public async IAsyncEnumerable<RaftLogEntry> GetEntriesFromAsync(ulong fromIndex, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default) {
        for (var i = fromIndex; i <= LastIndex; i++) {
            if (ct.IsCancellationRequested) yield break;
            yield return await GetEntryAsync(i, ct);
        }
    }

    public ValueTask AppendAsync(IEnumerable<RaftLogEntry> entries, CancellationToken ct = default) {
        _entries.AddRange(entries);
        return ValueTask.CompletedTask;
    }

    public ValueTask TruncateFromAsync(ulong fromIndex, CancellationToken ct = default) {
        if (fromIndex > 0 && fromIndex <= LastIndex)
            _entries.RemoveRange((int)(fromIndex - 1), _entries.Count - (int)(fromIndex - 1));
        return ValueTask.CompletedTask;
    }

    public ValueTask<ulong> GetTermAtAsync(ulong index, CancellationToken ct = default) {
        if (index == 0 || index > LastIndex)
            throw new ArgumentOutOfRangeException(nameof(index));
        return ValueTask.FromResult(_entries[(int)(index - 1)].Term);
    }
}
