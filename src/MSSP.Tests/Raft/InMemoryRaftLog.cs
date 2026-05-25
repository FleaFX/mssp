namespace MSSP.Raft;

sealed class InMemoryRaftLog : IRaftLog {
    readonly List<RaftLogEntry> _entries = [];

    public ulong LastIndex => LastIncludedIndex + (ulong)_entries.Count;
    public ulong LastTerm => _entries.Count > 0 ? _entries[^1].Term : LastIncludedTerm;
    public ulong LastIncludedIndex { get; private set; }
    public ulong LastIncludedTerm { get; private set; }

    public ValueTask<RaftLogEntry> GetEntryAsync(ulong index, CancellationToken cancellationToken = default) {
        if (index == 0 || index <= LastIncludedIndex || index > LastIndex)
            throw new ArgumentOutOfRangeException(nameof(index));
        return ValueTask.FromResult(_entries[(int)(index - LastIncludedIndex - 1)]);
    }

    public async IAsyncEnumerable<RaftLogEntry> GetEntriesFromAsync(ulong fromIndex, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default) {
        for (var i = fromIndex; i <= LastIndex; i++) {
            if (cancellationToken.IsCancellationRequested) yield break;
            yield return await GetEntryAsync(i, cancellationToken);
        }
    }

    public ValueTask AppendAsync(IEnumerable<RaftLogEntry> entries, CancellationToken cancellationToken = default) {
        _entries.AddRange(entries);
        return ValueTask.CompletedTask;
    }

    public ValueTask TruncateFromAsync(ulong fromIndex, CancellationToken cancellationToken = default) {
        if (fromIndex > LastIncludedIndex && fromIndex <= LastIndex)
            _entries.RemoveRange((int)(fromIndex - LastIncludedIndex - 1), _entries.Count - (int)(fromIndex - LastIncludedIndex - 1));
        return ValueTask.CompletedTask;
    }

    public ValueTask<ulong> GetTermAtAsync(ulong index, CancellationToken cancellationToken = default) {
        if (index == LastIncludedIndex) return ValueTask.FromResult(LastIncludedTerm);
        if (index == 0 || index < LastIncludedIndex || index > LastIndex)
            throw new ArgumentOutOfRangeException(nameof(index));
        return ValueTask.FromResult(_entries[(int)(index - LastIncludedIndex - 1)].Term);
    }

    public ValueTask CompactToAsync(ulong lastIncludedIndex, ulong lastIncludedTerm, CancellationToken cancellationToken = default) {
        if (lastIncludedIndex >= LastIndex)
            _entries.Clear(); // snapshot covers all entries (InstallSnapshot scenario)
        else {
            var toRemove = (int)(lastIncludedIndex - LastIncludedIndex);
            if (toRemove > 0) _entries.RemoveRange(0, toRemove);
        }
        LastIncludedIndex = lastIncludedIndex;
        LastIncludedTerm = lastIncludedTerm;
        return ValueTask.CompletedTask;
    }
}
