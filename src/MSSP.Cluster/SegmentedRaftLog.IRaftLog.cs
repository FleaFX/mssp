using System.Runtime.CompilerServices;
using MSSP.Raft;

namespace MSSP.Cluster;

sealed partial class SegmentedRaftLog {
    /// <inheritdoc/>
    public async ValueTask<RaftLogEntry> GetEntryAsync(ulong index, CancellationToken cancellationToken = default) {
        var seg = FindSegment(index) ?? throw new ArgumentOutOfRangeException(nameof(index));
        return await seg.ReadEntryAsync(index, cancellationToken);
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<RaftLogEntry> GetEntriesFromAsync(
        ulong fromIndex,
        [EnumeratorCancellation] CancellationToken cancellationToken = default) {

        for (var i = fromIndex; i <= LastIndex; i++) {
            if (cancellationToken.IsCancellationRequested) yield break;
            yield return await GetEntryAsync(i, cancellationToken);
        }
    }

    /// <inheritdoc/>
    public async ValueTask AppendAsync(IEnumerable<RaftLogEntry> entries, CancellationToken cancellationToken = default) {
        foreach (var entry in entries) {
            await EnsureActiveSegmentAsync(cancellationToken);
            await _segments[^1].AppendEntryAsync(entry, cancellationToken);
        }
    }

    /// <inheritdoc/>
    public ValueTask TruncateFromAsync(ulong fromIndex, CancellationToken cancellationToken = default) {
        if (fromIndex == 0 || fromIndex > LastIndex) return ValueTask.CompletedTask;

        var segIdx = FindSegmentIndex(fromIndex);
        if (segIdx < 0) return ValueTask.CompletedTask;

        _segments[segIdx].TruncateFrom(fromIndex);

        for (var i = _segments.Count - 1; i > segIdx; i--) {
            _segments[i].DeleteAndDispose();
            _segments.RemoveAt(i);
        }

        if (_segments[segIdx].IsEmpty && _segments.Count > 1) {
            _segments[segIdx].DeleteAndDispose();
            _segments.RemoveAt(segIdx);
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc/>
    public async ValueTask<ulong> GetTermAtAsync(ulong index, CancellationToken cancellationToken = default) {
        if (index == LastIncludedIndex) return LastIncludedTerm;
        var entry = await GetEntryAsync(index, cancellationToken);
        return entry.Term;
    }

    /// <inheritdoc/>
    public async ValueTask CompactToAsync(
        ulong lastIncludedIndex,
        ulong lastIncludedTerm,
        CancellationToken cancellationToken = default) {

        if (lastIncludedIndex <= LastIncludedIndex) return;

        var snapshotPath = Path.Combine(_dataDir, SnapshotFile);
        var tmp = snapshotPath + ".tmp";
        await File.WriteAllTextAsync(tmp, $"{{\"lastIncludedIndex\":{lastIncludedIndex},\"lastIncludedTerm\":{lastIncludedTerm}}}", cancellationToken);
        File.Move(tmp, snapshotPath, overwrite: true);

        LastIncludedIndex = lastIncludedIndex;
        LastIncludedTerm  = lastIncludedTerm;

        for (var i = _segments.Count - 1; i >= 0; i--) {
            if (_segments[i].LastIndex > lastIncludedIndex) continue;
            _segments[i].DeleteAndDispose();
            _segments.RemoveAt(i);
        }
    }
}
