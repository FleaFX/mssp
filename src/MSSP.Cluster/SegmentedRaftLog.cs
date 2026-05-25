using MSSP.Raft;

namespace MSSP.Cluster;

/// <summary>
/// <see cref="IRaftLog"/> implementation backed by a sequence of append-only segment files.
/// </summary>
/// <remarks>
/// <para>
/// Segment files are named <c>raft-{baseIndex:D20}.seg</c>, where <c>baseIndex</c> is the
/// one-based log index of the first entry in that segment. Entry layout inside each segment:
/// <c>[term:8LE][index:8LE][type:1][payloadLen:4LE][payload:N][crc32:4LE]</c>.
/// </para>
/// <para>
/// Snapshot metadata is stored in <c>raft-snapshot.json</c> and updated atomically on each
/// <see cref="CompactToAsync"/> call. Segment files fully covered by the snapshot are deleted.
/// </para>
/// <para>
/// A new segment is started whenever the active segment reaches <c>maxSegmentBytes</c>.
/// </para>
/// </remarks>
sealed partial class SegmentedRaftLog : IRaftLog, IDisposable {
    const int HeaderSize = 8 + 8 + 1 + 4;
    const int FooterSize = 4;
    const string SnapshotFile = "raft-snapshot.json";
    const string SegmentPrefix = "raft-";
    const string SegmentSuffix = ".seg";

    readonly string _dataDir;
    readonly long _maxSegmentBytes;
    readonly List<Segment> _segments = [];

    /// <inheritdoc/>
    public ulong LastIncludedIndex { get; private set; }

    /// <inheritdoc/>
    public ulong LastIncludedTerm  { get; private set; }

    /// <inheritdoc/>
    public ulong LastIndex => _segments.Count > 0 ? _segments[^1].LastIndex : LastIncludedIndex;

    /// <inheritdoc/>
    public ulong LastTerm  => _segments.Count > 0 ? _segments[^1].LastTerm  : LastIncludedTerm;

    SegmentedRaftLog(string dataDir, long maxSegmentBytes) {
        _dataDir = dataDir;
        _maxSegmentBytes = maxSegmentBytes;
    }

    async ValueTask EnsureActiveSegmentAsync(CancellationToken cancellationToken) {
        if (_segments.Count == 0 || _segments[^1].SizeBytes >= _maxSegmentBytes) {
            var baseIndex = LastIndex + 1;
            var name = $"{SegmentPrefix}{baseIndex:D20}{SegmentSuffix}";
            var path = Path.Combine(_dataDir, name);
            var seg = await Segment.CreateAsync(path, baseIndex, cancellationToken);
            _segments.Add(seg);
        }
    }

    Segment? FindSegment(ulong index) {
        if (index == 0 || index <= LastIncludedIndex || index > LastIndex) return null;
        var i = FindSegmentIndex(index);
        return i >= 0 ? _segments[i] : null;
    }

    int FindSegmentIndex(ulong index) {
        // binary search: find the last segment whose BaseIndex <= index
        var lo = 0;
        var hi = _segments.Count - 1;
        var result = -1;
        while (lo <= hi) {
            var mid = (lo + hi) / 2;
            if (_segments[mid].BaseIndex <= index) {
                result = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        if (result >= 0 && _segments[result].LastIndex >= index) return result;
        return -1;
    }

    /// <inheritdoc/>
    /// <inheritdoc/>
    public void Dispose() {
        foreach (var seg in _segments) seg.Dispose();
        _segments.Clear();
    }
}
