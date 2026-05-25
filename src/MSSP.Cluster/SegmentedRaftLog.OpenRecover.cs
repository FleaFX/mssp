using System.Text.Json;

namespace MSSP.Cluster;

sealed partial class SegmentedRaftLog {
    /// <summary>
    /// Opens or creates the segmented Raft log in <paramref name="dataDirectory"/>.
    /// Performs torn-write recovery on each segment before returning.
    /// </summary>
    public static async ValueTask<SegmentedRaftLog> OpenAsync(
        string dataDirectory,
        long maxSegmentBytes = 64 * 1024 * 1024,
        CancellationToken cancellationToken = default) {

        Directory.CreateDirectory(dataDirectory);
        var log = new SegmentedRaftLog(dataDirectory, maxSegmentBytes);
        await log.RecoverAsync(cancellationToken);
        return log;
    }

    async Task RecoverAsync(CancellationToken cancellationToken) {
        var snapshotPath = Path.Combine(_dataDir, SnapshotFile);
        if (File.Exists(snapshotPath)) {
            var json = await File.ReadAllTextAsync(snapshotPath, cancellationToken);
            using var doc = JsonDocument.Parse(json);
            LastIncludedIndex = doc.RootElement.GetProperty("lastIncludedIndex").GetUInt64();
            LastIncludedTerm  = doc.RootElement.GetProperty("lastIncludedTerm").GetUInt64();
        }

        var segFiles = Directory.GetFiles(_dataDir, SegmentPrefix + "*" + SegmentSuffix)
            .Select(f => (path: f, baseIndex: ParseBaseIndex(Path.GetFileName(f))))
            .Where(t => t.baseIndex.HasValue)
            .OrderBy(t => t.baseIndex!.Value)
            .ToList();

        foreach (var (path, baseIndexOpt) in segFiles) {
            var baseIndex = baseIndexOpt!.Value;

            var seg = await Segment.OpenAsync(path, baseIndex, cancellationToken);
            if (seg.LastIndex > 0 && seg.LastIndex <= LastIncludedIndex) {
                seg.Dispose();
                File.Delete(path);
                continue;
            }

            _segments.Add(seg);
        }
    }

    static ulong? ParseBaseIndex(string fileName) {
        if (!fileName.StartsWith(SegmentPrefix) || !fileName.EndsWith(SegmentSuffix))
            return null;
        var digits = fileName[SegmentPrefix.Length..^SegmentSuffix.Length];
        return ulong.TryParse(digits, out var v) ? v : null;
    }
}
