using System.Diagnostics.Metrics;

namespace MSSP.Storage;

/// <summary>
/// Metrics for the LSM store component. Tracks MemTable size, flush operations,
/// compaction operations, and SST file statistics.
/// </summary>
internal sealed class LsmStoreMetrics : IDisposable {
    readonly Meter _meter;

    // Flush
    readonly Counter<long> _flushCount;
    readonly Histogram<double> _flushDuration;

    // Compaction
    readonly Counter<long> _compactionCount;
    readonly Histogram<double> _compactionDuration;

    // MemTable (push model: maintained as regular fields, updated under write-lock)
    long _memTableSize;
    readonly long _memTableCapacity;

    // SST per level (push model: nested list maintained as atomic snapshot)
    // Use a volatile array reference for atomic swap
    volatile LevelSnapshot[] _levelSnapshots = [];

    /// <summary>
    /// Initializes a new instance of <see cref="LsmStoreMetrics"/>. 
    /// </summary>
    /// <param name="factory">The meter factory for creating meters.</param>
    /// <param name="memTableCapacity">The maximum capacity of the MemTable in bytes.</param>
    internal LsmStoreMetrics(IMeterFactory factory, long memTableCapacity) {
        _meter = factory.Create("MSSP.Storage");
        _memTableCapacity = memTableCapacity;

        _flushCount = _meter.CreateCounter<long>(
            "mssp.lsmstore.flush.count",
            description: "Number of completed MemTable flushes.");

        _flushDuration = _meter.CreateHistogram<double>(
            "mssp.lsmstore.flush.duration",
            unit: "ms",
            description: "Duration of a MemTable flush.");

        _compactionCount = _meter.CreateCounter<long>(
            "mssp.lsmstore.compaction.count",
            description: "Number of completed compactions per level.");

        _compactionDuration = _meter.CreateHistogram<double>(
            "mssp.lsmstore.compaction.duration",
            unit: "ms",
            description: "Duration of a compaction per level.");

        _meter.CreateObservableGauge<long>(
            "mssp.lsmstore.memtable.size",
            () => _memTableSize,
            unit: "bytes",
            description: "Current size of the MemTable in bytes.");

        _meter.CreateObservableGauge<long>(
            "mssp.lsmstore.memtable.capacity",
            () => _memTableCapacity,
            unit: "bytes",
            description: "Maximum capacity of the MemTable in bytes.");

        _meter.CreateObservableGauge<int>(
            "mssp.lsmstore.sst.files",
            () => _levelSnapshots.Select(s =>
                new Measurement<int>(s.FileCount, new KeyValuePair<string, object?>("level", s.LevelName))),
            unit: "{files}",
            description: "Number of SST files per level.");

        _meter.CreateObservableGauge<long>(
            "mssp.lsmstore.sst.bytes",
            () => _levelSnapshots.Select(s =>
                new Measurement<long>(s.TotalBytes, new KeyValuePair<string, object?>("level", s.LevelName))),
            unit: "bytes",
            description: "Total size of SST files per level.");
    }

    /// <summary>
    /// Called by FlushAsync after the SST file is written.
    /// </summary>
    /// <param name="durationMs">The duration of the flush in milliseconds.</param>
    /// <param name="memTableSize">The current MemTable size in bytes (typically 0 after flush).</param>
    /// <param name="levelSnapshots">Snapshot of all SST levels.</param>
    internal void RecordFlush(long durationMs, int memTableSize, LevelSnapshot[] levelSnapshots) {
        _flushCount.Add(1);
        _flushDuration.Record(durationMs);
        _memTableSize = memTableSize;
        _levelSnapshots = levelSnapshots;
    }

    /// <summary>
    /// Called by CompactLevelAsync after the compacted file is registered.
    /// </summary>
    /// <param name="levelName">The name of the level being compacted (e.g., "L1", "L2").</param>
    /// <param name="durationMs">The duration of the compaction in milliseconds.</param>
    /// <param name="levelSnapshots">Snapshot of all SST levels.</param>
    internal void RecordCompaction(string levelName, long durationMs, LevelSnapshot[] levelSnapshots) {
        var tag = new KeyValuePair<string, object?>("level", levelName);
        _compactionCount.Add(1, tag);
        _compactionDuration.Record(durationMs, tag);
        _levelSnapshots = levelSnapshots;
    }

    /// <summary>
    /// Called by WriteAsync to keep the MemTable size gauge current.
    /// </summary>
    /// <param name="size">The current MemTable size in bytes.</param>
    internal void UpdateMemTableSize(long size) => _memTableSize = size;

    /// <summary>
    /// Disposes the meter.
    /// </summary>
    public void Dispose() => _meter.Dispose();

    /// <summary>
    /// Snapshot of SST files at a single level.
    /// </summary>
    /// <param name="LevelName">The name of the level (e.g., "L1", "L2").</param>
    /// <param name="FileCount">The number of SST files at this level.</param>
    /// <param name="TotalBytes">The total size of all SST files at this level in bytes.</param>
    internal readonly record struct LevelSnapshot(string LevelName, int FileCount, long TotalBytes);
}
