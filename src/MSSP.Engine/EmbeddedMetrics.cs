using System.Diagnostics.Metrics;

namespace MSSP.Engine;

/// <summary>
/// Metrics for the embedded MSSP client. Tracks append operations, read operations,
/// conflicts, and active subscriptions.
/// </summary>
internal sealed class EmbeddedMetrics : IDisposable {
    readonly Meter _meter;
    readonly Counter<long> _appendCount;
    readonly Histogram<double> _appendDuration;
    readonly Counter<long> _appendConflicts;
    readonly Counter<long> _readCount;
    readonly UpDownCounter<int> _activeSubscriptions;

    /// <summary>
    /// Initializes a new instance of <see cref="EmbeddedMetrics"/>. 
    /// </summary>
    /// <param name="factory">The meter factory for creating meters.</param>
    internal EmbeddedMetrics(IMeterFactory factory) {
        _meter = factory.Create("MSSP");

        _appendCount = _meter.CreateCounter<long>(
            "mssp.append.count",
            unit: "{events}",
            description: "Total number of events written via AppendAsync.");

        _appendDuration = _meter.CreateHistogram<double>(
            "mssp.append.duration",
            unit: "ms",
            description: "End-to-end duration of AppendAsync.");

        _appendConflicts = _meter.CreateCounter<long>(
            "mssp.append.conflicts",
            description: "Number of OptimisticConcurrencyExceptions thrown by AppendAsync.");

        _readCount = _meter.CreateCounter<long>(
            "mssp.read.count",
            unit: "{events}",
            description: "Total number of events returned by ReadAsync.");

        _activeSubscriptions = _meter.CreateUpDownCounter<int>(
            "mssp.subscription.active",
            description: "Number of currently active SubscribeAsync calls.");
    }

    /// <summary>
    /// Records an append operation.
    /// </summary>
    /// <param name="eventCount">The number of events appended.</param>
    /// <param name="durationMs">The duration of the append operation in milliseconds.</param>
    internal void RecordAppend(long eventCount, long durationMs) {
        _appendCount.Add(eventCount);
        _appendDuration.Record(durationMs);
    }

    /// <summary>
    /// Records an optimistic concurrency conflict.
    /// </summary>
    internal void RecordConflict() => _appendConflicts.Add(1);

    /// <summary>
    /// Records a read operation.
    /// </summary>
    /// <param name="eventCount">The number of events read.</param>
    internal void RecordRead(long eventCount) => _readCount.Add(eventCount);

    /// <summary>
    /// Called when a subscription starts.
    /// </summary>
    internal void SubscriptionStarted() => _activeSubscriptions.Add(1);

    /// <summary>
    /// Called when a subscription stops.
    /// </summary>
    internal void SubscriptionStopped() => _activeSubscriptions.Add(-1);

    /// <summary>
    /// Disposes the meter.
    /// </summary>
    public void Dispose() => _meter.Dispose();
}
