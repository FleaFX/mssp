using System.Diagnostics.Metrics;

namespace MSSP.Cluster;

/// <summary>
/// Metrics for the Raft consensus component. Tracks term, leader status,
/// committed/applied indices, and election count.
/// </summary>
internal sealed class RaftMetrics : IDisposable {
    readonly Meter _meter;
    readonly Counter<long> _electionCount;

    // Push model: maintained as fields, updated by RaftHostedService
    long _term;
    int _isLeader;
    long _committedIndex;
    long _appliedIndex;

    /// <summary>
    /// Initializes a new instance of <see cref="RaftMetrics"/>. 
    /// </summary>
    /// <param name="factory">The meter factory for creating meters.</param>
    internal RaftMetrics(IMeterFactory factory) {
        _meter = factory.Create("MSSP.Cluster");

        _electionCount = _meter.CreateCounter<long>(
            "mssp.raft.election.count",
            description: "Number of elections started by this node.");

        _meter.CreateObservableGauge(
            "mssp.raft.term",
            () => _term,
            description: "Current Raft term.");

        _meter.CreateObservableGauge(
            "mssp.raft.is_leader",
            () => _isLeader,
            description: "1 if this node is the current leader, 0 otherwise.");

        _meter.CreateObservableGauge(
            "mssp.raft.committed_index",
            () => _committedIndex,
            description: "Last committed log index.");

        _meter.CreateObservableGauge(
            "mssp.raft.applied_index",
            () => _appliedIndex,
            description: "Last applied log index.");
    }

    /// <summary>
    /// Updates the current Raft state metrics.
    /// </summary>
    /// <param name="term">The current Raft term.</param>
    /// <param name="isLeader">Whether this node is the current leader.</param>
    /// <param name="committedIndex">The last committed log index.</param>
    /// <param name="appliedIndex">The last applied log index.</param>
    internal void Update(long term, bool isLeader, long committedIndex, long appliedIndex) {
        _term = term;
        _isLeader = isLeader ? 1 : 0;
        _committedIndex = committedIndex;
        _appliedIndex = appliedIndex;
    }

    /// <summary>
    /// Records that an election has started.
    /// </summary>
    internal void RecordElection() => _electionCount.Add(1);

    /// <summary>
    /// Disposes the meter.
    /// </summary>
    public void Dispose() => _meter.Dispose();
}
