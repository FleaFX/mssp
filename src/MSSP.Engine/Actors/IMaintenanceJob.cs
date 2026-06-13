namespace MSSP.Engine;

/// <summary>
/// Represents a single unit of background maintenance work (flush or compaction) that runs in two phases:
/// an off-thread I/O phase (<see cref="RunAsync"/>) and an actor-thread commit phase
/// (<c>CompleteAsync</c> on the concrete type).
/// </summary>
internal interface IMaintenanceJob {
    /// <summary>
    /// Executes the I/O-intensive part of the job off the actor thread.
    /// Must not touch shared mutable state; all inputs must be captured at construction time.
    /// </summary>
    ValueTask RunAsync(CancellationToken cancellationToken);
}
