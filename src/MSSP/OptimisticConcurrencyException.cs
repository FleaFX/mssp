namespace MSSP;

/// <summary>
/// Thrown when an append fails because the stream's current revision does not match the expected revision.
/// </summary>
public class OptimisticConcurrencyException(StreamId streamId, StreamRevision expectedRevision)
    : Exception($"Optimistic concurrency conflict on stream '{streamId}': expected revision was not satisfied.") {
    /// <summary>
    /// Gets the stream for which the conflict occurred.
    /// </summary>
    public StreamId StreamId { get; } = streamId;

    /// <summary>
    /// Gets the expected revision that was not satisfied.
    /// </summary>
    public StreamRevision ExpectedRevision { get; } = expectedRevision;
}
