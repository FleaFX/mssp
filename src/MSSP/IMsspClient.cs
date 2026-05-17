namespace MSSP;

/// <summary>
/// Provides access to an MSSP event store.
/// </summary>
public interface IMsspClient {
    /// <summary>
    /// Appends events to the specified stream.
    /// </summary>
    /// <param name="streamId">The stream to append to.</param>
    /// <param name="expectedRevision">The expected current revision of the stream, used for optimistic concurrency.</param>
    /// <param name="events">The events to append.</param>
    /// <param name="ct">A cancellation token.</param>
    ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken ct = default);

    /// <summary>
    /// Reads events from the specified stream.
    /// </summary>
    /// <param name="streamId">The stream to read from.</param>
    /// <param name="from">The revision to start reading from. Defaults to the start of the stream.</param>
    /// <param name="ct">A cancellation token.</param>
    /// <returns>An async sequence of recorded events, in order from <paramref name="from"/>.</returns>
    IAsyncEnumerable<RecordedEvent> ReadAsync(StreamId streamId, StreamRevision from = default, CancellationToken ct = default);
}
