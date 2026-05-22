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
    /// <param name="cancellationToken">A cancellation token.</param>
    ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads events from the specified stream.
    /// </summary>
    /// <param name="streamId">The stream to read from.</param>
    /// <param name="from">The revision to start reading from. Defaults to the start of the stream.</param>
    /// <param name="direction">The direction to read events. Defaults to <see cref="ReadDirection.Forwards"/>.</param>
    /// <param name="maxCount">The maximum number of events to read. Defaults to <see cref="long.MaxValue"/>.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>An async sequence of recorded events, in the order specified by <paramref name="direction"/>, starting from <paramref name="from"/>. </returns>
    IAsyncEnumerable<RecordedEvent> ReadAsync(StreamId streamId, StreamRevision from = default, ReadDirection direction = ReadDirection.Forwards, long maxCount = long.MaxValue, CancellationToken cancellationToken = default);

    /// <summary>
    /// Subscribes to an event stream, first replaying historical events from <paramref name="fromPosition"/> (catch-up),
    /// then delivering live events as they are written.
    /// </summary>
    /// <param name="filter">Determines which events are delivered.</param>
    /// <param name="fromPosition">The global position to start from. Defaults to <see cref="GlobalPosition.Start"/>.</param>
    /// <param name="cancellationToken">A cancellation token. Cancelling this token ends the subscription.</param>
    /// <returns>An async sequence of subscription events.</returns>
    IAsyncEnumerable<SubscriptionEvent> SubscribeAsync(SubscriptionFilter filter, GlobalPosition fromPosition = default, CancellationToken cancellationToken = default);
}
