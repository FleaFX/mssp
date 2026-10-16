namespace MSSP.Engine;

/// <summary>
/// An <see cref="IMsspClient"/> that forwards every call to the client of a hosted service.
/// </summary>
/// <remarks>
/// The generic host constructs every hosted service before it starts any of them, so a hosted
/// service that injects <see cref="IMsspClient"/> resolves it before the store is open. This
/// client can be resolved at any time; only a call made while the store is not open throws.
/// </remarks>
/// <param name="client">
/// Returns the open client, or throws <see cref="InvalidOperationException"/> while the store is not open.
/// </param>
internal sealed class HostedMsspClient(Func<IMsspClient> client) : IMsspClient {
    /// <inheritdoc/>
    public ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken cancellationToken = default) =>
        client().AppendAsync(streamId, expectedRevision, events, cancellationToken);

    /// <inheritdoc/>
    public IAsyncEnumerable<RecordedEvent> ReadAsync(StreamId streamId, StreamRevision from = default, ReadDirection direction = ReadDirection.Forwards, long maxCount = long.MaxValue, CancellationToken cancellationToken = default) =>
        client().ReadAsync(streamId, from, direction, maxCount, cancellationToken);

    /// <inheritdoc/>
    public IAsyncEnumerable<SubscriptionEvent> SubscribeAsync(SubscriptionFilter filter, GlobalPosition fromPosition = default, CancellationToken cancellationToken = default) =>
        client().SubscribeAsync(filter, fromPosition, cancellationToken);
}
