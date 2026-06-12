namespace MSSP.Engine;

public sealed partial class EmbeddedMsspClient {
    /// <inheritdoc/>
    public ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken cancellationToken = default) =>
        _engine!.AppendAsync(streamId, expectedRevision, events, cancellationToken);
}
