using System.Buffers.Binary;
using System.Threading.Channels;
using MSSP.Storage;

namespace MSSP.Embedded;

/// <summary>
/// Decorator around <see cref="ILsmStore{TKey}"/> that transparently assigns a <see cref="GlobalPosition"/>
/// to each written event, appends it to the <see cref="SubscriptionLog"/>, and publishes it to the
/// <see cref="SubscriptionBus"/> — without the client being aware of any of this.
/// </summary>
/// <remarks>
/// All methods that mutate state must be called while the caller holds the write lock.
/// </remarks>
public sealed class SubscriptionPipeline : ILsmStore<EventKey>, ISubscriptionProvider {
    readonly ILsmStore<EventKey> _inner;
    readonly SubscriptionLog _subscriptionLog;
    readonly SubscriptionBus _bus = new();
    ulong _globalSequence;

    /// <summary>
    /// Creates a new pipeline wrapping <paramref name="inner"/>.
    /// Initialises the global sequence counter from the last position recorded in <paramref name="subscriptionLog"/>.
    /// </summary>
    public SubscriptionPipeline(ILsmStore<EventKey> inner, SubscriptionLog subscriptionLog) {
        _inner = inner;
        _subscriptionLog = subscriptionLog;
        _globalSequence = subscriptionLog.GetLastPosition().Value;
    }

    /// <summary>
    /// The <see cref="GlobalPosition"/> of the most recently written event.
    /// Must be read while holding the write lock.
    /// </summary>
    public GlobalPosition CurrentPosition => new(_globalSequence);

    /// <summary>
    /// The format of the underlying subscription log.
    /// </summary>
    public SubscriptionLogFormat LogFormat => _subscriptionLog.Format;

    /// <summary>
    /// Assigns the next <see cref="GlobalPosition"/>, injects it into the reserved slot of
    /// <paramref name="value"/> (last 8 bytes), writes to the inner store, appends to the
    /// subscription log, and publishes to the bus.
    /// Must be called while holding the write lock.
    /// </summary>
    public async ValueTask WriteAsync(EventKey key, ReadOnlyMemory<byte> value, CancellationToken ct) {
        var pos = new GlobalPosition(++_globalSequence);

        var buffer = new byte[value.Length];
        value.CopyTo(buffer);
        BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan()[^8..], pos.Value);
        ReadOnlyMemory<byte> injected = buffer;

        await _inner.WriteAsync(key, injected, ct);
        await _subscriptionLog.AppendAsync(pos, key, injected, ct);
        _bus.Publish(((EventValue)injected).ToSubscriptionEvent(key));
    }

    /// <summary>
    /// <inheritdoc cref="ILsmStore{TKey}.ScanSnapshotFrom"/>
    /// </summary>
    public IEnumerable<KeyValuePair<EventKey, ReadOnlyMemory<byte>?>> ScanSnapshotFrom(EventKey from)
        => _inner.ScanSnapshotFrom(from);

    /// <summary>
    /// <inheritdoc cref="ILsmStore{TKey}.ScanAllFrom"/>
    /// </summary>
    public IEnumerable<KeyValuePair<EventKey, ReadOnlyMemory<byte>?>> ScanAllFrom(EventKey from)
        => _inner.ScanAllFrom(from);

    /// <summary>
    /// Registers a live subscription channel for <paramref name="filter"/>.
    /// Must be called while holding the write lock.
    /// </summary>
    public ChannelReader<SubscriptionEvent> Register(SubscriptionFilter filter)
        => _bus.Register(filter);

    /// <summary>
    /// Unregisters a previously registered channel.
    /// Must be called while holding the write lock.
    /// </summary>
    public void Unregister(ChannelReader<SubscriptionEvent> reader)
        => _bus.Unregister(reader);

    /// <summary>
    /// Returns a snapshot of historical events from the subscription log, starting at <paramref name="from"/>.
    /// Must be called while holding the write lock to capture a consistent snapshot.
    /// </summary>
    public IEnumerable<SubscriptionEvent> ScanFrom(GlobalPosition from, Func<EventKey, SubscriptionEvent>? resolver = null)
        => _subscriptionLog.ScanFrom(from, resolver);

    /// <summary>
    /// Completes all active subscription channels. Called on shutdown.
    /// </summary>
    public void CompleteAll() => _bus.CompleteAll();

    /// <inheritdoc/>
    public void Dispose() {
        _bus.CompleteAll();
        _subscriptionLog.Dispose();
        _inner.Dispose();
    }
}
