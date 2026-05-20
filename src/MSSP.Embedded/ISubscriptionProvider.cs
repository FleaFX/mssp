using System.Threading.Channels;

namespace MSSP.Embedded;

/// <summary>
/// Provides access to the subscription infrastructure: live channel registration,
/// historical catch-up scanning, and the current position watermark.
/// </summary>
public interface ISubscriptionProvider {
    /// <summary>
    /// The <see cref="GlobalPosition"/> of the most recently written event.
    /// Must be read while holding the write lock.
    /// </summary>
    GlobalPosition CurrentPosition { get; }

    /// <summary>
    /// The format of the underlying subscription log.
    /// </summary>
    SubscriptionLogFormat LogFormat { get; }

    /// <summary>
    /// Registers a live subscription channel for <paramref name="filter"/>.
    /// Must be called while holding the write lock.
    /// </summary>
    ChannelReader<SubscriptionEvent> Register(SubscriptionFilter filter);

    /// <summary>
    /// Unregisters a previously registered channel.
    /// Must be called while holding the write lock.
    /// </summary>
    void Unregister(ChannelReader<SubscriptionEvent> reader);

    /// <summary>
    /// Returns a snapshot of historical events from the subscription log, starting at <paramref name="from"/>.
    /// Must be called while holding the write lock to capture a consistent snapshot.
    /// </summary>
    IEnumerable<SubscriptionEvent> ScanFrom(GlobalPosition from, Func<EventKey, SubscriptionEvent>? resolver = null);
}
