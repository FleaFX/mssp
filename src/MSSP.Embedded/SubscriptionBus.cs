using System.Threading.Channels;

namespace MSSP.Embedded;

/// <summary>
/// Distributes live events to active subscriptions.
/// All methods must be called while holding the client write lock.
/// </summary>
public sealed class SubscriptionBus {
    readonly record struct Registration(SubscriptionFilter Filter, Channel<SubscriptionEvent> Channel);

    readonly List<Registration> _registrations = [];

    /// <summary>
    /// Registers a new subscription channel for <paramref name="filter"/> and returns its reader.
    /// </summary>
    public ChannelReader<SubscriptionEvent> Register(SubscriptionFilter filter) {
        var channel = Channel.CreateUnbounded<SubscriptionEvent>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
        _registrations.Add(new Registration(filter, channel));
        return channel.Reader;
    }

    /// <summary>
    /// Writes <paramref name="evt"/> to every registered channel whose filter matches it.
    /// </summary>
    public void Publish(SubscriptionEvent evt) {
        foreach (var reg in _registrations.Where(reg => reg.Filter.Matches(evt)))
            reg.Channel.Writer.TryWrite(evt);
    }

    /// <summary>
    /// Completes all active subscription channels and clears the registration list.
    /// </summary>
    public void CompleteAll() {
        foreach (var reg in _registrations)
            reg.Channel.Writer.TryComplete();
        _registrations.Clear();
    }

    /// <summary>
    /// Completes and removes the channel identified by <paramref name="reader"/>.
    /// </summary>
    public void Unregister(ChannelReader<SubscriptionEvent> reader) {
        for (var i = 0; i < _registrations.Count; i++) {
            if (!ReferenceEquals(_registrations[i].Channel.Reader, reader)) continue;

            _registrations[i].Channel.Writer.TryComplete();
            _registrations.RemoveAt(i);
            return;
        }
    }
}
