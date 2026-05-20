using System.Threading.Channels;
using MSSP;

namespace MSSP.Cluster;

/// <summary>
/// Distributes live events to active subscriptions.
/// All methods must be called while holding the client write lock.
/// </summary>
internal sealed class SubscriptionBus {
    readonly record struct Registration(SubscriptionFilter Filter, Channel<SubscriptionEvent> Channel);

    readonly List<Registration> _registrations = [];

    internal ChannelReader<SubscriptionEvent> Register(SubscriptionFilter filter) {
        var channel = Channel.CreateUnbounded<SubscriptionEvent>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
        _registrations.Add(new Registration(filter, channel));
        return channel.Reader;
    }

    internal void Publish(SubscriptionEvent evt) {
        foreach (var reg in _registrations)
            if (reg.Filter.Matches(evt))
                reg.Channel.Writer.TryWrite(evt);
    }

    internal void CompleteAll() {
        foreach (var reg in _registrations)
            reg.Channel.Writer.TryComplete();
        _registrations.Clear();
    }

    internal void Unregister(ChannelReader<SubscriptionEvent> reader) {
        for (int i = 0; i < _registrations.Count; i++) {
            if (ReferenceEquals(_registrations[i].Channel.Reader, reader)) {
                _registrations[i].Channel.Writer.TryComplete();
                _registrations.RemoveAt(i);
                return;
            }
        }
    }
}
