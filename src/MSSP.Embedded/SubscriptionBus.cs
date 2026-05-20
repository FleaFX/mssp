using System.Threading.Channels;

namespace MSSP.Embedded;

/// <summary>
/// Distributes live events to active subscriptions.
/// All methods must be called while holding the client write lock.
/// </summary>
public sealed class SubscriptionBus {
    readonly record struct Registration(SubscriptionFilter Filter, Channel<SubscriptionEvent> Channel);

    readonly List<Registration> _registrations = [];

    public ChannelReader<SubscriptionEvent> Register(SubscriptionFilter filter) {
        var channel = Channel.CreateUnbounded<SubscriptionEvent>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
        _registrations.Add(new Registration(filter, channel));
        return channel.Reader;
    }

    public void Publish(SubscriptionEvent evt) {
        foreach (var reg in _registrations)
            if (reg.Filter.Matches(evt))
                reg.Channel.Writer.TryWrite(evt);
    }

    public void CompleteAll() {
        foreach (var reg in _registrations)
            reg.Channel.Writer.TryComplete();
        _registrations.Clear();
    }

    public void Unregister(ChannelReader<SubscriptionEvent> reader) {
        for (int i = 0; i < _registrations.Count; i++) {
            if (ReferenceEquals(_registrations[i].Channel.Reader, reader)) {
                _registrations[i].Channel.Writer.TryComplete();
                _registrations.RemoveAt(i);
                return;
            }
        }
    }
}
