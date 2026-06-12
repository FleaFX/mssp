using System.Threading.Channels;
using MSSP.Storage;

namespace MSSP.Engine;

sealed partial class StoreEngine {
    /// <summary>
    /// Schedules subscription registration on the actor thread.
    /// Returns a <see cref="SubscriptionRegistration"/> with the live channel, catch-up scan,
    /// and the position watermark at which catch-up ends and live begins.
    /// For ReferenceOnly log format, the registration also carries a snapshot that must be disposed
    /// by the caller once catch-up iteration is complete.
    /// </summary>
    public ValueTask<SubscriptionRegistration> RegisterSubscriptionAsync(SubscriptionFilter filter, GlobalPosition fromPosition, CancellationToken cancellationToken) {
        var tcs = new TaskCompletionSource<SubscriptionRegistration>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_mailbox.Writer.TryWrite(new RegisterSubscriptionCommand(filter, fromPosition, tcs)))
            tcs.TrySetException(new ObjectDisposedException(nameof(StoreEngine)));
        return new ValueTask<SubscriptionRegistration>(tcs.Task.WaitAsync(cancellationToken));
    }

    /// <summary>
    /// Posts an unregister command to the actor mailbox. Fire-and-forget: does not wait for the channel to close.
    /// </summary>
    public void UnregisterSubscription(ChannelReader<SubscriptionEvent> channel) =>
        _mailbox.Writer.TryWrite(new UnregisterSubscriptionCommand(channel));

    ValueTask HandleRegisterSubscription(RegisterSubscriptionCommand cmd) {
        LsmStoreSnapshot<EventKey>? resolverSnapshot = null;
        try {
            var catchUpPosition = CurrentPosition;
            var liveChannel = _subscriptionBus.Register(cmd.Filter);

            Func<EventKey, SubscriptionEvent>? resolver = null;
            if (subscriptionLog.Format != SubscriptionLogFormat.FullPayload) {
                resolverSnapshot = store.TakeReadSnapshot();
                resolver = key => ResolveFromSnapshot(resolverSnapshot, key);
            }

            var catchUpScan = subscriptionLog.ScanFrom(cmd.FromPosition, resolver);
            cmd.Reply.TrySetResult(new SubscriptionRegistration(liveChannel, catchUpScan, catchUpPosition, resolverSnapshot));
        } catch (Exception ex) {
            resolverSnapshot?.Dispose();
            cmd.Reply.TrySetException(ex);
        }
        return ValueTask.CompletedTask;
    }

    ValueTask HandleUnregisterSubscription(UnregisterSubscriptionCommand cmd) {
        _subscriptionBus.Unregister(cmd.Channel);
        return ValueTask.CompletedTask;
    }

    static SubscriptionEvent ResolveFromSnapshot(LsmStoreSnapshot<EventKey> snapshot, EventKey key) {
        foreach (var (k, v) in snapshot.ScanFrom(key)) {
            if (!k.Equals(key)) break;
            if (v is null) break;
            return ((EventValue)v.Value).ToSubscriptionEvent(k);
        }
        throw new InvalidOperationException($"Event {key.StreamId}@{key.Revision} not found in store.");
    }
}
