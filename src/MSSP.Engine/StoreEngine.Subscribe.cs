using System.Threading.Channels;

namespace MSSP.Engine;

sealed partial class StoreEngine {
    ISubscriptionProvider Subscriptions => (ISubscriptionProvider)pipeline;

    /// <summary>
    /// Schedules subscription registration on the actor thread.
    /// Returns a <see cref="SubscriptionRegistration"/> with the live channel, catch-up scan,
    /// and the position watermark at which catch-up ends and live begins.
    /// </summary>
    public ValueTask<SubscriptionRegistration> RegisterSubscriptionAsync(SubscriptionFilter filter, GlobalPosition fromPosition, Func<EventKey, SubscriptionEvent>? resolver, CancellationToken cancellationToken) {
        var tcs = new TaskCompletionSource<SubscriptionRegistration>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_mailbox.Writer.TryWrite(new RegisterSubscriptionCommand(filter, fromPosition, resolver, tcs)))
            tcs.TrySetException(new ObjectDisposedException(nameof(StoreEngine)));
        return new ValueTask<SubscriptionRegistration>(tcs.Task.WaitAsync(cancellationToken));
    }

    /// <summary>
    /// Posts an unregister command to the actor mailbox. Fire-and-forget: does not wait for the channel to close.
    /// </summary>
    public void UnregisterSubscription(ChannelReader<SubscriptionEvent> channel) =>
        _mailbox.Writer.TryWrite(new UnregisterSubscriptionCommand(channel));

    ValueTask HandleRegisterSubscription(RegisterSubscriptionCommand cmd) {
        try {
            var catchUpPosition = Subscriptions.CurrentPosition;
            var liveChannel = Subscriptions.Register(cmd.Filter);
            var catchUpScan = Subscriptions.ScanFrom(cmd.FromPosition, cmd.Resolver);
            cmd.Reply.TrySetResult(new SubscriptionRegistration(liveChannel, catchUpScan, catchUpPosition));
        } catch (Exception ex) {
            cmd.Reply.TrySetException(ex);
        }
        return ValueTask.CompletedTask;
    }

    ValueTask HandleUnregisterSubscription(UnregisterSubscriptionCommand cmd) {
        Subscriptions.Unregister(cmd.Channel);
        return ValueTask.CompletedTask;
    }
}
