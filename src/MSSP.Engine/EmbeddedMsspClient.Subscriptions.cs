using System.Runtime.CompilerServices;

namespace MSSP.Engine;

public sealed partial class EmbeddedMsspClient {
    /// <inheritdoc/>
    public async IAsyncEnumerable<SubscriptionEvent> SubscribeAsync(
        SubscriptionFilter filter,
        GlobalPosition fromPosition = default,
        [EnumeratorCancellation] CancellationToken cancellationToken = default) {

        _metrics?.SubscriptionStarted();
        var reg = await _engine!.RegisterSubscriptionAsync(filter, fromPosition, cancellationToken);

        try {
            foreach (var evt in reg.CatchUpScan) {
                if (cancellationToken.IsCancellationRequested) yield break;
                if (evt.Position > reg.CatchUpPosition) break;
                if (filter.Matches(evt)) yield return evt;
            }

            await foreach (var evt in reg.LiveChannel.ReadAllAsync(cancellationToken)) {
                if (evt.Position <= reg.CatchUpPosition) continue;
                yield return evt;
            }
        } finally {
            reg.ResolverSnapshot?.Dispose();
            _engine.UnregisterSubscription(reg.LiveChannel);
            _metrics?.SubscriptionStopped();
        }
    }
}
