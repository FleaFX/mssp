using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace MSSP.Engine;

public sealed partial class EmbeddedMsspClient {
    /// <inheritdoc/>
    public async IAsyncEnumerable<SubscriptionEvent> SubscribeAsync(
        SubscriptionFilter filter,
        GlobalPosition fromPosition = default,
        [EnumeratorCancellation] CancellationToken cancellationToken = default) {

        ChannelReader<SubscriptionEvent> liveChannel;
        IEnumerable<SubscriptionEvent> catchUpScan;
        GlobalPosition catchUpPosition;

        _metrics?.SubscriptionStarted();
        if (_engine is { } engine) {
            var reg = await engine.RegisterSubscriptionAsync(filter, fromPosition, BuildResolver(), cancellationToken);
            liveChannel = reg.LiveChannel;
            catchUpScan = reg.CatchUpScan;
            catchUpPosition = reg.CatchUpPosition;
        } else {
            await _writeLock.WaitAsync(cancellationToken);
            try {
                catchUpPosition = subscriptions.CurrentPosition;
                liveChannel = subscriptions.Register(filter);
                catchUpScan = subscriptions.ScanFrom(fromPosition, BuildResolver());
            } finally {
                _writeLock.Release();
            }
        }

        try {
            // CATCH-UP: replay historical events from the subscription log.
            // The log is ordered by GlobalPosition, so we can break on first entry past the snapshot.
            foreach (var evt in catchUpScan) {
                if (cancellationToken.IsCancellationRequested) yield break;
                if (evt.Position > catchUpPosition) break;
                if (filter.Matches(evt)) yield return evt;
            }

            // LIVE: deliver events written after the catch-up snapshot.
            // The overlap guard skips any events already delivered in catch-up.
            await foreach (var evt in liveChannel.ReadAllAsync(cancellationToken)) {
                if (evt.Position <= catchUpPosition) continue;
                yield return evt;
            }
        } finally {
            if (_engine is { } eng) {
                eng.UnregisterSubscription(liveChannel);
            } else {
                await _writeLock.WaitAsync(CancellationToken.None);
                try {
                    subscriptions.Unregister(liveChannel);
                } finally {
                    _writeLock.Release();
                }
            }
            _metrics?.SubscriptionStopped();
        }
    }

    // For FullPayload format the log contains full event data; no resolver needed.
    // For ReferenceOnly the log stores only EventKey pointers, resolved here via SST scan.
    Func<EventKey, SubscriptionEvent>? BuildResolver() {
        if (subscriptions.LogFormat == SubscriptionLogFormat.FullPayload) return null;
        return key => {
            foreach (var (k, v) in store.ScanSnapshotFrom(key)) {
                if (!k.Equals(key)) break;
                if (v is null) break;
                return ((EventValue)v.Value).ToSubscriptionEvent(k);
            }
            throw new InvalidOperationException($"Event {key.StreamId}@{key.Revision} not found in store.");
        };
    }
}
