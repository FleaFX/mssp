using System.Buffers.Binary;

namespace MSSP.Engine;

sealed partial class StoreEngine {
    async ValueTask HandleCommittedBatchAsync(CommittedBatch batch, CancellationToken cancellationToken) {
        var toResolve = new List<TaskCompletionSource<bool>>();

        foreach (var record in batch.Records) {
            ReadOnlyMemory<byte> bytes = record;
            var span = bytes.Span;
            if (span.Length < 5) continue;

            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(span[1..]);
            if (keyLen < 0 || 5 + keyLen > span.Length) continue;

            EventKey key = bytes.Slice(5, keyLen);
            Memory<byte> value = bytes[(5 + keyLen)..].ToArray();

            var pos = value.Length >= 8
                ? BinaryPrimitives.ReadUInt64LittleEndian(value.Span[^8..])
                : 0UL;

            if (await store.TryBeginFlushAsync(keyLen + value.Length, cancellationToken) is { } flushJob) {
                _flush!.Enqueue(flushJob);
            }
            await store.WriteAsync(key, value, cancellationToken);

            if (pos > _currentPosition) {
                _currentPosition = pos;
                await subscriptionLog.AppendAsync(new GlobalPosition(pos), key, value, cancellationToken);
                _subscriptionBus.Publish(((EventValue)value).ToSubscriptionEvent(key));
            }

            if (_pending.TryPeek(out var entry) && entry.LastPosition == pos) {
                _pending.Dequeue();
                toResolve.Add(entry.Reply);
            }
        }

        await subscriptionLog.FlushAsync(cancellationToken);

        foreach (var tcs in toResolve)
            tcs.SetResult(true);
    }

    async ValueTask HandleFlushCompletedAsync(FlushCompleted msg, CancellationToken cancellationToken) {
        if (msg.Error is OperationCanceledException) return;
        if (msg.Error is not null) throw msg.Error;
        await msg.Job.CompleteAsync(cancellationToken);
        TryFulfillPendingPlanRequest();
    }
}
