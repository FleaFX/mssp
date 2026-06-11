using System.Buffers.Binary;

namespace MSSP.Engine;

sealed partial class StoreEngine {
    async ValueTask HandleCommittedBatchAsync(CommittedBatch batch, CancellationToken ct) {
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

            await pipeline.WriteAsync(key, value, ct);

            if (_pending.TryPeek(out var entry) && entry.LastPosition == pos) {
                _pending.Dequeue();
                toResolve.Add(entry.Reply);
            }
        }

        await pipeline.FlushAsync(ct);

        foreach (var tcs in toResolve)
            tcs.SetResult(true);
    }
}
