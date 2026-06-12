using System.Buffers.Binary;
using MSSP.Engine.Storage;

namespace MSSP.Engine;

sealed partial class StoreEngine {
    async ValueTask HandleAppendAsync(AppendCommand cmd, CancellationToken ct) {
        if (!_revisions.Contains(cmd.StreamId.Value)) {
            var (exists, rev) = LookupCurrentRevision(cmd.StreamId.Value);
            if (exists) _revisions.Set(cmd.StreamId.Value, rev);
        }

        if (!_revisions.CheckConcurrency(cmd.StreamId.Value, cmd.ExpectedRevision)) {
            cmd.Reply.TrySetException(new OptimisticConcurrencyException(cmd.StreamId, cmd.ExpectedRevision));
            return;
        }

        var baseRevision = _revisions.TryGet(cmd.StreamId.Value, out var current) ? current + 1 : 0UL;
        var offset = 0UL;
        var lastPosition = 0UL;

        try {
            foreach (var eventData in cmd.Events) {
                var key = new EventKey(cmd.StreamId.Value, baseRevision + offset++);
                var value = EventValue.From(eventData, cmd.Timestamp);
                lastPosition = (ulong)(++_nextPosition);
                BinaryPrimitives.WriteUInt64LittleEndian(value.Span[^8..], lastPosition);
                if (!await log.TryAppendAsync(WalRecord.From(key, value), ct))
                    throw new InvalidOperationException("WAL append failed.");
                _revisions.Set(cmd.StreamId.Value, key.Revision);
            }
        } catch (Exception ex) {
            _revisions.Remove(cmd.StreamId.Value);
            cmd.Reply.TrySetException(ex);
            return;
        }

        if (offset == 0) {
            cmd.Reply.TrySetResult(true);
            return;
        }

        _pending.Enqueue(new PendingAppend(lastPosition, cmd.Reply));
    }

    (bool exists, ulong revision) LookupCurrentRevision(string streamId) {
        ulong? max = null;
        var startKey = new EventKey(streamId, 0UL);
        foreach (var (key, _) in store.ScanAllFrom(startKey)) {
            if (key.StreamId != streamId) break;
            max = key.Revision;
        }
        return (max.HasValue, max ?? 0UL);
    }
}
