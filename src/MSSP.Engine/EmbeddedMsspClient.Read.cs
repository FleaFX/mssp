using System.Runtime.CompilerServices;

namespace MSSP.Engine;

public sealed partial class EmbeddedMsspClient {
    /// <inheritdoc/>
    public async IAsyncEnumerable<RecordedEvent> ReadAsync(StreamId streamId, StreamRevision from = default, ReadDirection direction = ReadDirection.Forwards, long maxCount = long.MaxValue, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        // For forwards reads, seed the scan at the requested revision so the SST sparse index
        // seeks directly to the right block instead of scanning from revision 0.
        var startKey = new EventKey(streamId.Value, direction == ReadDirection.Forwards ? (ulong)from : 0UL);

        using var snapshot = await _engine!.CaptureSnapshotAsync(cancellationToken);
        var events = direction == ReadDirection.Forwards
            ? ReadForwards(snapshot.ScanFrom(startKey), streamId.Value, maxCount, cancellationToken)
            : ReadBackwards(snapshot.ScanFrom(startKey), streamId.Value, from, maxCount, cancellationToken);
        foreach (var evt in events) {
            _metrics?.RecordRead(1);
            yield return evt;
        }
    }

    static IEnumerable<RecordedEvent> ReadForwards(IEnumerable<KeyValuePair<EventKey, ReadOnlyMemory<byte>?>> scan, string streamId, long maxCount, CancellationToken cancellationToken) {
        var count = 0L;
        foreach (var (key, value) in scan) {
            cancellationToken.ThrowIfCancellationRequested();
            if (key.StreamId != streamId) break;
            if (value is null) continue;
            if (count++ >= maxCount) yield break;
            yield return ((EventValue)value.Value).ToRecordedEvent(key);
        }
    }

    static IEnumerable<RecordedEvent> ReadBackwards(IEnumerable<KeyValuePair<EventKey, ReadOnlyMemory<byte>?>> scan, string streamId, StreamRevision from, long maxCount, CancellationToken cancellationToken) {
        var hasUpperBound = from != default;

        if (maxCount < long.MaxValue) {
            // Sliding window: O(maxCount) memory instead of O(stream length).
            var window = new Queue<RecordedEvent>((int)Math.Min(maxCount, 4096));
            foreach (var (key, value) in scan) {
                cancellationToken.ThrowIfCancellationRequested();
                if (key.StreamId != streamId) break;
                if (hasUpperBound && key.Revision > (ulong)from) break;
                if (value is null) continue;
                window.Enqueue(((EventValue)value.Value).ToRecordedEvent(key));
                if (window.Count > maxCount) window.Dequeue();
            }
            var arr = window.ToArray();
            Array.Reverse(arr);
            foreach (var evt in arr)
                yield return evt;
        } else {
            // Unbounded: materialize to reverse; stop early if from is explicit.
            var allEvents = new List<RecordedEvent>();
            foreach (var (key, value) in scan) {
                cancellationToken.ThrowIfCancellationRequested();
                if (key.StreamId != streamId) break;
                if (hasUpperBound && key.Revision > (ulong)from) break;
                if (value is null) continue;
                allEvents.Add(((EventValue)value.Value).ToRecordedEvent(key));
            }
            allEvents.Reverse();
            foreach (var evt in allEvents)
                yield return evt;
        }
    }
}
