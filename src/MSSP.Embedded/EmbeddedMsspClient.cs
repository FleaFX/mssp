using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;
using MSSP.Log;
using MSSP.LsmTree;

namespace MSSP.Embedded;

/// <summary>
/// An embedded, single-process implementation of <see cref="IMsspClient"/> that stores events on the local filesystem.
/// </summary>
public sealed class EmbeddedMsspClient : IMsspClient, IDisposable {
    readonly StreamSegment<WalRecord> _wal;
    readonly MemTable<EventKey> _memTable;
    readonly SemaphoreSlim _writeLock = new(1, 1);
    readonly Dictionary<string, ulong> _streamRevisions = new();

    EmbeddedMsspClient(StreamSegment<WalRecord> wal, MemTable<EventKey> memTable) {
        _wal = wal;
        _memTable = memTable;
    }

    /// <summary>
    /// Opens or creates an embedded event store at the given <paramref name="dataDirectory"/>.
    /// </summary>
    /// <param name="dataDirectory">The directory in which to store WAL and SST files.</param>
    /// <param name="memTableCapacityBytes">The maximum size of the in-memory write buffer before it must be flushed to disk.</param>
    /// <returns>An <see cref="EmbeddedMsspClient"/> ready for use.</returns>
    public static EmbeddedMsspClient Open(string dataDirectory, int memTableCapacityBytes = 64 * 1024 * 1024) {
        Directory.CreateDirectory(dataDirectory);
        var walStream = new FileStream(
            Path.Combine(dataDirectory, "wal.log"),
            FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None,
            bufferSize: 4096, FileOptions.WriteThrough | FileOptions.Asynchronous);
        var wal = new StreamSegment<WalRecord>(walStream);
        WalAppendDelegate walDelegate = (record, ct) => wal.TryAppendAsync(record, ct);
        return new EmbeddedMsspClient(wal, new MemTable<EventKey>(memTableCapacityBytes, walDelegate));
    }

    /// <inheritdoc/>
    public async ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken ct = default) {
        await _writeLock.WaitAsync(ct);
        try {
            if (!CheckConcurrency(streamId.Value, expectedRevision))
                throw new OptimisticConcurrencyException(streamId, expectedRevision);

            if (_memTable.IsFull)
                throw new InvalidOperationException("MemTable is full; flush to SST before appending. (Not yet implemented.)");

            var nextRevision = _streamRevisions.TryGetValue(streamId.Value, out var current) ? current + 1 : 0UL;
            var timestamp = DateTimeOffset.UtcNow;

            foreach (var @event in events) {
                var key = new EventKey(streamId.Value, nextRevision);
                var value = SerializeValue(@event, timestamp);
                if (!await _memTable.TryWriteAsync(key, value, ct))
                    throw new InvalidOperationException("WAL append failed.");
                _streamRevisions[streamId.Value] = nextRevision++;
            }
        } finally {
            _writeLock.Release();
        }
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<RecordedEvent> ReadAsync(StreamId streamId, StreamRevision from = default, [EnumeratorCancellation] CancellationToken ct = default) {
        // TODO: merge with SST layers once flush is implemented
        foreach (var (key, value) in _memTable) {
            if (ct.IsCancellationRequested) yield break;
            if (key.StreamId != streamId.Value) continue;
            if (key.Revision < from) continue;
            if (value is null) continue; // tombstone
            yield return DeserializeValue(key, value.Value);
        }
    }

    bool CheckConcurrency(string streamId, StreamRevision expected) {
        var exists = _streamRevisions.TryGetValue(streamId, out var current);
        if (expected == StreamRevision.Any) return true;
        if (expected == StreamRevision.NoStream) return !exists;
        if (expected == StreamRevision.StreamExists) return exists;
        return exists && current == expected;
    }

    static ReadOnlyMemory<byte> SerializeValue(EventData @event, DateTimeOffset timestamp) {
        var typeBytes = Encoding.UTF8.GetBytes(@event.EventType);
        var buffer = new byte[4 + typeBytes.Length + 8 + @event.Data.Length];
        var span = buffer.AsSpan();
        BinaryPrimitives.WriteInt32LittleEndian(span, typeBytes.Length);
        typeBytes.CopyTo(span[4..]);
        BinaryPrimitives.WriteInt64LittleEndian(span[(4 + typeBytes.Length)..], timestamp.ToUnixTimeMilliseconds());
        @event.Data.Span.CopyTo(span[(4 + typeBytes.Length + 8)..]);
        return buffer;
    }

    static RecordedEvent DeserializeValue(EventKey key, ReadOnlyMemory<byte> value) {
        var span = value.Span;
        var typeLen = BinaryPrimitives.ReadInt32LittleEndian(span);
        var eventType = Encoding.UTF8.GetString(span.Slice(4, typeLen));
        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(span[(4 + typeLen)..]));
        var data = value[(4 + typeLen + 8)..];
        return new RecordedEvent(key.StreamId, key.Revision, eventType, data, timestamp);
    }

    /// <inheritdoc/>
    public void Dispose() {
        _writeLock.Dispose();
        _memTable.Dispose();
        _wal.Dispose();
    }
}
