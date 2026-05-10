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
    // Matches MemTable<TKey>.WriteMarker — both define the on-disk WAL record format.
    const byte WalWriteMarker = 0x01;

    readonly string _dataDirectory;
    readonly int _memTableCapacityBytes;
    StreamSegment<WalRecord> _wal;
    readonly SemaphoreSlim _writeLock = new(1, 1);
    readonly Dictionary<string, ulong> _streamRevisions;
    readonly List<string> _sstFiles;
    MemTable<EventKey> _memTable;

    EmbeddedMsspClient(string dataDirectory, int memTableCapacityBytes, StreamSegment<WalRecord> wal, MemTable<EventKey> memTable, List<string> sstFiles, Dictionary<string, ulong> streamRevisions) {
        _dataDirectory = dataDirectory;
        _memTableCapacityBytes = memTableCapacityBytes;
        _wal = wal;
        _memTable = memTable;
        _sstFiles = sstFiles;
        _streamRevisions = streamRevisions;
    }

    /// <summary>
    /// Opens or creates an embedded event store at the given <paramref name="dataDirectory"/>,
    /// recovering any unflushed writes from the WAL.
    /// </summary>
    /// <param name="dataDirectory">The directory in which to store WAL and SST files.</param>
    /// <param name="memTableCapacityBytes">The maximum size of the in-memory write buffer before it is flushed to an SST file.</param>
    /// <param name="ct">Token to cancel the open operation.</param>
    /// <returns>An <see cref="EmbeddedMsspClient"/> ready for use.</returns>
    public static async ValueTask<EmbeddedMsspClient> OpenAsync(string dataDirectory, int memTableCapacityBytes = 64 * 1024 * 1024, CancellationToken ct = default) {
        Directory.CreateDirectory(dataDirectory);

        var walStream = new FileStream(
            Path.Combine(dataDirectory, "wal.log"),
            FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None,
            bufferSize: 4096, FileOptions.WriteThrough | FileOptions.Asynchronous);
        var wal = new StreamSegment<WalRecord>(walStream);

        var sstFiles = Directory
            .EnumerateFiles(dataDirectory, "*.sst")
            .OrderBy(f => f)
            .ToList();

        var streamRevisions = new Dictionary<string, ulong>();
        var sstMaxRevisions = BuildSstRevisions(sstFiles);
        foreach (var (streamId, revision) in sstMaxRevisions)
            streamRevisions[streamId] = revision;

        WalAppendDelegate walDelegate = (record, cancelToken) => wal.TryAppendAsync(record, cancelToken);
        var memTable = new MemTable<EventKey>(memTableCapacityBytes, walDelegate);

        await foreach (var record in wal.WithCancellation(ct)) {
            ReadOnlyMemory<byte> bytes = record;
            var span = bytes.Span;
            if (span.Length < 5 || span[0] != WalWriteMarker) continue;

            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(span[1..]);
            EventKey key = bytes.Slice(5, keyLen);

            if (!streamRevisions.TryGetValue(key.StreamId, out var current) || key.Revision > current)
                streamRevisions[key.StreamId] = key.Revision;

            if (!sstMaxRevisions.TryGetValue(key.StreamId, out var sstMax) || key.Revision > sstMax)
                memTable.ApplyRecord(bytes);
        }

        return new EmbeddedMsspClient(dataDirectory, memTableCapacityBytes, wal, memTable, sstFiles, streamRevisions);
    }

    /// <inheritdoc/>
    public async ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken ct = default) {
        await _writeLock.WaitAsync(ct);
        try {
            if (!CheckConcurrency(streamId.Value, expectedRevision))
                throw new OptimisticConcurrencyException(streamId, expectedRevision);

            var baseRevision = _streamRevisions.TryGetValue(streamId.Value, out var current) ? current + 1 : 0UL;
            var timestamp = DateTimeOffset.UtcNow;

            var serialized = Serialize(streamId.Value, baseRevision, timestamp, events);
            if (serialized.Count == 0) return;

            foreach (var (key, value) in serialized) {
                ReadOnlyMemory<byte> keyBytes = key;
                var entrySize = keyBytes.Length + value.Length;

                if (entrySize > _memTableCapacityBytes)
                    throw new InvalidOperationException("Single event exceeds MemTable capacity.");

                if (_memTable.Size + entrySize > _memTableCapacityBytes)
                    await FlushAsync(ct);

                if (!await _memTable.TryWriteAsync(key, value, ct))
                    throw new InvalidOperationException("WAL append failed.");
                _streamRevisions[streamId.Value] = key.Revision;
            }
        } finally {
            _writeLock.Release();
        }
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<RecordedEvent> ReadAsync(StreamId streamId, StreamRevision from = default, [EnumeratorCancellation] CancellationToken ct = default) {
        string[] sstSnapshot;
        MemTable<EventKey> memTableSnapshot;

        await _writeLock.WaitAsync(ct);
        try {
            sstSnapshot = [.. _sstFiles];
            memTableSnapshot = _memTable;
        } finally {
            _writeLock.Release();
        }

        foreach (var sstPath in sstSnapshot) {
            if (ct.IsCancellationRequested) yield break;
            using var stream = new FileStream(sstPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);
            var reader = new SstReader<EventKey>(stream);
            foreach (var (key, value) in reader.Scan()) {
                if (key.StreamId != streamId.Value) continue;
                if (key.Revision < from) continue;
                if (value is null) continue;
                yield return DeserializeValue(key, value.Value);
            }
        }

        foreach (var (key, value) in memTableSnapshot) {
            if (ct.IsCancellationRequested) yield break;
            if (key.StreamId != streamId.Value) continue;
            if (key.Revision < from) continue;
            if (value is null) continue;
            yield return DeserializeValue(key, value.Value);
        }
    }

    async ValueTask FlushAsync(CancellationToken ct) {
        var sstPath = Path.Combine(_dataDirectory, $"{DateTimeOffset.UtcNow.Ticks:D19}.sst");
        await using var stream = new FileStream(sstPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous);
        await SstWriter.WriteAsync<EventKey>(_memTable, stream, cancellationToken: ct);
        _sstFiles.Add(sstPath);

        _wal.Dispose();
        var walStream = new FileStream(
            Path.Combine(_dataDirectory, "wal.log"),
            FileMode.Create, FileAccess.ReadWrite, FileShare.None,
            bufferSize: 4096, FileOptions.WriteThrough | FileOptions.Asynchronous);
        _wal = new StreamSegment<WalRecord>(walStream);

        WalAppendDelegate walDelegate = (record, ct2) => _wal.TryAppendAsync(record, ct2);
        _memTable = new MemTable<EventKey>(_memTableCapacityBytes, walDelegate);
    }

    bool CheckConcurrency(string streamId, StreamRevision expected) {
        var exists = _streamRevisions.TryGetValue(streamId, out var current);
        if (expected == StreamRevision.Any) return true;
        if (expected == StreamRevision.NoStream) return !exists;
        if (expected == StreamRevision.StreamExists) return exists;
        return exists && current == expected;
    }

    static Dictionary<string, ulong> BuildSstRevisions(IEnumerable<string> sstFiles) {
        var revisions = new Dictionary<string, ulong>();
        foreach (var sstPath in sstFiles) {
            using var stream = new FileStream(sstPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096);
            var reader = new SstReader<EventKey>(stream);
            foreach (var (key, _) in reader.Scan()) {
                if (!revisions.TryGetValue(key.StreamId, out var rev) || key.Revision > rev)
                    revisions[key.StreamId] = key.Revision;
            }
        }
        return revisions;
    }

    static List<(EventKey Key, ReadOnlyMemory<byte> Value)> Serialize(string streamId, ulong baseRevision, DateTimeOffset timestamp, IEnumerable<EventData> events) {
        var result = new List<(EventKey, ReadOnlyMemory<byte>)>();
        var offset = 0UL;
        foreach (var @event in events) {
            result.Add((new EventKey(streamId, baseRevision + offset), SerializeValue(@event, timestamp)));
            offset++;
        }
        return result;
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
