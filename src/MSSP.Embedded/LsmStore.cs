using System.Buffers.Binary;
using MSSP.Log;
using MSSP.LsmTree;

namespace MSSP.Embedded;

sealed class LsmStore : IDisposable {
    // Matches MemTable<TKey>.WriteMarker — both define the on-disk WAL record format.
    const byte WalWriteMarker = 0x01;

    readonly string _dataDirectory;
    readonly int _capacityBytes;
    StreamSegment<WalRecord> _wal;
    readonly List<string> _sstFiles;
    MemTable<EventKey> _memTable;

    LsmStore(string dataDirectory, int capacityBytes, StreamSegment<WalRecord> wal, MemTable<EventKey> memTable, List<string> sstFiles) {
        _dataDirectory = dataDirectory;
        _capacityBytes = capacityBytes;
        _wal = wal;
        _memTable = memTable;
        _sstFiles = sstFiles;
    }

    internal static async ValueTask<LsmStore> OpenAsync(string dataDirectory, int capacityBytes, CancellationToken ct) {
        Directory.CreateDirectory(dataDirectory);

        var sstFiles = Directory.EnumerateFiles(dataDirectory, "*.sst").OrderBy(f => f).ToList();
        var sstRevisions = BuildSstRevisions(sstFiles);

        var walStream = new FileStream(
            WalPath(dataDirectory),
            FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None,
            bufferSize: 4096, FileOptions.WriteThrough | FileOptions.Asynchronous);
        var wal = new StreamSegment<WalRecord>(walStream);
        var memTable = new MemTable<EventKey>(capacityBytes, WalAppendDelegate);

        await ReplayWalAsync(wal, sstRevisions, memTable, ct);

        return new LsmStore(dataDirectory, capacityBytes, wal, memTable, sstFiles);

        ValueTask<bool> WalAppendDelegate(ReadOnlyMemory<byte> record, CancellationToken cancelToken) =>
            wal.TryAppendAsync(record, cancelToken);
    }

    internal async ValueTask WriteAsync(EventKey key, ReadOnlyMemory<byte> value, CancellationToken ct) {
        ReadOnlyMemory<byte> keyBytes = key;
        var entrySize = keyBytes.Length + value.Length;

        if (entrySize > _capacityBytes)
            throw new InvalidOperationException("Single event exceeds MemTable capacity.");

        if (_memTable.Size + entrySize > _capacityBytes)
            await FlushAsync(ct);

        if (!await _memTable.TryWriteAsync(key, value, ct))
            throw new InvalidOperationException("WAL append failed.");
    }

    internal (bool exists, ulong revision) LookupCurrentRevision(string streamId) {
        ulong? max = null;

        foreach (var (key, value) in _memTable.ScanFrom(new EventKey(streamId, 0UL))) {
            if (key.StreamId != streamId) break;
            if (value is not null) max = key.Revision;
        }

        var startKey = new EventKey(streamId, 0UL);
        foreach (var sstPath in _sstFiles) {
            using var stream = new FileStream(sstPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096);
            foreach (var (key, _) in new SstReader<EventKey>(stream).Scan(startKey)) {
                if (key.StreamId != streamId) break;
                max = max.HasValue ? Math.Max(max.Value, key.Revision) : key.Revision;
            }
        }

        return (max.HasValue, max ?? 0UL);
    }

    internal (string[] SstFiles, MemTable<EventKey> MemTable) TakeSnapshot() =>
        ([.. _sstFiles], _memTable);

    async ValueTask FlushAsync(CancellationToken ct) {
        var sstPath = Path.Combine(_dataDirectory, $"{DateTimeOffset.UtcNow.Ticks:D19}.sst");
        await using var sstStream = new FileStream(sstPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous);
        await SstWriter.WriteAsync<EventKey>(_memTable, sstStream, cancellationToken: ct);
        _sstFiles.Add(sstPath);

        _wal.Dispose();
        var walStream = new FileStream(
            WalPath(_dataDirectory),
            FileMode.Create, FileAccess.ReadWrite, FileShare.None,
            bufferSize: 4096, FileOptions.WriteThrough | FileOptions.Asynchronous);
        _wal = new StreamSegment<WalRecord>(walStream);
        _memTable = new MemTable<EventKey>(_capacityBytes, (record, ct2) => _wal.TryAppendAsync(record, ct2));
    }

    static string WalPath(string dataDirectory) => Path.Combine(dataDirectory, "wal.log");

    static Dictionary<string, ulong> BuildSstRevisions(IEnumerable<string> sstFiles) {
        var revisions = new Dictionary<string, ulong>();
        foreach (var sstPath in sstFiles) {
            using var stream = new FileStream(sstPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096);
            foreach (var (key, _) in new SstReader<EventKey>(stream).Scan()) {
                if (!revisions.TryGetValue(key.StreamId, out var rev) || key.Revision > rev)
                    revisions[key.StreamId] = key.Revision;
            }
        }
        return revisions;
    }

    static async ValueTask ReplayWalAsync(StreamSegment<WalRecord> wal, Dictionary<string, ulong> sstRevisions, MemTable<EventKey> memTable, CancellationToken ct) {
        await foreach (var record in wal.WithCancellation(ct)) {
            ReadOnlyMemory<byte> bytes = record;
            var span = bytes.Span;
            if (span.Length < 5 || span[0] != WalWriteMarker) continue;

            EventKey key = bytes.Slice(5, BinaryPrimitives.ReadInt32LittleEndian(span[1..]));

            if (!sstRevisions.TryGetValue(key.StreamId, out var sstMax) || key.Revision > sstMax)
                memTable.ApplyRecord(bytes);
        }
    }

    public void Dispose() {
        _memTable.Dispose();
        _wal.Dispose();
    }
}
