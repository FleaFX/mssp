using System.Buffers;
using System.Buffers.Binary;

namespace MSSP.Embedded;

/// <summary>
/// Persistent, append-only log of all events in global-position order.
/// Used to replay historical events during catch-up subscriptions.
/// </summary>
/// <remarks>
/// The log is split into fixed-size segments named by their starting GlobalPosition
/// (e.g. <c>subscriptions-00000000000000000001.log</c>). Sealed segments are never
/// modified; only the active (last) segment is written to.
///
/// Entry format — FullPayload:
///   [globalPosition: 8 LE][keyLen: 4 LE][key: keyLen bytes][valueLen: 4 LE][value: valueLen bytes]
/// Entry format — ReferenceOnly:
///   [globalPosition: 8 LE][keyLen: 4 LE][key: keyLen bytes]
/// </remarks>
internal sealed class SubscriptionLog : IDisposable {
    const int SparseEvery = 128;
    const string FilePrefix = "subscriptions-";
    const string FileSuffix = ".log";

    readonly string _dataDirectory;
    readonly SubscriptionLogFormat _format;
    readonly long _segmentSizeBytes;

    // Sealed (read-only) segments, sorted by start position.
    readonly List<SegmentMeta> _sealed = [];

    // Active (writable) segment state.
    string _activePath = string.Empty;
    GlobalPosition _activeStart;
    GlobalPosition _activeEnd;
    readonly List<(GlobalPosition Position, long ByteOffset)> _activeSparseIndex = [];
    int _activeEntryCount;
    FileStream? _activeStream;

    internal SubscriptionLogFormat Format => _format;

    SubscriptionLog(string dataDirectory, SubscriptionLogFormat format, long segmentSizeBytes) {
        _dataDirectory = dataDirectory;
        _format = format;
        _segmentSizeBytes = segmentSizeBytes;
    }

    /// <summary>Opens or creates the subscription log in <paramref name="dataDirectory"/>.</summary>
    internal static SubscriptionLog Open(string dataDirectory, SubscriptionLogFormat format, long segmentSizeBytes) {
        var log = new SubscriptionLog(dataDirectory, format, segmentSizeBytes);
        log.LoadExistingSegments();
        return log;
    }

    void LoadExistingSegments() {
        var files = Directory.GetFiles(_dataDirectory, FilePrefix + "*" + FileSuffix)
            .OrderBy(f => f)
            .ToArray();

        for (int i = 0; i < files.Length; i++) {
            var path = files[i];
            var startPos = ParseStartPosition(path);
            var isLast = i == files.Length - 1;
            var (end, sparseIndex, entryCount) = ScanForIndex(path);

            if (!isLast) {
                _sealed.Add(new SegmentMeta(path, new GlobalPosition(startPos), end, sparseIndex));
            } else {
                _activeStart = new GlobalPosition(startPos);
                _activeEnd = end;
                _activeSparseIndex.AddRange(sparseIndex);
                _activeEntryCount = entryCount;
                _activePath = path;
                _activeStream = OpenForAppend(path);
            }
        }
        // If no existing files, _activeStream stays null — created lazily on first append.
    }

    /// <summary>
    /// Returns the position of the last written event, or <see cref="GlobalPosition.Start"/> if empty.
    /// Must be called under the write lock.
    /// </summary>
    internal GlobalPosition GetLastPosition() {
        if (_activeStream != null) return _activeEnd;
        if (_sealed.Count > 0) return _sealed[^1].End;
        return GlobalPosition.Start;
    }

    /// <summary>
    /// Appends an event to the log. Must be called under the write lock.
    /// </summary>
    internal async ValueTask AppendAsync(GlobalPosition position, EventKey key, ReadOnlyMemory<byte> value, CancellationToken ct) {
        if (_activeStream == null) {
            OpenNewSegment(position);
        } else if (_activeStream.Length >= _segmentSizeBytes) {
            RotateSegment(position);
        }

        ReadOnlyMemory<byte> keyBytes = key;
        int entrySize = _format == SubscriptionLogFormat.FullPayload
            ? 8 + 4 + keyBytes.Length + 4 + value.Length
            : 8 + 4 + keyBytes.Length;

        var buf = ArrayPool<byte>.Shared.Rent(entrySize);
        try {
            var span = buf.AsSpan();
            BinaryPrimitives.WriteUInt64LittleEndian(span, position.Value);
            BinaryPrimitives.WriteInt32LittleEndian(span[8..], keyBytes.Length);
            keyBytes.Span.CopyTo(span[12..]);

            if (_format == SubscriptionLogFormat.FullPayload) {
                int valueOffset = 12 + keyBytes.Length;
                BinaryPrimitives.WriteInt32LittleEndian(span[valueOffset..], value.Length);
                value.Span.CopyTo(span[(valueOffset + 4)..]);
            }

            if (_activeEntryCount % SparseEvery == 0)
                _activeSparseIndex.Add((position, _activeStream!.Position));

            await _activeStream!.WriteAsync(buf.AsMemory(0, entrySize), ct);
            await _activeStream.FlushAsync(ct);
        } finally {
            ArrayPool<byte>.Shared.Return(buf);
        }

        _activeEnd = position;
        _activeEntryCount++;
    }

    /// <summary>
    /// Returns an <see cref="IEnumerable{T}"/> that can be safely iterated outside the write lock.
    /// Must be called while holding the write lock to capture a consistent snapshot.
    /// </summary>
    internal IEnumerable<SubscriptionEvent> ScanFrom(
        GlobalPosition from,
        Func<EventKey, SubscriptionEvent>? resolver = null) {

        var sealedSnapshot = _sealed.ToArray();
        (string Path, GlobalPosition Start, GlobalPosition End, (GlobalPosition Position, long ByteOffset)[] SparseIndex)? activeSnapshot =
            _activeStream != null
                ? (_activePath, _activeStart, _activeEnd, _activeSparseIndex.ToArray())
                : null;

        return IterateAll(from, sealedSnapshot, activeSnapshot, resolver);
    }

    IEnumerable<SubscriptionEvent> IterateAll(
        GlobalPosition from,
        SegmentMeta[] sealedSegs,
        (string Path, GlobalPosition Start, GlobalPosition End, (GlobalPosition Position, long ByteOffset)[] SparseIndex)? active,
        Func<EventKey, SubscriptionEvent>? resolver) {

        foreach (var seg in sealedSegs) {
            if (seg.End < from) continue;
            foreach (var evt in ReadSegmentFrom(seg.Path, from, seg.SparseIndex, resolver))
                yield return evt;
        }

        if (active.HasValue && active.Value.End >= from) {
            foreach (var evt in ReadSegmentFrom(active.Value.Path, from, active.Value.SparseIndex, resolver))
                yield return evt;
        }
    }

    IEnumerable<SubscriptionEvent> ReadSegmentFrom(
        string path,
        GlobalPosition from,
        (GlobalPosition Position, long ByteOffset)[] sparseIndex,
        Func<EventKey, SubscriptionEvent>? resolver) {

        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

        var startOffset = FindStartOffset(sparseIndex, from);
        fs.Seek(startOffset, SeekOrigin.Begin);

        var posBuf = new byte[8];
        var intBuf = new byte[4];

        while (true) {
            if (fs.Read(posBuf) < 8) yield break;
            var pos = new GlobalPosition(BinaryPrimitives.ReadUInt64LittleEndian(posBuf));

            if (fs.Read(intBuf) < 4) yield break;
            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(intBuf);
            if (keyLen < 0) yield break;

            var keyBytes = new byte[keyLen];
            if (fs.Read(keyBytes) < keyLen) yield break;

            if (_format == SubscriptionLogFormat.FullPayload) {
                if (fs.Read(intBuf) < 4) yield break;
                var valueLen = BinaryPrimitives.ReadInt32LittleEndian(intBuf);
                if (valueLen < 0) yield break;

                var valueBytes = new byte[valueLen];
                if (fs.Read(valueBytes) < valueLen) yield break;

                if (pos >= from) {
                    EventKey key = (ReadOnlyMemory<byte>)keyBytes;
                    EventValue value = (ReadOnlyMemory<byte>)valueBytes;
                    yield return value.ToSubscriptionEvent(key);
                }
            } else {
                // ReferenceOnly: skip value storage, resolve via SST lookup.
                if (pos >= from) {
                    if (resolver == null)
                        throw new InvalidOperationException(
                            "A resolver is required when SubscriptionLogFormat is ReferenceOnly.");
                    EventKey key = (ReadOnlyMemory<byte>)keyBytes;
                    yield return resolver(key);
                }
            }
        }
    }

    /// <summary>
    /// Returns paths of sealed segments whose last event is before <paramref name="cutoffPosition"/>.
    /// These are safe to archive or delete.
    /// </summary>
    internal IReadOnlyList<string> GetArchivablePaths(GlobalPosition cutoffPosition) =>
        _sealed.Where(s => s.End < cutoffPosition).Select(s => s.Path).ToList();

    void OpenNewSegment(GlobalPosition startPosition) {
        _activePath = Path.Combine(_dataDirectory, $"{FilePrefix}{startPosition.Value:D20}{FileSuffix}");
        _activeStart = startPosition;
        _activeEnd = startPosition;
        _activeSparseIndex.Clear();
        _activeEntryCount = 0;
        _activeStream = OpenForAppend(_activePath);
    }

    void RotateSegment(GlobalPosition newStart) {
        _activeStream!.Flush();
        _sealed.Add(new SegmentMeta(_activePath, _activeStart, _activeEnd, [.._activeSparseIndex]));
        _activeStream.Dispose();
        _activeStream = null;
        OpenNewSegment(newStart);
    }

    (GlobalPosition End, (GlobalPosition Position, long ByteOffset)[] SparseIndex, int EntryCount) ScanForIndex(string path) {
        var index = new List<(GlobalPosition, long)>();
        var end = GlobalPosition.Start;
        int count = 0;

        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var smallBuf = new byte[8];

        while (true) {
            long entryOffset = fs.Position;
            if (fs.Read(smallBuf, 0, 8) < 8) break;
            var pos = new GlobalPosition(BinaryPrimitives.ReadUInt64LittleEndian(smallBuf));

            if (fs.Read(smallBuf, 0, 4) < 4) break;
            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(smallBuf);
            if (keyLen < 0 || fs.Position + keyLen > fs.Length) break;
            fs.Seek(keyLen, SeekOrigin.Current);

            if (_format == SubscriptionLogFormat.FullPayload) {
                if (fs.Read(smallBuf, 0, 4) < 4) break;
                var valueLen = BinaryPrimitives.ReadInt32LittleEndian(smallBuf);
                if (valueLen < 0 || fs.Position + valueLen > fs.Length) break;
                fs.Seek(valueLen, SeekOrigin.Current);
            }

            if (count % SparseEvery == 0) index.Add((pos, entryOffset));
            end = pos;
            count++;
        }

        return (end, index.ToArray(), count);
    }

    static long FindStartOffset((GlobalPosition Position, long ByteOffset)[] sparseIndex, GlobalPosition from) {
        if (sparseIndex.Length == 0) return 0;
        int lo = 0, hi = sparseIndex.Length - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) / 2;
            if (sparseIndex[mid].Position <= from) lo = mid;
            else hi = mid - 1;
        }
        return sparseIndex[lo].Position <= from ? sparseIndex[lo].ByteOffset : 0;
    }

    static ulong ParseStartPosition(string path) {
        var name = Path.GetFileNameWithoutExtension(path);
        return ulong.Parse(name.AsSpan(FilePrefix.Length));
    }

    static FileStream OpenForAppend(string path) => new FileStream(
        path,
        FileMode.OpenOrCreate,
        FileAccess.Write,
        FileShare.Read,
        bufferSize: 4096,
        FileOptions.Asynchronous | FileOptions.WriteThrough);

    /// <inheritdoc/>
    public void Dispose() => _activeStream?.Dispose();

    readonly record struct SegmentMeta(
        string Path,
        GlobalPosition Start,
        GlobalPosition End,
        (GlobalPosition Position, long ByteOffset)[] SparseIndex);
}
