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
public sealed partial class SubscriptionLog : IDisposable {
    const int SparseEvery = 128;
    const string FilePrefix = "subscriptions-";
    const string FileSuffix = ".log";

    readonly string _dataDirectory;
    readonly IEntryCodec _codec;
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

    /// <summary>
    /// The format used to encode entries in this log.
    /// </summary>
    public SubscriptionLogFormat Format => _codec.Format;

    SubscriptionLog(string dataDirectory, IEntryCodec codec, long segmentSizeBytes) {
        _dataDirectory = dataDirectory;
        _codec = codec;
        _segmentSizeBytes = segmentSizeBytes;
    }

    /// <summary>
    /// Opens or creates the subscription log in <paramref name="dataDirectory"/>.
    /// </summary>
    /// <param name="dataDirectory">The directory where segment files are stored.</param>
    /// <param name="format">Determines whether entries store the full event payload or only the key reference.</param>
    /// <param name="segmentSizeBytes">Maximum size in bytes of a single segment file before a new segment is started.</param>
    /// <returns>An opened <see cref="SubscriptionLog"/> ready to append and scan.</returns>
    public static SubscriptionLog Open(string dataDirectory, SubscriptionLogFormat format, long segmentSizeBytes) {
        IEntryCodec codec = format switch {
            SubscriptionLogFormat.FullPayload => FullPayloadCodec.Instance,
            SubscriptionLogFormat.ReferenceOnly => ReferenceOnlyCodec.Instance,
            _ => throw new ArgumentOutOfRangeException(nameof(format))
        };
        var log = new SubscriptionLog(dataDirectory, codec, segmentSizeBytes);
        log.LoadExistingSegments();
        return log;
    }

    void LoadExistingSegments() {
        var files = Directory.GetFiles(_dataDirectory, FilePrefix + "*" + FileSuffix)
            .OrderBy(f => f)
            .ToArray();

        for (var i = 0; i < files.Length; i++) {
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
    public GlobalPosition GetLastPosition() {
        if (_activeStream != null) return _activeEnd;
        if (_sealed.Count > 0) return _sealed[^1].End;
        return GlobalPosition.Start;
    }

    /// <summary>
    /// Appends an event to the log. Must be called under the write lock.
    /// </summary>
    /// <param name="position">The global position of the event.</param>
    /// <param name="key">The event key identifying stream and revision.</param>
    /// <param name="value">The full event value, including the embedded <see cref="GlobalPosition"/> in the last 8 bytes.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    public async ValueTask AppendAsync(GlobalPosition position, EventKey key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken) {
        if (_activeStream == null) {
            OpenNewSegment(position);
        } else if (_activeStream.Length >= _segmentSizeBytes) {
            RotateSegment(position);
        }

        ReadOnlyMemory<byte> keyBytes = key;
        var headerSize = 12 + keyBytes.Length;
        var entrySize = headerSize + _codec.PayloadSize(value);

        var buf = ArrayPool<byte>.Shared.Rent(entrySize);
        try {
            WriteCommonHeader(buf, position, keyBytes);
            _codec.EncodePayload(buf.AsSpan(headerSize), value);

            if (_activeEntryCount % SparseEvery == 0)
                _activeSparseIndex.Add((position, _activeStream!.Position));

            await _activeStream!.WriteAsync(buf.AsMemory(0, entrySize), cancellationToken);
            await _activeStream.FlushAsync(cancellationToken);
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
    /// <param name="from">The global position to start scanning from (inclusive).</param>
    /// <param name="resolver">
    /// Optional function to reconstruct a <see cref="SubscriptionEvent"/> from an <see cref="EventKey"/>.
    /// Required when the log format is <see cref="SubscriptionLogFormat.ReferenceOnly"/>.
    /// </param>
    /// <returns>A lazily-evaluated sequence of subscription events in global-position order.</returns>
    public IEnumerable<SubscriptionEvent> ScanFrom(
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
        fs.Seek(FindStartOffset(sparseIndex, from), SeekOrigin.Begin);

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

            if (pos < from) {
                if (!_codec.TrySkipPayload(fs, intBuf)) yield break;
                continue;
            }

            EventKey key = (ReadOnlyMemory<byte>)keyBytes;
            if (!_codec.TryDecodeEvent(fs, intBuf, key, pos, resolver, out var evt)) yield break;
            yield return evt;
        }
    }

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
        var count = 0;

        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var buf = new byte[8];

        while (true) {
            var entryOffset = fs.Position;
            if (fs.Read(buf, 0, 8) < 8) break;
            var pos = new GlobalPosition(BinaryPrimitives.ReadUInt64LittleEndian(buf));

            if (fs.Read(buf, 0, 4) < 4) break;
            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(buf);
            if (keyLen < 0 || fs.Position + keyLen > fs.Length) break;
            fs.Seek(keyLen, SeekOrigin.Current);

            if (!_codec.TrySkipPayload(fs, buf.AsSpan(0, 4))) break;

            if (count % SparseEvery == 0) index.Add((pos, entryOffset));
            end = pos;
            count++;
        }

        return (end, index.ToArray(), count);
    }

    static void WriteCommonHeader(Span<byte> span, GlobalPosition position, ReadOnlyMemory<byte> keyBytes) {
        BinaryPrimitives.WriteUInt64LittleEndian(span, position.Value);
        BinaryPrimitives.WriteInt32LittleEndian(span[8..], keyBytes.Length);
        keyBytes.Span.CopyTo(span[12..]);
    }

    static long FindStartOffset((GlobalPosition Position, long ByteOffset)[] sparseIndex, GlobalPosition from) {
        if (sparseIndex.Length == 0) return 0;
        int lo = 0, hi = sparseIndex.Length - 1;
        while (lo < hi) {
            var mid = (lo + hi + 1) / 2;
            if (sparseIndex[mid].Position <= from) lo = mid;
            else hi = mid - 1;
        }
        return sparseIndex[lo].Position <= from ? sparseIndex[lo].ByteOffset : 0;
    }

    static ulong ParseStartPosition(string path) {
        var name = Path.GetFileNameWithoutExtension(path);
        return ulong.Parse(name.AsSpan(FilePrefix.Length));
    }

    static FileStream OpenForAppend(string path) => new(
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
