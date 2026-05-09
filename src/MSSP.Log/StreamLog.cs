using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;

namespace MSSP.Log;

// On-disk format: sequential length-prefixed records.
// Each record: dataLen(4, little-endian) + data(dataLen bytes) + crc32(4, little-endian)
// The CRC32 covers the data bytes only. A checksum mismatch during recovery indicates
// a torn write or silent disk corruption; recovery stops at the first corrupt record.

/// <summary>
/// Appends log records to a stream and reads them back for recovery.
/// </summary>
/// <remarks>
/// For on-disk durability, open the underlying file with <see cref="FileOptions.WriteThrough"/>.
/// The stream must be seekable for enumeration to work.
/// </remarks>
public sealed class StreamLog<TRecord> : ILogSegment<TRecord> where TRecord : ILogRecord<TRecord> {
    readonly Stream _stream;
    volatile bool _completed;

    /// <summary>
    /// Creates a <see cref="StreamLog{TRecord}"/> that appends to <paramref name="stream"/>.
    /// The stream must be writable and positioned at the end of any existing content.
    /// </summary>
    internal StreamLog(Stream stream) => _stream = stream;

    /// <inheritdoc/>
    public async ValueTask<bool> TryAppendAsync(TRecord record, CancellationToken cancellationToken = default) {
        if (_completed) return false;

        ReadOnlyMemory<byte> bytes = record;
        var buf = ArrayPool<byte>.Shared.Rent(4 + bytes.Length + 4);
        try {
            BinaryPrimitives.WriteInt32LittleEndian(buf, bytes.Length);
            bytes.Span.CopyTo(buf.AsSpan(4));
            BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4 + bytes.Length), Crc32.HashToUInt32(bytes.Span));
            await _stream.WriteAsync(buf.AsMemory(0, 4 + bytes.Length + 4), cancellationToken);
            await _stream.FlushAsync(cancellationToken);
            return true;
        } catch (IOException) {
            return false;
        } finally {
            ArrayPool<byte>.Shared.Return(buf);
        }
    }

    /// <inheritdoc/>
    public void Complete() => _completed = true;

    /// <inheritdoc/>
    async IAsyncEnumerator<TRecord> IAsyncEnumerable<TRecord>.GetAsyncEnumerator(CancellationToken cancellationToken) {
        if (!_stream.CanSeek) yield break;

        _stream.Position = 0;
        var buf = new byte[4];
        while (_stream.Length - _stream.Position >= 4) {
            await _stream.ReadExactlyAsync(buf, CancellationToken.None);
            var len = BinaryPrimitives.ReadInt32LittleEndian(buf);

            if (_stream.Length - _stream.Position < len + 4)
                yield break; // Truncated data or checksum — uncommitted write, discard.

            var data = new byte[len];
            await _stream.ReadExactlyAsync(data, CancellationToken.None);

            await _stream.ReadExactlyAsync(buf, CancellationToken.None);
            if (BinaryPrimitives.ReadUInt32LittleEndian(buf) != Crc32.HashToUInt32(data))
                yield break; // Checksum mismatch — torn write or disk corruption, stop recovery.

            ReadOnlyMemory<byte> bytes = data;
            yield return bytes;
        }
    }

    /// <inheritdoc/>
    public void Dispose() => _stream.Dispose();
}
