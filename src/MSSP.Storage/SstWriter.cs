using System.Buffers;
using System.Buffers.Binary;

namespace MSSP.Storage;

/// <summary>
/// Writes a sorted sequence of entries to a stream in the SST file format.
/// </summary>
static class SstWriter {
    /// <summary>
    /// Writes all <paramref name="entries"/> to <paramref name="output"/> as an immutable SST file.
    /// </summary>
    /// <remarks>
    /// Entries must be provided in ascending key order. The stream must be writable,
    /// seekable, and positioned at offset 0.
    /// </remarks>
    /// <param name="entries">The sorted entries to write. <c>null</c> values represent tombstones.</param>
    /// <param name="output">The destination stream.</param>
    /// <param name="sparseInterval">Number of data entries between consecutive index entries.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    internal static async ValueTask WriteAsync<TKey>(
        IEnumerable<KeyValuePair<TKey, ReadOnlyMemory<byte>?>> entries,
        Stream output,
        int sparseInterval = Sst.DefaultSparseInterval,
        CancellationToken cancellationToken = default)
        where TKey : IKey<TKey> {

        var sparseIndex = new List<(ReadOnlyMemory<byte> KeyBytes, long DataOffset)>();
        var entryCount = 0;

        foreach (var (key, value) in entries) {
            var dataOffset = output.Position;
            ReadOnlyMemory<byte> keyBytes = key;

            if (entryCount % sparseInterval == 0)
                sparseIndex.Add((keyBytes, dataOffset));

            await WriteDataEntryAsync(output, keyBytes, value, cancellationToken);
            entryCount++;
        }

        var indexOffset = output.Position;

        foreach (var (keyBytes, dataOffset) in sparseIndex)
            await WriteIndexEntryAsync(output, keyBytes, dataOffset, cancellationToken);

        await WriteFooterAsync(output, indexOffset, entryCount, sparseIndex.Count, sparseInterval, cancellationToken);
    }

    static async ValueTask WriteDataEntryAsync(Stream output, ReadOnlyMemory<byte> keyBytes, ReadOnlyMemory<byte>? value, CancellationToken ct) {
        if (value is null) {
            var len = 1 + 4 + keyBytes.Length;
            var buf = ArrayPool<byte>.Shared.Rent(len);
            try {
                buf[0] = Sst.TombstoneMarker;
                BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(1), keyBytes.Length);
                keyBytes.Span.CopyTo(buf.AsSpan(5));
                await output.WriteAsync(buf.AsMemory(0, len), ct);
            } finally {
                ArrayPool<byte>.Shared.Return(buf);
            }
        } else {
            var val = value.Value;
            var len = 1 + 4 + keyBytes.Length + 4 + val.Length;
            var buf = ArrayPool<byte>.Shared.Rent(len);
            try {
                buf[0] = Sst.WriteMarker;
                BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(1), keyBytes.Length);
                keyBytes.Span.CopyTo(buf.AsSpan(5));
                BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(5 + keyBytes.Length), val.Length);
                val.Span.CopyTo(buf.AsSpan(9 + keyBytes.Length));
                await output.WriteAsync(buf.AsMemory(0, len), ct);
            } finally {
                ArrayPool<byte>.Shared.Return(buf);
            }
        }
    }

    static async ValueTask WriteIndexEntryAsync(Stream output, ReadOnlyMemory<byte> keyBytes, long dataOffset, CancellationToken ct) {
        var len = 4 + keyBytes.Length + 8;
        var buf = ArrayPool<byte>.Shared.Rent(len);
        try {
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(0), keyBytes.Length);
            keyBytes.Span.CopyTo(buf.AsSpan(4));
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(4 + keyBytes.Length), dataOffset);
            await output.WriteAsync(buf.AsMemory(0, len), ct);
        } finally {
            ArrayPool<byte>.Shared.Return(buf);
        }
    }

    static async ValueTask WriteFooterAsync(Stream output, long indexOffset, int entryCount, int indexEntryCount, int sparseInterval, CancellationToken ct) {
        var buf = ArrayPool<byte>.Shared.Rent(Sst.FooterSize);
        try {
            Sst.Magic.CopyTo(buf.AsSpan(0));
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(8), indexOffset);
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(16), entryCount);
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(20), indexEntryCount);
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(24), sparseInterval);
            await output.WriteAsync(buf.AsMemory(0, Sst.FooterSize), ct);
        } finally {
            ArrayPool<byte>.Shared.Return(buf);
        }
    }
}
