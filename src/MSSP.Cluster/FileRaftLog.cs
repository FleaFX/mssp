using System.Buffers.Binary;
using System.IO.Hashing;
using MSSP.Raft;

namespace MSSP.Cluster;

// Entry layout: [term:8LE][index:8LE][type:1][payloadLen:4LE][payload:bytes][crc32:4LE]
sealed class FileRaftLog : IRaftLog, IDisposable {
    const int HeaderSize = 8 + 8 + 1 + 4; // term + index + type + payloadLen
    const int FooterSize = 4;              // crc32

    readonly FileStream _file;
    readonly List<long> _offsets = new(); // _offsets[i] = file offset of entry at logIndex (i+1)

    FileRaftLog(FileStream file) => _file = file;

    public ulong LastIndex => (ulong)_offsets.Count;
    public ulong LastTerm { get; private set; }

    public static async ValueTask<FileRaftLog> OpenAsync(string dataDirectory, CancellationToken ct = default) {
        Directory.CreateDirectory(dataDirectory);
        var path = Path.Combine(dataDirectory, "raft.log");
        var file = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite,
            FileShare.None, 4096, FileOptions.WriteThrough | FileOptions.Asynchronous);

        var log = new FileRaftLog(file);
        await log.RecoverAsync(ct);
        return log;
    }

    async Task RecoverAsync(CancellationToken ct) {
        _file.Seek(0, SeekOrigin.Begin);
        var headerBuf = new byte[HeaderSize];
        var crcBuf = new byte[4];

        while (true) {
            var offset = _file.Position;
            var read = await _file.ReadAsync(headerBuf, ct);
            if (read == 0) break;
            if (read < HeaderSize) { await TruncateTo(offset); break; }

            var term = BinaryPrimitives.ReadUInt64LittleEndian(headerBuf);
            var index = BinaryPrimitives.ReadUInt64LittleEndian(headerBuf.AsSpan(8));
            var type = headerBuf[16];
            var payloadLen = BinaryPrimitives.ReadInt32LittleEndian(headerBuf.AsSpan(17));

            if (payloadLen < 0) { await TruncateTo(offset); break; }

            var payload = new byte[payloadLen];
            var payloadRead = await _file.ReadAsync(payload, ct);
            if (payloadRead < payloadLen) { await TruncateTo(offset); break; }

            var crcRead = await _file.ReadAsync(crcBuf, ct);
            if (crcRead < 4) { await TruncateTo(offset); break; }

            // verify CRC32 over header + payload
            var crcStored = BinaryPrimitives.ReadUInt32LittleEndian(crcBuf);
            var crcComputed = ComputeCrc(headerBuf, payload);
            if (crcStored != crcComputed) { await TruncateTo(offset); break; }

            _offsets.Add(offset);
            LastTerm = term;
        }
    }

    public async ValueTask<RaftLogEntry> GetEntryAsync(ulong index, CancellationToken ct = default) {
        if (index == 0 || index > LastIndex)
            throw new ArgumentOutOfRangeException(nameof(index));

        var offset = _offsets[(int)(index - 1)];
        _file.Seek(offset, SeekOrigin.Begin);

        var headerBuf = new byte[HeaderSize];
        await _file.ReadExactlyAsync(headerBuf, ct);

        var term = BinaryPrimitives.ReadUInt64LittleEndian(headerBuf);
        var idx = BinaryPrimitives.ReadUInt64LittleEndian(headerBuf.AsSpan(8));
        var type = (RaftLogEntryType)headerBuf[16];
        var payloadLen = BinaryPrimitives.ReadInt32LittleEndian(headerBuf.AsSpan(17));

        var payload = new byte[payloadLen];
        if (payload.Length > 0)
            await _file.ReadExactlyAsync(payload, ct);

        return new RaftLogEntry(term, idx, type, payload);
    }

    public async IAsyncEnumerable<RaftLogEntry> GetEntriesFromAsync(ulong fromIndex, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default) {
        for (var i = fromIndex; i <= LastIndex; i++) {
            if (ct.IsCancellationRequested) yield break;
            yield return await GetEntryAsync(i, ct);
        }
    }

    public async ValueTask AppendAsync(IEnumerable<RaftLogEntry> entries, CancellationToken ct = default) {
        _file.Seek(0, SeekOrigin.End);

        foreach (var entry in entries) {
            var offset = _file.Position;
            var payload = entry.Payload.IsEmpty ? Array.Empty<byte>() : entry.Payload.ToArray();
            var buf = new byte[HeaderSize + payload.Length + FooterSize];
            var span = buf.AsSpan();

            BinaryPrimitives.WriteUInt64LittleEndian(span, entry.Term);
            BinaryPrimitives.WriteUInt64LittleEndian(span[8..], entry.Index);
            span[16] = (byte)entry.Type;
            BinaryPrimitives.WriteInt32LittleEndian(span[17..], payload.Length);
            payload.CopyTo(span[HeaderSize..]);

            var crc = ComputeCrc(span[..HeaderSize].ToArray(), payload);
            BinaryPrimitives.WriteUInt32LittleEndian(span[(HeaderSize + payload.Length)..], crc);

            await _file.WriteAsync(buf, ct);
            _offsets.Add(offset);
            LastTerm = entry.Term;
        }
    }

    public ValueTask TruncateFromAsync(ulong fromIndex, CancellationToken ct = default) {
        if (fromIndex == 0 || fromIndex > LastIndex) return ValueTask.CompletedTask;

        var truncateOffset = _offsets[(int)(fromIndex - 1)];
        _file.SetLength(truncateOffset);
        _offsets.RemoveRange((int)(fromIndex - 1), _offsets.Count - (int)(fromIndex - 1));
        LastTerm = _offsets.Count > 0 ? ReadTermAt(_offsets[^1]) : 0;
        return ValueTask.CompletedTask;
    }

    public async ValueTask<ulong> GetTermAtAsync(ulong index, CancellationToken ct = default) {
        var entry = await GetEntryAsync(index, ct);
        return entry.Term;
    }

    async ValueTask TruncateTo(long offset) {
        _file.SetLength(offset);
        await Task.CompletedTask;
    }

    ulong ReadTermAt(long offset) {
        Span<byte> buf = stackalloc byte[8];
        _file.Seek(offset, SeekOrigin.Begin);
        _file.ReadExactly(buf);
        return BinaryPrimitives.ReadUInt64LittleEndian(buf);
    }

    static uint ComputeCrc(byte[] header, byte[] payload) {
        var crc = new Crc32();
        crc.Append(header);
        if (payload.Length > 0) crc.Append(payload);
        return BinaryPrimitives.ReadUInt32LittleEndian(crc.GetCurrentHash());
    }

    public void Dispose() => _file.Dispose();
}
