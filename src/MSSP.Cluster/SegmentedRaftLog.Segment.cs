using System.Buffers.Binary;
using System.IO.Hashing;
using MSSP.Raft;

namespace MSSP.Cluster;

sealed partial class SegmentedRaftLog {
    /// <summary>
    /// A single segment file: one append-only file plus an in-memory offset index.
    /// </summary>
    sealed class Segment : IDisposable {
        readonly FileStream _file;
        readonly List<long> _offsets = []; // _offsets[i] = file offset of entry at BaseIndex + i

        public ulong BaseIndex { get; }
        public ulong LastIndex => _offsets.Count > 0 ? BaseIndex + (ulong)_offsets.Count - 1 : 0;
        public ulong LastTerm  { get; private set; }
        public long  SizeBytes => _file.Length;
        public bool  IsEmpty   => _offsets.Count == 0;

        Segment(FileStream file, ulong baseIndex) {
            _file = file;
            BaseIndex = baseIndex;
        }

        public static async ValueTask<Segment> OpenAsync(string path, ulong baseIndex, CancellationToken cancellationToken) {
            var file = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite,
                FileShare.None, 4096, FileOptions.WriteThrough | FileOptions.Asynchronous);
            var seg = new Segment(file, baseIndex);
            await seg.RecoverAsync(cancellationToken);
            return seg;
        }

        public static async ValueTask<Segment> CreateAsync(string path, ulong baseIndex, CancellationToken cancellationToken) {
            var file = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite,
                FileShare.None, 4096, FileOptions.WriteThrough | FileOptions.Asynchronous);
            return new Segment(file, baseIndex);
        }

        async Task RecoverAsync(CancellationToken cancellationToken) {
            _file.Seek(0, SeekOrigin.Begin);
            var headerBuf = new byte[HeaderSize];
            var crcBuf    = new byte[4];

            while (true) {
                var offset = _file.Position;
                var read = await _file.ReadAsync(headerBuf, cancellationToken);
                if (read == 0) break;
                if (read < HeaderSize) { _file.SetLength(offset); break; }

                var term       = BinaryPrimitives.ReadUInt64LittleEndian(headerBuf);
                var payloadLen = BinaryPrimitives.ReadInt32LittleEndian(headerBuf.AsSpan(17));
                if (payloadLen < 0) { _file.SetLength(offset); break; }

                var payload     = new byte[payloadLen];
                var payloadRead = await _file.ReadAsync(payload, cancellationToken);
                if (payloadRead < payloadLen) { _file.SetLength(offset); break; }

                var crcRead = await _file.ReadAsync(crcBuf, cancellationToken);
                if (crcRead < 4) { _file.SetLength(offset); break; }

                var crcStored   = BinaryPrimitives.ReadUInt32LittleEndian(crcBuf);
                var crcComputed = ComputeCrc(headerBuf, payload);
                if (crcStored != crcComputed) { _file.SetLength(offset); break; }

                _offsets.Add(offset);
                LastTerm = term;
            }
        }

        public async ValueTask<RaftLogEntry> ReadEntryAsync(ulong index, CancellationToken cancellationToken) {
            var offset = _offsets[(int)(index - BaseIndex)];
            _file.Seek(offset, SeekOrigin.Begin);

            var headerBuf = new byte[HeaderSize];
            await _file.ReadExactlyAsync(headerBuf, cancellationToken);

            var term       = BinaryPrimitives.ReadUInt64LittleEndian(headerBuf);
            var idx        = BinaryPrimitives.ReadUInt64LittleEndian(headerBuf.AsSpan(8));
            var type       = (RaftLogEntryType)headerBuf[16];
            var payloadLen = BinaryPrimitives.ReadInt32LittleEndian(headerBuf.AsSpan(17));

            var payload = new byte[payloadLen];
            if (payload.Length > 0)
                await _file.ReadExactlyAsync(payload, cancellationToken);

            return new RaftLogEntry(term, idx, type, payload);
        }

        public async ValueTask AppendEntryAsync(RaftLogEntry entry, CancellationToken cancellationToken) {
            _file.Seek(0, SeekOrigin.End);
            var offset  = _file.Position;
            var payload = entry.Payload.IsEmpty ? [] : entry.Payload.ToArray();
            var buf     = new byte[HeaderSize + payload.Length + FooterSize];
            var span    = buf.AsSpan();

            BinaryPrimitives.WriteUInt64LittleEndian(span,        entry.Term);
            BinaryPrimitives.WriteUInt64LittleEndian(span[8..],   entry.Index);
            span[16] = (byte)entry.Type;
            BinaryPrimitives.WriteInt32LittleEndian(span[17..],   payload.Length);
            payload.CopyTo(span[HeaderSize..]);

            var crc = ComputeCrc(span[..HeaderSize].ToArray(), payload);
            BinaryPrimitives.WriteUInt32LittleEndian(span[(HeaderSize + payload.Length)..], crc);

            await _file.WriteAsync(buf, cancellationToken);
            _offsets.Add(offset);
            LastTerm = entry.Term;
        }

        public void TruncateFrom(ulong fromIndex) {
            if (fromIndex < BaseIndex || fromIndex > LastIndex) return;
            var i = (int)(fromIndex - BaseIndex);
            var truncateOffset = _offsets[i];
            _file.SetLength(truncateOffset);
            _offsets.RemoveRange(i, _offsets.Count - i);
            LastTerm = _offsets.Count > 0 ? ReadTermAt(_offsets[^1]) : 0;
        }

        public void DeleteAndDispose() {
            var path = _file.Name;
            _file.Dispose();
            if (File.Exists(path)) File.Delete(path);
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
}
