using System.Buffers.Binary;

namespace MSSP.Engine;

partial class SubscriptionLog {
    /// <summary>
    /// Stores the complete event value inline: <c>[valueLen: 4 LE][value: valueLen bytes]</c>.
    /// Catch-up reads are purely sequential; no SST lookups required.
    /// </summary>
    sealed class FullPayloadCodec : IEntryCodec {
        internal static readonly FullPayloadCodec Instance = new();

        SubscriptionLogFormat IEntryCodec.Format => SubscriptionLogFormat.FullPayload;

        /// <inheritdoc />
        int IEntryCodec.PayloadSize(ReadOnlyMemory<byte> value) => 4 + value.Length;

        /// <inheritdoc />
        void IEntryCodec.EncodePayload(Span<byte> dest, ReadOnlyMemory<byte> value) {
            BinaryPrimitives.WriteInt32LittleEndian(dest, value.Length);
            value.Span.CopyTo(dest[4..]);
        }

        /// <inheritdoc />
        bool IEntryCodec.TrySkipPayload(Stream stream, Span<byte> intBuf) {
            if (stream.Read(intBuf[..4]) < 4) return false;
            var valueLen = BinaryPrimitives.ReadInt32LittleEndian(intBuf);
            if (valueLen < 0 || stream.Position + valueLen > stream.Length) return false;
            stream.Seek(valueLen, SeekOrigin.Current);
            return true;
        }

        /// <inheritdoc />
        bool IEntryCodec.TryDecodeEvent(Stream stream, Span<byte> intBuf, EventKey key, GlobalPosition pos,
            Func<EventKey, SubscriptionEvent>? resolver, out SubscriptionEvent evt) {
            if (stream.Read(intBuf[..4]) < 4) { evt = default; return false; }
            var valueLen = BinaryPrimitives.ReadInt32LittleEndian(intBuf);
            if (valueLen < 0) { evt = default; return false; }
            var valueBytes = new byte[valueLen];
            if (stream.Read(valueBytes) < valueLen) { evt = default; return false; }
            EventValue value = (ReadOnlyMemory<byte>)valueBytes;
            evt = value.ToSubscriptionEvent(key);
            return true;
        }
    }
}
