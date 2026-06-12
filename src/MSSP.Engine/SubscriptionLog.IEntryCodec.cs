namespace MSSP.Engine;

partial class SubscriptionLog {
    /// <summary>
    /// Encodes and decodes the per-entry payload section.
    /// </summary>
    interface IEntryCodec {
        /// <summary>
        /// The log format this codec handles.
        /// </summary>
        SubscriptionLogFormat Format { get; }

        /// <summary>
        /// Returns the number of payload bytes written for the given <paramref name="value"/>.
        /// </summary>
        int PayloadSize(ReadOnlyMemory<byte> value);

        /// <summary>
        /// Writes the payload for <paramref name="value"/> into <paramref name="dest"/>.
        /// </summary>
        void EncodePayload(Span<byte> dest, ReadOnlyMemory<byte> value);

        /// <summary>
        /// Advances <paramref name="stream"/> past the current entry's payload without decoding it.
        /// Returns <see langword="false"/> if the stream is truncated.
        /// </summary>
        bool TrySkipPayload(Stream stream, Span<byte> intBuf);

        /// <summary>
        /// Reads the payload from <paramref name="stream"/> and produces a <see cref="SubscriptionEvent"/>.
        /// Returns <see langword="false"/> if the stream is truncated.
        /// </summary>
        bool TryDecodeEvent(Stream stream, Span<byte> intBuf, EventKey key, GlobalPosition pos,
            Func<EventKey, SubscriptionEvent>? resolver, out SubscriptionEvent evt);
    }
}
