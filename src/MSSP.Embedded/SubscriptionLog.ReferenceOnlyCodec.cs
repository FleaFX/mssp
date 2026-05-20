namespace MSSP.Embedded;

partial class SubscriptionLog {
    /// <summary>
    /// Stores only the <see cref="EventKey"/> pointer; no value bytes are written to the log.
    /// Catch-up requires a <c>resolver</c> to perform SST lookups for each event.
    /// </summary>
    sealed class ReferenceOnlyCodec : IEntryCodec {
        internal static readonly ReferenceOnlyCodec Instance = new();

        SubscriptionLogFormat IEntryCodec.Format => SubscriptionLogFormat.ReferenceOnly;

        /// <inheritdoc />
        int IEntryCodec.PayloadSize(ReadOnlyMemory<byte> value) => 0;

        /// <inheritdoc />
        void IEntryCodec.EncodePayload(Span<byte> dest, ReadOnlyMemory<byte> value) { }

        /// <inheritdoc />
        bool IEntryCodec.TrySkipPayload(Stream stream, Span<byte> intBuf) => true;

        /// <inheritdoc />
        bool IEntryCodec.TryDecodeEvent(Stream stream, Span<byte> intBuf, EventKey key, GlobalPosition pos,
            Func<EventKey, SubscriptionEvent>? resolver, out SubscriptionEvent evt) {
            if (resolver == null)
                throw new InvalidOperationException(
                    "A resolver is required when SubscriptionLogFormat is ReferenceOnly.");
            evt = resolver(key);
            return true;
        }
    }
}
