using System.Buffers.Binary;
using MSSP.Storage;

namespace MSSP.Embedded;

/// <summary>
/// <see cref="ILsmStore{TKey}"/> decorator that injects the next <see cref="GlobalPosition"/>
/// into the reserved slot (last 8 bytes) of every written value before forwarding to the inner
/// store. The position is derived from <see cref="ISubscriptionProvider.CurrentPosition"/> so
/// it stays correct after failover: when a follower becomes the new leader its first write
/// continues from the last position recorded in the subscription log.
/// </summary>
/// <remarks>
/// <para>
/// Every value passed to <see cref="WriteAsync"/> must be at least 8 bytes long; the last 8 bytes
/// are reserved for the <see cref="GlobalPosition"/> and are overwritten unconditionally.
/// <see cref="EventValue.From"/> always satisfies this contract.
/// </para>
/// <para>
/// Must be called while the caller holds the write lock.
/// Scans are forwarded transparently to the inner store.
/// </para>
/// </remarks>
public sealed class GlobalPositionDecorator(
    ILsmStore<EventKey> inner,
    ISubscriptionProvider subscriptions
) : ILsmStore<EventKey> {

    /// <inheritdoc/>
    public async ValueTask WriteAsync(EventKey key, Memory<byte> value, CancellationToken ct) {
        var pos = new GlobalPosition(subscriptions.CurrentPosition.Value + 1);
        BinaryPrimitives.WriteUInt64LittleEndian(value.Span[^8..], pos.Value);
        await inner.WriteAsync(key, value, ct);
    }

    /// <inheritdoc/>
    public IEnumerable<KeyValuePair<EventKey, ReadOnlyMemory<byte>?>> ScanSnapshotFrom(EventKey from)
        => inner.ScanSnapshotFrom(from);

    /// <inheritdoc/>
    public IEnumerable<KeyValuePair<EventKey, ReadOnlyMemory<byte>?>> ScanAllFrom(EventKey from)
        => inner.ScanAllFrom(from);

    /// <inheritdoc/>
    public void Dispose() => inner.Dispose();
}
