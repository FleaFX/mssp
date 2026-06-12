namespace MSSP.Engine;

/// <summary>
/// Configuration options for the embedded MSSP event store.
/// </summary>
public sealed class MsspOptions {
    /// <summary>
    /// The directory in which to store WAL and SST files.
    /// </summary>
    public string DataDirectory { get; set; } = "./mssp-data";

    /// <summary>
    /// The maximum size of the in-memory write buffer before it is flushed to an SST file.
    /// Defaults to 64 MiB.
    /// </summary>
    public int MemTableCapacityBytes { get; set; } = 64 * 1024 * 1024;

    /// <summary>
    /// When <c>true</c>, a bloom filter sidecar (<c>.bf</c>) is maintained alongside each SST file
    /// to skip unnecessary disk reads during point lookups. Defaults to <c>true</c>.
    /// </summary>
    public bool UseBloomFilters { get; set; } = true;

    /// <summary>
    /// The format used for subscription log entries.
    /// <see cref="SubscriptionLogFormat.FullPayload"/> (the default) stores the complete event alongside
    /// the key and enables purely sequential catch-up reads.
    /// <see cref="SubscriptionLogFormat.ReferenceOnly"/> stores only the key pointer and is more
    /// disk-efficient but requires SST lookups during catch-up.
    /// </summary>
    public SubscriptionLogFormat SubscriptionLogFormat { get; set; } = SubscriptionLogFormat.FullPayload;

    /// <summary>
    /// Maximum size in bytes of a single subscription log segment before a new segment is started.
    /// Smaller segments are easier to archive individually. Defaults to 64 MiB.
    /// </summary>
    public long SubscriptionLogSegmentSizeBytes { get; set; } = 64 * 1024 * 1024;
}
