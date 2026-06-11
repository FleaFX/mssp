namespace MSSP.Engine;

/// <summary>
/// Controls how events are stored in the subscription log.
/// </summary>
public enum SubscriptionLogFormat {
    /// <summary>
    /// Each entry stores only the <see cref="EventKey"/> pointer.
    /// Produces a smaller log file, but catch-up requires a point-lookup
    /// into the SST files for each event.
    /// </summary>
    ReferenceOnly,

    /// <summary>
    /// Each entry stores the full event payload alongside the key.
    /// Doubles the on-disk data but enables purely sequential catch-up reads.
    /// This is the recommended default for small to medium payloads.
    /// </summary>
    FullPayload
}
