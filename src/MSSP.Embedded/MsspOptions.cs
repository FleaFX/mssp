namespace MSSP.Embedded;

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
}
