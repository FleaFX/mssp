namespace MSSP;

/// <summary>
/// Specifies the direction in which to read events from a stream.
/// </summary>
public enum ReadDirection {
    /// <summary>
    /// Read events from the lowest to the highest revision (oldest to newest).
    /// </summary>
    Forwards = 1,

    /// <summary>
    /// Read events from the highest to the lowest revision (newest to oldest).
    /// </summary>
    Backwards = 2
}
