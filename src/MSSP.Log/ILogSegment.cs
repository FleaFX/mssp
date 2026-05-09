namespace MSSP.Log;

/// <summary>
/// A bounded segment of a log that can be appended to, enumerated, and completed.
/// </summary>
/// <typeparam name="TRecord">The type of a record in the segment.</typeparam>
public interface ILogSegment<TRecord> : ILog<TRecord>, IDisposable where TRecord : ILogRecord<TRecord> {
    /// <summary>
    /// Marks the segment as complete. No further records may be appended.
    /// </summary>
    void Complete();
}
