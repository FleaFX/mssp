namespace MSSP.Log;

/// <summary>
/// Marker interface for log records.
/// </summary>
/// <typeparam name="TRecord">The type of the log record.</typeparam>
interface ILogRecord<TRecord> where TRecord : ILogRecord<TRecord> {
    /// <summary>
    /// Implicitly casts the given <paramref name="record"/> to a <see cref="Memory{T}"/>.
    /// </summary>
    /// <param name="record">The record to cast.</param>
    static abstract implicit operator Memory<byte>(TRecord record);

    /// <summary>
    /// Implicitly casts the given <paramref name="memory"/> to a <typeparamref name="TRecord"/>.
    /// </summary>
    /// <param name="memory">The memory to cast.</param>
    static abstract implicit operator TRecord(Memory<byte> memory);
}