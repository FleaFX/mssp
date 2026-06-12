namespace MSSP.Engine.Storage;

/// <summary>
/// Provides log structured record storage.
/// </summary>
/// <typeparam name="TRecord">The type of a record in the log.</typeparam>
/// <remarks>
/// Each item yielded by the enumeration is a batch of records that were committed to
/// durable storage together. The apply loop processes a full batch before flushing
/// downstream stores, so that one fsync covers all records in the batch.
/// </remarks>
public interface ILog<TRecord> : IAsyncEnumerable<TRecord[]> where TRecord : ILogRecord<TRecord> {
    /// <summary>
    /// Appends the given <paramref name="record"/> to the log.
    /// </summary>
    /// <param name="record">The record to append to the log.</param>
    /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> that may be used to cancel the asynchronous operation.</param>
    ValueTask<bool> TryAppendAsync(TRecord record, CancellationToken cancellationToken = new());
}
