namespace MSSP.Log;

/// <summary>
/// Provides log structured record storage.
/// </summary>
/// <typeparam name="TRecord">The type of a record in the log.</typeparam>
public interface ILog<TRecord> : IAsyncEnumerable<TRecord> where TRecord : ILogRecord<TRecord> {
    /// <summary>
    /// Appends the given <paramref name="record"/> to the log.
    /// </summary>
    /// <param name="record">The record to append to the log.</param>
    /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> that may be used to cancel the asynchronous operation.</param>
    ValueTask<bool> TryAppendAsync(TRecord record, CancellationToken cancellationToken = new());
}