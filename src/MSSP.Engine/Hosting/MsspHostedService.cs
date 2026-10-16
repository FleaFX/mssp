using Microsoft.Extensions.Hosting;
using System.Diagnostics.Metrics;
using MSSP.Engine.BloomFilters;
using MSSP.Engine.Storage;

namespace MSSP.Engine;

/// <summary>
/// Manages the lifecycle of <see cref="EmbeddedMsspClient"/> as an <see cref="IHostedService"/>.
/// Opens the store on host startup and disposes it on shutdown.
/// </summary>
/// <remarks>
/// Under a host, the store opens in <see cref="StartingAsync"/>, before any hosted service starts,
/// and closes in <see cref="StoppedAsync"/>, after every hosted service has stopped. Other hosted
/// services can therefore use the store from their <c>StartAsync</c> until the end of their
/// <c>StopAsync</c>, whatever order they were registered in. Used without a host,
/// <see cref="StartAsync"/> opens the store and <see cref="StopAsync"/> closes it.
/// </remarks>
public sealed class MsspHostedService(
    MsspOptions options,
    IMeterFactory? meterFactory = null
) : IHostedLifecycleService, IAsyncDisposable {
    EmbeddedMsspClient? _client;
    bool _hostManaged;

    /// <summary>
    /// Gets the <see cref="EmbeddedMsspClient"/> while the store is open.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown while the store is not open: before the host has started it, or after the host has stopped it.</exception>
    public EmbeddedMsspClient Client =>
        _client ?? throw new InvalidOperationException("IMsspClient is not available before the host has started.");

    /// <inheritdoc/>
    public Task StartingAsync(CancellationToken cancellationToken) {
        _hostManaged = true;
        return OpenAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public Task StartAsync(CancellationToken cancellationToken) =>
        OpenAsync(cancellationToken);

    /// <inheritdoc/>
    public Task StartedAsync(CancellationToken cancellationToken) =>
        Task.CompletedTask;

    /// <inheritdoc/>
    public Task StoppingAsync(CancellationToken cancellationToken) =>
        Task.CompletedTask;

    /// <inheritdoc/>
    public Task StopAsync(CancellationToken cancellationToken) =>
        // Under a host, StoppedAsync closes the store, after every other hosted service has stopped.
        _hostManaged ? Task.CompletedTask : CloseAsync();

    /// <inheritdoc/>
    public Task StoppedAsync(CancellationToken cancellationToken) =>
        CloseAsync();

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() =>
        await CloseAsync();

    /// <summary>
    /// Opens the store, unless it is already open.
    /// </summary>
    async Task OpenAsync(CancellationToken cancellationToken) {
        if (_client is not null) return;
        ISstAccess<EventKey>? sst = options.UseBloomFilters
            ? new BloomFilteredSstAccess<EventKey>(DefaultSstAccess<EventKey>.Instance)
            : null;
        _client = await EmbeddedMsspClient.OpenAsync(
            options.DataDirectory,
            options.MemTableCapacityBytes,
            sst,
            options.SubscriptionLogFormat,
            options.SubscriptionLogSegmentSizeBytes,
            meterFactory,
            cancellationToken);
    }

    /// <summary>
    /// Closes the store, if it is open.
    /// </summary>
    async Task CloseAsync() {
        if (_client is not {} client) return;
        await client.DisposeAsync();
        _client = null;
    }
}
