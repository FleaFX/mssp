using Microsoft.Extensions.Hosting;
using MSSP.BloomFilters;
using MSSP.LsmTree;

namespace MSSP.Embedded;

/// <summary>
/// Manages the lifecycle of <see cref="EmbeddedMsspClient"/> as an <see cref="IHostedService"/>.
/// Opens the store on host startup and disposes it on shutdown.
/// </summary>
public sealed class MsspHostedService(MsspOptions options) : IHostedService, IDisposable {
    EmbeddedMsspClient? _client;
    bool _disposed;

    /// <summary>
    /// Gets the <see cref="IMsspClient"/> once the host has started.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if accessed before <see cref="StartAsync"/> has completed.</exception>
    internal IMsspClient Client =>
        _client ?? throw new InvalidOperationException("IMsspClient is not available before the host has started.");

    /// <inheritdoc/>
    public async Task StartAsync(CancellationToken cancellationToken) {
        ISstAccess<EventKey>? sst = options.UseBloomFilters
            ? new BloomFilteredSstAccess<EventKey>(DefaultSstAccess<EventKey>.Instance)
            : null;
        _client = await EmbeddedMsspClient.OpenAsync(options.DataDirectory, options.MemTableCapacityBytes, sst, cancellationToken);
    }

    /// <inheritdoc/>
    public Task StopAsync(CancellationToken cancellationToken) {
        Dispose();
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public void Dispose() {
        if (_disposed) return;
        _disposed = true;
        _client?.Dispose();
        _client = null;
    }
}
