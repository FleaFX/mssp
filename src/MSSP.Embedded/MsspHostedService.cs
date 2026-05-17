using Microsoft.Extensions.Hosting;
using MSSP.BloomFilters;
using MSSP.LsmTree;

namespace MSSP.Embedded;

sealed class MsspHostedService(MsspOptions options) : IHostedService, IDisposable {
    EmbeddedMsspClient? _client;
    bool _disposed;

    internal IMsspClient Client =>
        _client ?? throw new InvalidOperationException("IMsspClient is not available before the host has started.");

    public async Task StartAsync(CancellationToken cancellationToken) {
        ISstAccess<EventKey>? sst = options.UseBloomFilters
            ? new BloomFilteredSstAccess<EventKey>(DefaultSstAccess<EventKey>.Instance)
            : null;
        _client = await EmbeddedMsspClient.OpenAsync(options.DataDirectory, options.MemTableCapacityBytes, sst, cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken) {
        Dispose();
        return Task.CompletedTask;
    }

    public void Dispose() {
        if (_disposed) return;
        _disposed = true;
        _client?.Dispose();
        _client = null;
    }
}
