using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MSSP.Engine;
using MSSP.Extensions;
using MSSP.Raft;

namespace MSSP.Cluster;

public class ClusterHostingTests {
    [Fact]
    public async Task Client_CanBeResolved_BeforeTheHostStarts() {
        var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
            var services = new ServiceCollection();
            AddSingleNodeCluster(services, dir);
            await using var provider = services.BuildServiceProvider();

            var client = provider.GetRequiredService<IMsspClient>();

            client.Should().NotBeNull();
        } finally {
            DeleteDirectory(dir);
        }
    }

    [Fact]
    public async Task Client_Throws_WhenCalledBeforeTheHostStarts() {
        var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
            var services = new ServiceCollection();
            AddSingleNodeCluster(services, dir);
            await using var provider = services.BuildServiceProvider();
            var client = provider.GetRequiredService<IMsspClient>();

            var act = async () => await client.ReadAsync("s").EnumerateAsync();

            await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("Client is not available before the host has started.");
        } finally {
            DeleteDirectory(dir);
        }
    }

    [Fact]
    public async Task HostedService_UsesClient_WhenRegisteredBeforeAddMssp() {
        var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
            var state = new ProbeState();
            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton(state);
            builder.Services.AddHostedService<ReadingProbe>();
            AddSingleNodeCluster(builder.Services, dir);
            using var host = builder.Build();
            try {
                await host.StartAsync(TestContext.Current.CancellationToken);
                await state.Started.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            } finally {
                await host.StopAsync(CancellationToken.None);
            }

            state.StopFailure.Should().BeNull();
        } finally {
            DeleteDirectory(dir);
        }
    }

    [Fact]
    public async Task HostedService_UsesClient_WhileStopping_WhenRegisteredBeforeAddMssp() {
        var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
            var state = new ProbeState();
            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton(state);
            builder.Services.AddHostedService<ReadingProbe>();
            AddSingleNodeCluster(builder.Services, dir);
            using var host = builder.Build();
            try {
                await host.StartAsync(TestContext.Current.CancellationToken);
                await state.Started.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            } finally {
                await host.StopAsync(CancellationToken.None);
            }

            state.StopFailure.Should().BeNull();
        } finally {
            DeleteDirectory(dir);
        }
    }

    static void AddSingleNodeCluster(IServiceCollection services, string dir) {
        services.AddMssp(o => o.DataDirectory = dir)
            .AddCluster(o => {
                o.NodeId = "node-1";
                o.Peers = [new RaftClusterMember("node-1", new Uri("http://127.0.0.1:1"))];
            });
    }

    static void DeleteDirectory(string dir) {
        if (Directory.Exists(dir))
            Directory.Delete(dir, recursive: true);
    }
}

sealed class ProbeState {
    public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Exception? StopFailure { get; set; }
}

sealed class ReadingProbe(IMsspClient client, ProbeState state) : BackgroundService {
    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        try {
            await foreach (var _ in client.ReadAsync("probe-stream", cancellationToken: stoppingToken)) { }
            state.Started.TrySetResult();
        } catch (Exception ex) {
            state.Started.TrySetException(ex);
            return;
        }

        try {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        } catch (OperationCanceledException) { }

        try {
            await foreach (var _ in client.ReadAsync("probe-stream", cancellationToken: CancellationToken.None)) { }
        } catch (Exception ex) {
            state.StopFailure = ex;
        }
    }
}
