using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MSSP.Extensions;

namespace MSSP.Engine;

public class MsspHostingTests {
    [Fact]
    public async Task Client_CanBeResolved_BeforeTheHostStarts() {
        var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
            var services = new ServiceCollection();
            services.AddMssp(o => o.DataDirectory = dir);
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
            services.AddMssp(o => o.DataDirectory = dir);
            await using var provider = services.BuildServiceProvider();
            var client = provider.GetRequiredService<IMsspClient>();

            var append = async () => await client.AppendAsync("s", StreamRevision.NoStream, [Event("e")]);
            await append.Should().ThrowAsync<InvalidOperationException>().WithMessage("IMsspClient is not available before the host has started.");

            var read = async () => await client.ReadAsync("s").EnumerateAsync();
            await read.Should().ThrowAsync<InvalidOperationException>().WithMessage("IMsspClient is not available before the host has started.");
        } finally {
            DeleteDirectory(dir);
        }
    }

    [Fact]
    public async Task HostedService_UsesClient_WhenRegisteredAfterAddMssp() {
        var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
            var state = new ProbeState();
            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton(state);
            builder.Services.AddMssp(o => o.DataDirectory = dir);
            builder.Services.AddHostedService<AppendingProbe>();
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
    public async Task HostedService_UsesClient_WhenRegisteredBeforeAddMssp() {
        var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
            var state = new ProbeState();
            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton(state);
            builder.Services.AddHostedService<AppendingProbe>();
            builder.Services.AddMssp(o => o.DataDirectory = dir);
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
            builder.Services.AddHostedService<AppendingProbe>();
            builder.Services.AddMssp(o => o.DataDirectory = dir);
            using var host = builder.Build();
            try {
                await host.StartAsync(TestContext.Current.CancellationToken);
                await state.Started.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            } finally {
                await host.StopAsync(CancellationToken.None);
            }

            state.StopFailure.Should().BeNull();
            await using (var reopened = await EmbeddedMsspClient.OpenAsync(dir)) {
                var events = await reopened.ReadAsync("probe-stream").ToListAsync();
                events.Should().Contain(e => e.EventType == "stopped");
            }
        } finally {
            DeleteDirectory(dir);
        }
    }

    [Fact]
    public async Task Client_Throws_AfterTheHostStopped() {
        var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddMssp(o => o.DataDirectory = dir);
            using var host = builder.Build();
            await host.StartAsync(TestContext.Current.CancellationToken);
            await host.StopAsync(CancellationToken.None);
            var client = host.Services.GetRequiredService<IMsspClient>();

            var act = async () => await client.AppendAsync("s", StreamRevision.Any, [Event("e")]);

            await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("IMsspClient is not available before the host has started.");
        } finally {
            DeleteDirectory(dir);
        }
    }

    [Fact]
    public async Task StartAsync_WithoutHost_OpensTheStore_AndStopAsyncClosesIt() {
        var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
            await using var service = new MsspHostedService(new MsspOptions { DataDirectory = dir });
            await service.StartAsync(CancellationToken.None);
            service.Client.Should().NotBeNull();
            await service.StopAsync(CancellationToken.None);

            var act = () => service.Client;

            act.Should().Throw<InvalidOperationException>().WithMessage("IMsspClient is not available before the host has started.");
        } finally {
            DeleteDirectory(dir);
        }
    }

    [Fact]
    public async Task StartAsync_AfterStartingAsync_KeepsTheSameClient() {
        var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
            await using var service = new MsspHostedService(new MsspOptions { DataDirectory = dir });
            await service.StartingAsync(CancellationToken.None);
            var client = service.Client;
            await service.StartAsync(CancellationToken.None);

            service.Client.Should().BeSameAs(client);
        } finally {
            DeleteDirectory(dir);
        }
    }

    [Fact]
    public async Task StopAsync_AfterStartingAsync_KeepsTheStoreOpenUntilStoppedAsync() {
        var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
            await using var service = new MsspHostedService(new MsspOptions { DataDirectory = dir });
            await service.StartingAsync(CancellationToken.None);
            await service.StopAsync(CancellationToken.None);

            var act = () => service.Client;

            act.Should().NotThrow();

            await service.StoppedAsync(CancellationToken.None);

            var actStopped = () => service.Client;

            actStopped.Should().Throw<InvalidOperationException>().WithMessage("IMsspClient is not available before the host has started.");
        } finally {
            DeleteDirectory(dir);
        }
    }

    [Fact]
    public async Task DisposeAsync_AfterStoppedAsync_DoesNotThrow() {
        var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
            await using var service = new MsspHostedService(new MsspOptions { DataDirectory = dir });
            await service.StartingAsync(CancellationToken.None);
            await service.StoppedAsync(CancellationToken.None);

            var act = async () => await service.DisposeAsync();

            await act.Should().NotThrowAsync();
        } finally {
            DeleteDirectory(dir);
        }
    }

    internal static EventData Event(string type) =>
        new(type, "{}"u8.ToArray());

    static void DeleteDirectory(string dir) {
        if (Directory.Exists(dir))
            Directory.Delete(dir, recursive: true);
    }
}

sealed class ProbeState {
    public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Exception? StopFailure { get; set; }
}

sealed class AppendingProbe(IMsspClient client, ProbeState state) : BackgroundService {
    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        try {
            await client.AppendAsync("probe-stream", StreamRevision.Any, [MsspHostingTests.Event("started")], stoppingToken);
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
            await client.AppendAsync("probe-stream", StreamRevision.Any, [MsspHostingTests.Event("stopped")], CancellationToken.None);
        } catch (Exception ex) {
            state.StopFailure = ex;
        }
    }
}
