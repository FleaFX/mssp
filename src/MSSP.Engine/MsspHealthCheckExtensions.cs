using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace MSSP.Engine;

/// <summary>
/// Extends <see cref="MsspBuilder"/> with an ASP.NET Core health check for the embedded store.
/// </summary>
public static class MsspHealthCheckExtensions {
    /// <summary>
    /// Registers a readiness health check that reports <c>Healthy</c> once the embedded store
    /// is open and ready to accept reads and writes, and <c>Unhealthy</c> until then.
    /// </summary>
    /// <remarks>
    /// Do not call this method in cluster mode. Use <c>AddClusterHealthChecks()</c> from
    /// <c>MSSP.Cluster</c> instead — <see cref="MsspHostedService"/> is not registered in
    /// cluster mode, so <see cref="EmbeddedStoreHealthCheck"/> cannot be resolved.
    /// </remarks>
    public static MsspBuilder AddHealthChecks(this MsspBuilder builder) {
        builder.Services.AddHealthChecks()
            .AddCheck<EmbeddedStoreHealthCheck>("mssp", tags: ["ready"]);
        return builder;
    }
}

/// <summary>
/// Health check that reports whether the embedded MSSP store is open and ready.
/// </summary>
internal sealed class EmbeddedStoreHealthCheck(MsspHostedService hostedService) : IHealthCheck {
    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) {
        try {
            _ = hostedService.Client;
            return Task.FromResult(HealthCheckResult.Healthy());
        } catch (InvalidOperationException ex) {
            return Task.FromResult(HealthCheckResult.Unhealthy(ex.Message));
        }
    }
}
