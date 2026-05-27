using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;

namespace MSSP.Embedded;

/// <summary>
/// Extends <see cref="MsspBuilder"/> with OpenTelemetry-compatible metrics via
/// <see cref="System.Diagnostics.Metrics"/>. 
/// </summary>
public static class MsspMetricsExtensions {
    /// <summary>
    /// Enables MSSP metrics. Registers <see cref="IMeterFactory"/> so that
    /// <c>MsspHostedService</c> and <c>RaftHostedService</c> can create meters.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Consumers add their own exporter separately, for example:
    /// <code>
    /// services.AddOpenTelemetry()
    ///     .WithMetrics(b => b
    ///         .AddMeter("MSSP.Storage")
    ///         .AddMeter("MSSP")
    ///         .AddMeter("MSSP.Cluster")
    ///         .AddPrometheusExporter());
    /// </code>
    /// </para>
    /// <para>
    /// Metrics are fully opt-in. If this method is not called, no metrics are
    /// collected and there is no performance overhead.
    /// </para>
    /// </remarks>
    public static MsspBuilder AddMetrics(this MsspBuilder builder) {
        builder.Services.AddMetrics();
        return builder;
    }
}
