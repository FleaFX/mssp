using Microsoft.Extensions.DependencyInjection;
using MSSP.Embedded;
using System.Diagnostics.Metrics;

namespace MSSP.Cluster;

/// <summary>
/// Extends <see cref="MsspBuilder"/> with the Raft cluster layer.
/// </summary>
public static class MsspClusterBuilderExtensions {
    /// <summary>
    /// Adds the Raft consensus cluster to the MSSP service registrations.
    /// </summary>
    /// <remarks>
    /// Replaces the embedded <see cref="MsspHostedService"/> and <see cref="IMsspClient"/>
    /// registrations with their cluster equivalents: <see cref="RaftHostedService"/> and
    /// <see cref="ClusteredMsspClient"/>.
    /// </remarks>
    /// <param name="builder">The MSSP builder returned by <c>AddMssp()</c>.</param>
    /// <param name="configure">Delegate to configure <see cref="MsspClusterOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for further chaining.</returns>
    public static MsspBuilder AddCluster(this MsspBuilder builder, Action<MsspClusterOptions> configure) {
        var options = new MsspClusterOptions();
        configure(options);
        builder.Services.AddSingleton(options);

        // Replace the embedded registrations with cluster equivalents.
        // Also remove the IHostedService factory that AddMssp() registered to start MsspHostedService.
        var toRemove = builder.Services
            .Where(d =>
                d.ImplementationType == typeof(MsspHostedService) ||
                d.ServiceType == typeof(EmbeddedMsspClient) ||
                d.ServiceType == typeof(IMsspClient))
            .ToList();
        if (builder.EmbeddedHostedServiceDescriptor is not null)
            toRemove.Add(builder.EmbeddedHostedServiceDescriptor);
        foreach (var d in toRemove)
            builder.Services.Remove(d);

        builder.Services.AddSingleton<RaftHostedService>(sp => new RaftHostedService(
            sp.GetRequiredService<MsspOptions>(),
            sp.GetRequiredService<MsspClusterOptions>(),
            sp.GetService<IMeterFactory>()));
        builder.Services.AddSingleton<EmbeddedMsspClient>(sp => sp.GetRequiredService<RaftHostedService>().Local);
        builder.Services.AddSingleton<IMsspClient>(sp => sp.GetRequiredService<RaftHostedService>().Client);
        builder.Services.AddHostedService(sp => sp.GetRequiredService<RaftHostedService>());

        return builder;
    }
}
