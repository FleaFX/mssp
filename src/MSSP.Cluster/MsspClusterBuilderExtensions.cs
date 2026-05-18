using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MSSP.Embedded;

namespace MSSP.Cluster;

public static class MsspClusterBuilderExtensions {
    public static MsspBuilder AddCluster(this MsspBuilder builder, Action<MsspClusterOptions> configure) {
        var options = new MsspClusterOptions();
        configure(options);
        builder.Services.AddSingleton(options);

        // Replace the embedded registrations with cluster equivalents
        var toRemove = builder.Services
            .Where(d =>
                d.ImplementationType == typeof(MsspHostedService) ||
                d.ServiceType == typeof(IMsspClient))
            .ToList();
        foreach (var d in toRemove)
            builder.Services.Remove(d);

        builder.Services.AddSingleton<RaftHostedService>();
        builder.Services.AddSingleton<ClusteredMsspClient>();
        builder.Services.AddSingleton<IMsspClient>(sp => sp.GetRequiredService<ClusteredMsspClient>());
        builder.Services.AddHostedService(sp => sp.GetRequiredService<RaftHostedService>());

        return builder;
    }
}
