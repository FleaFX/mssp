using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Diagnostics.Metrics;

namespace MSSP.Engine;

/// <summary>
/// Extension methods for registering MSSP with an <see cref="IServiceCollection"/>.
/// </summary>
public static class MsspServiceCollectionExtensions {
    /// <summary>
    /// Registers the embedded MSSP event store as a singleton <see cref="IMsspClient"/>.
    /// The client can be resolved at any time, also in the constructor of a hosted service;
    /// a call made while the store is not open throws <see cref="InvalidOperationException"/>.
    /// The store opens when the host starts, before any hosted service starts, and closes after
    /// every hosted service has stopped, whatever order the services were registered in.
    /// </summary>
    /// <param name="services">
    /// The <see cref="IServiceCollection"/> to add the services to.
    /// </param>
    /// <param name="configure">
    /// A delegate that configures the <see cref="MsspOptions"/>.
    /// </param>
    public static MsspBuilder AddMssp(this IServiceCollection services, Action<MsspOptions> configure) {
        var options = new MsspOptions();
        configure(options);
        services.AddSingleton(options);
        services.AddSingleton<MsspHostedService>(sp => new MsspHostedService(
            sp.GetRequiredService<MsspOptions>(),
            sp.GetService<IMeterFactory>()));
        services.AddSingleton<EmbeddedMsspClient>(sp => sp.GetRequiredService<MsspHostedService>().Client);
        services.AddSingleton<IMsspClient>(sp => {
            var hostedService = sp.GetRequiredService<MsspHostedService>();
            return new HostedMsspClient(() => hostedService.Client);
        });
        var hostedServiceDescriptor = ServiceDescriptor.Singleton<IHostedService>(
            sp => sp.GetRequiredService<MsspHostedService>());
        services.Add(hostedServiceDescriptor);
        return new MsspBuilder(services) { EmbeddedHostedServiceDescriptor = hostedServiceDescriptor };
    }
}
