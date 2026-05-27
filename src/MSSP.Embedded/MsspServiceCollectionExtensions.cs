using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Diagnostics.Metrics;

namespace MSSP.Embedded;

/// <summary>
/// Extension methods for registering MSSP with an <see cref="IServiceCollection"/>.
/// </summary>
public static class MsspServiceCollectionExtensions {
    /// <summary>
    /// Registers the embedded MSSP event store as a singleton <see cref="IMsspClient"/>.
    /// The store is opened when the host starts and closed when it stops.
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
        services.AddSingleton<IMsspClient>(sp => sp.GetRequiredService<EmbeddedMsspClient>());
        var hostedServiceDescriptor = ServiceDescriptor.Singleton<IHostedService>(
            sp => sp.GetRequiredService<MsspHostedService>());
        services.Add(hostedServiceDescriptor);
        return new MsspBuilder(services) { EmbeddedHostedServiceDescriptor = hostedServiceDescriptor };
    }
}
