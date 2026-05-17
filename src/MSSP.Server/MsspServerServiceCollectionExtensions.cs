using Microsoft.Extensions.DependencyInjection;

namespace MSSP.Server;

/// <summary>
/// Extension methods for registering the MSSP gRPC server with an <see cref="IServiceCollection"/>.
/// </summary>
public static class MsspServerServiceCollectionExtensions {
    /// <summary>
    /// Registers the MSSP gRPC service. Call <c>app.MapGrpcService&lt;MsspGrpcService&gt;()</c>
    /// to expose the endpoint.
    /// </summary>
    /// <param name="services">
    /// The <see cref="IServiceCollection"/> to add the services to.
    /// </param>
    public static IServiceCollection AddMsspServer(this IServiceCollection services) {
        services.AddGrpc();
        return services;
    }
}
