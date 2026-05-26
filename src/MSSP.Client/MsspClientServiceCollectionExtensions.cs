using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using MsspGrpcClient = MSSP.Grpc.Mssp.MsspClient;

namespace MSSP.Client;

/// <summary>
/// Extension methods for registering the MSSP remote client with an <see cref="IServiceCollection"/>.
/// </summary>
public static class MsspClientServiceCollectionExtensions {
    /// <summary>
    /// Registers a remote <see cref="IMsspClient"/> that communicates with an MSSP server over gRPC.
    /// </summary>
    /// <param name="services">
    /// The <see cref="IServiceCollection"/> to add the services to.
    /// </param>
    /// <param name="configure">
    /// A delegate that configures the <see cref="MsspClientOptions"/>.
    /// </param>
    public static MsspBuilder AddMssp(this IServiceCollection services, Action<MsspClientOptions> configure) {
        var options = new MsspClientOptions();
        configure(options);
        services.AddSingleton(_ => GrpcChannel.ForAddress(options.Address, new GrpcChannelOptions {
            HttpHandler = new OpaqueHttpHandler(new SocketsHttpHandler()),
            DisposeHttpClient = true,
        }));
        services.AddSingleton<IMsspClient>(sp => new RemoteMsspClient(new MsspGrpcClient(sp.GetRequiredService<GrpcChannel>())));
        return new MsspBuilder(services);
    }
}
