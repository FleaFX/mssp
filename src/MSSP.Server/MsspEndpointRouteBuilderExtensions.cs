using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;

namespace MSSP.Server;

/// <summary>
/// Extension methods for mapping the MSSP gRPC endpoint on an <see cref="IEndpointRouteBuilder"/>.
/// </summary>
public static class MsspEndpointRouteBuilderExtensions {
    /// <summary>
    /// Maps the MSSP gRPC service endpoint.
    /// </summary>
    /// <param name="endpoints">
    /// The <see cref="IEndpointRouteBuilder"/> on which to map the endpoint.
    /// </param>
    public static IEndpointRouteBuilder UseMssp(this IEndpointRouteBuilder endpoints) {
        endpoints.MapGrpcService<MsspGrpcService>();
        return endpoints;
    }
}
