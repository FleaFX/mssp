using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;

namespace MSSP.Cluster;

public static class MsspClusterEndpointRouteBuilderExtensions {
    public static IEndpointRouteBuilder UseCluster(this IEndpointRouteBuilder endpoints) {
        endpoints.MapGrpcService<RaftGrpcService>();
        return endpoints;
    }
}
