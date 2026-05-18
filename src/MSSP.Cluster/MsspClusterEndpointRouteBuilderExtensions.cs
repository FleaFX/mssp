using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;

namespace MSSP.Cluster;

/// <summary>
/// Extends <see cref="IEndpointRouteBuilder"/> to register the Raft gRPC transport endpoint.
/// </summary>
public static class MsspClusterEndpointRouteBuilderExtensions {
    /// <summary>
    /// Maps the internal Raft gRPC service so cluster peers can reach this node's
    /// <c>RequestVote</c> and <c>AppendEntries</c> RPCs.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <returns>The same <paramref name="endpoints"/> for further chaining.</returns>
    public static IEndpointRouteBuilder UseCluster(this IEndpointRouteBuilder endpoints) {
        endpoints.MapGrpcService<RaftGrpcService>();
        return endpoints;
    }
}
