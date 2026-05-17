using Microsoft.Extensions.DependencyInjection;

namespace MSSP.Server;

/// <summary>
/// Extension methods for configuring the MSSP gRPC server on a <see cref="MsspBuilder"/>.
/// </summary>
public static class MsspServerBuilderExtensions {
    /// <summary>
    /// Registers the MSSP gRPC server. Call <c>app.MapGrpcService&lt;MsspGrpcService&gt;()</c>
    /// to expose the endpoint.
    /// </summary>
    /// <param name="builder">
    /// The <see cref="MsspBuilder"/> returned by <c>AddMssp()</c>.
    /// </param>
    public static MsspBuilder AddServer(this MsspBuilder builder) {
        builder.Services.AddGrpc();
        return builder;
    }
}
