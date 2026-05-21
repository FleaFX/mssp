namespace MSSP.Client;

/// <summary>
/// Configuration options for the remote MSSP gRPC client.
/// </summary>
public sealed class MsspClientOptions {
    /// <summary>
    /// The address of the MSSP server.
    /// </summary>
    public Uri Address { get; set; } = new("https://localhost:5001");
}
