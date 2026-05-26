namespace MSSP.Client;

/// <summary>
/// Wraps a <see cref="System.Net.Http.SocketsHttpHandler"/> in an opaque <see cref="System.Net.Http.HttpMessageHandler"/>
/// so that grpc-dotnet cannot detect the underlying handler type via reflection.
/// </summary>
/// <remarks>
/// <para>
/// When grpc-dotnet detects a <see cref="System.Net.Http.SocketsHttpHandler"/> (or a
/// <see cref="System.Net.Http.DelegatingHandler"/> that wraps one) it uses
/// <c>SocketConnectivitySubchannelTransport</c>, which opens a second raw TCP socket to the server
/// for connectivity monitoring. This raw socket connects to the same port as the gRPC channel but
/// never sends an HTTP/2 connection preface.
/// </para>
/// <para>
/// When the server is Kestrel configured for <c>HttpProtocols.Http2</c> (prior-knowledge HTTP/2
/// only), Kestrel expects the HTTP/2 preface within a short timeout. Because the monitoring socket
/// never sends it, Kestrel closes the connection after approximately one second. The transport
/// interprets this closure as a connectivity failure, resets the channel, and cancels any
/// in-flight gRPC calls with <c>StatusCode.Cancelled / "gRPC call disposed."</c>.
/// </para>
/// <para>
/// By presenting an opaque handler that is not recognized as a <see cref="System.Net.Http.SocketsHttpHandler"/>,
/// grpc-dotnet falls back to <c>PassThroughTransport</c>, which does not open a separate
/// monitoring socket. The actual HTTP/2 connection is still managed internally by the wrapped
/// <see cref="System.Net.Http.SocketsHttpHandler"/>.
/// </para>
/// </remarks>
sealed class OpaqueHttpHandler(SocketsHttpHandler inner) : HttpMessageHandler {
    readonly HttpMessageInvoker _invoker = new(inner, disposeHandler: false);
    
    /// <inheritdoc/>
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
        _invoker.SendAsync(request, cancellationToken);

    /// <inheritdoc/>
    protected override void Dispose(bool disposing) {
        if (disposing) {
            _invoker.Dispose();
            inner.Dispose();
        }
        base.Dispose(disposing);
    }
}
