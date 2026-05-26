namespace MSSP.Cluster;

/// <summary>
/// An opaque <see cref="System.Net.Http.HttpMessageHandler"/> that wraps a <see cref="System.Net.Http.SocketsHttpHandler"/>
/// via an <see cref="System.Net.Http.HttpMessageInvoker"/> without exposing the underlying handler type.
/// </summary>
/// <remarks>
/// <para>
/// When grpc-dotnet detects a <see cref="System.Net.Http.SocketsHttpHandler"/> (or a
/// <see cref="System.Net.Http.DelegatingHandler"/> that wraps one) it selects
/// <c>SocketConnectivitySubchannelTransport</c>, which opens a raw TCP monitoring socket to the server
/// purely for connectivity probing. This raw socket connects to the same port but never sends an
/// HTTP/2 connection preface.
/// </para>
/// <para>
/// When the server (Kestrel) is configured for <c>HttpProtocols.Http2</c> (prior-knowledge HTTP/2 only),
/// it expects the preface within a short timeout. Because the monitoring socket never sends it,
/// Kestrel closes the connection; the transport interprets this as a connectivity failure, calls
/// <c>Reset()</c>, and cancels any in-flight gRPC call with
/// <c>StatusCode.Cancelled / "gRPC call disposed."</c>.
/// </para>
/// <para>
/// By presenting a handler that is not a <see cref="System.Net.Http.SocketsHttpHandler"/>,
/// grpc-dotnet falls back to <c>PassiveSubchannelTransport</c>, which opens no extra socket.
/// </para>
/// </remarks>
sealed class OpaqueInvokerHandler(HttpMessageInvoker invoker, SocketsHttpHandler inner) : HttpMessageHandler {

    /// <inheritdoc/>
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
        invoker.SendAsync(request, cancellationToken);

    /// <inheritdoc/>
    protected override void Dispose(bool disposing) {
        if (disposing) { invoker.Dispose(); inner.Dispose(); }
        base.Dispose(disposing);
    }
}
