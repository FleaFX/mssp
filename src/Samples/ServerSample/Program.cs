using Microsoft.AspNetCore.Server.Kestrel.Core;
using MSSP.Embedded;
using MSSP.Server;

var builder = WebApplication.CreateBuilder(args);

// Serve gRPC over plain HTTP/2 (no TLS) on port 5000.
// Plain HTTP/2 keeps the sample self-contained — no certificate setup needed.
builder.WebHost.ConfigureKestrel(options => {
    options.ListenLocalhost(5000, o => o.Protocols = HttpProtocols.Http2);
});

builder.Services
    .AddMssp(o => o.DataDirectory = "./mssp-data")
    .AddServer();   // registers the gRPC service + calls AddGrpc()

var app = builder.Build();
app.UseMssp();      // maps the MsspGrpcService endpoint

Console.WriteLine("MSSP server listening on http://localhost:5000");
Console.WriteLine("Start ClientSample in a separate terminal to interact with it.");
await app.RunAsync();
