using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MSSP;
using MSSP.Client;

// Required for gRPC over plain HTTP/2 (h2c — "prior knowledge").
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddMssp(o => o.Address = new Uri("http://localhost:6001"));

var host = builder.Build();
await host.StartAsync();

var client = host.Services.GetRequiredService<IMsspClient>();
var streamId = new StreamId("greetings");

Console.WriteLine($"Appending to stream '{streamId}'...");
var expectedRevision = StreamRevision.NoStream;
var maxRetries = 20;
for (var attempt = 0; attempt < maxRetries; attempt++) {
    try {
        await client.AppendAsync(streamId, expectedRevision, [
            new EventData("GreetingSent", """{"text":"Hello from ClientSample!"}"""u8.ToArray())
        ]);
        Console.WriteLine($"  ✓ Appended (expected: {(long)expectedRevision})");
        break;
    } catch (OptimisticConcurrencyException) {
        Console.WriteLine($"  Stream already exists (OCE at expected={(long)expectedRevision}), retrying with Any...");
        expectedRevision = StreamRevision.Any;
    } catch (RpcException ex) when (ex.StatusCode is StatusCode.Unavailable or StatusCode.Cancelled) {
        Console.WriteLine($"  Server unavailable/cancelled ({ex.StatusCode}: {ex.Status.Detail}), retrying in 500ms...");
        await Task.Delay(500);
    }
}

Console.WriteLine("\nReading all events in stream:");
await foreach (var e in client.ReadAsync(streamId)) {
    Console.WriteLine($"  rev={e.Revision}  {e.EventType}  {System.Text.Encoding.UTF8.GetString(e.Data.Span)}");
}

await host.StopAsync();
