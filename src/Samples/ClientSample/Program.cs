using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MSSP;
using MSSP.Client;

// Required for gRPC over plain HTTP/2 (matching ServerSample's Kestrel config).
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddMssp(o => {
    o.Address = new Uri("http://localhost:5000");
});

var host = builder.Build();
await host.StartAsync();

var client = host.Services.GetRequiredService<IMsspClient>();
var streamId = new StreamId("greetings-1");

EventData[] newEvents = [
    new("GreetingSent", """{"from":"ClientSample","text":"Hello from the gRPC client!"}"""u8.ToArray()),
];

// Start by asserting the stream must not yet exist (NoStream).
// On subsequent runs the stream already has events: MSSP throws OptimisticConcurrencyException.
// We catch it, read the current tail revision, and retry â€” appending after the last known event.
Console.WriteLine($"Appending event to '{streamId}' on the remote server...");
var expectedRevision = StreamRevision.NoStream;

while (true) {
    try {
        await client.AppendAsync(streamId, expectedRevision, newEvents);
        Console.WriteLine("Append succeeded.");
        break;
    } catch (OptimisticConcurrencyException ex) {
        Console.WriteLine($"Concurrency conflict: {ex.Message}");
        Console.WriteLine("Reading current tail revision and retrying...");

        ulong tail = 0;
        await foreach (var e in client.ReadAsync(streamId))
            tail = e.Revision;

        expectedRevision = tail;  // implicit conversion from ulong to StreamRevision
    }
}

// Read the full stream back from the server.
Console.WriteLine("\nReading all events in stream:");
await foreach (var e in client.ReadAsync(streamId)) {
    Console.WriteLine($"  rev={e.Revision}  {e.EventType}  {System.Text.Encoding.UTF8.GetString(e.Data.Span)}");
}

await host.StopAsync();
