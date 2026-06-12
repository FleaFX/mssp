using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MSSP;
using MSSP.Engine;

// Start a generic .NET host with the embedded MSSP store.
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddMssp(o => {
    o.DataDirectory = "./mssp-data";
});

var host = builder.Build();
await host.StartAsync();

var client = host.Services.GetRequiredService<IMsspClient>();
var streamId = new StreamId("orders-1");

EventData[] newEvents = [
    new("OrderPlaced",    """{"product":"Widget","qty":3}"""u8.ToArray()),
    new("OrderShipped",   """{"courier":"DHL","trackingId":"TRK-001"}"""u8.ToArray()),
    new("OrderDelivered", """{"deliveredAt":"2026-05-25T10:00:00Z"}"""u8.ToArray()),
];

// Start by asserting the stream must not yet exist (NoStream).
// On subsequent runs the stream already has events: MSSP throws OptimisticConcurrencyException.
// We catch it, read the current tail revision, and retry — appending after the last known event.
Console.WriteLine($"Appending 3 events to '{streamId}'...");
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

// Read the full stream back from the beginning.
Console.WriteLine("\nReading all events in stream:");
await foreach (var e in client.ReadAsync(streamId)) {
    Console.WriteLine($"  rev={e.Revision}  {e.EventType}  {System.Text.Encoding.UTF8.GetString(e.Data.Span)}");
}

// Subscribe to all events (catch-up from position 0, then live).
// Press Ctrl+C to stop.
Console.WriteLine("\nSubscribing to all streams (Ctrl+C to stop)...");
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

try {
    await foreach (var e in client.SubscribeAsync(SubscriptionFilter.All, cancellationToken: cts.Token)) {
        Console.WriteLine($"  pos={e.Position.Value}  {e.EventType}  stream={e.StreamId}");
    }
} catch (OperationCanceledException) { }

await host.StopAsync();
