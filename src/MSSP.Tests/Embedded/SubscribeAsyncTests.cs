using FluentAssertions;
using System.Text.RegularExpressions;

namespace MSSP.Embedded;

public class SubscribeAsyncTests {
    static EventData Event(string type, string payload) =>
        new(type, System.Text.Encoding.UTF8.GetBytes(payload));

    static async Task<(EmbeddedMsspClient Client, string DataDir)> CreateClientAsync(
        SubscriptionLogFormat format = SubscriptionLogFormat.FullPayload) {

        var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(dir);
        var client = await EmbeddedMsspClient.OpenAsync(dir, subscriptionLogFormat: format);
        return (client, dir);
    }

    static async Task<List<SubscriptionEvent>> CollectAsync(
        IAsyncEnumerable<SubscriptionEvent> source,
        int count,
        TimeSpan? timeout = null) {

        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(5));
        var result = new List<SubscriptionEvent>();
        await foreach (var evt in source.WithCancellation(cts.Token))
            if (result.Count < count) {
                result.Add(evt);
                if (result.Count == count) break;
            }
        return result;
    }

    [Theory]
    [InlineData(SubscriptionLogFormat.FullPayload)]
    [InlineData(SubscriptionLogFormat.ReferenceOnly)]
    public async Task FromStart_YieldsAllHistoricalEvents(SubscriptionLogFormat format) {
        var (client, dir) = await CreateClientAsync(format);
        try {
            await client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("A", "1"), Event("B", "2"), Event("C", "3")]);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var events = await CollectAsync(client.SubscribeAsync(SubscriptionFilter.All, cancellationToken: cts.Token), 3);

            events.Should().HaveCount(3);
            events.Select(e => e.EventType).Should().Equal("A", "B", "C");
        } finally {
            client.Dispose();
            Directory.Delete(dir, recursive: true);
        }
    }

    [Theory]
    [InlineData(SubscriptionLogFormat.FullPayload)]
    [InlineData(SubscriptionLogFormat.ReferenceOnly)]
    public async Task FromPosition_SkipsEarlierEvents(SubscriptionLogFormat format) {
        var (client, dir) = await CreateClientAsync(format);
        try {
            await client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("A", "1"), Event("B", "2")]);
            await client.AppendAsync("stream-a", (StreamRevision)1UL, [Event("C", "3"), Event("D", "4"), Event("E", "5")]);

            // Subscribe from position 3 — should skip the first two events
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var events = await CollectAsync(
                client.SubscribeAsync(SubscriptionFilter.All, fromPosition: new GlobalPosition(3), cancellationToken: cts.Token),
                3);

            events.Should().HaveCount(3);
            events.Select(e => e.EventType).Should().Equal("C", "D", "E");
        } finally {
            client.Dispose();
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task FilterForStream_YieldsOnlyMatchingStream() {
        var (client, dir) = await CreateClientAsync();
        try {
            await client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("InA", "x")]);
            await client.AppendAsync("stream-b", StreamRevision.NoStream, [Event("InB", "x")]);
            await client.AppendAsync("stream-a", (StreamRevision)0UL, [Event("InA2", "x")]);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var events = await CollectAsync(
                client.SubscribeAsync(SubscriptionFilter.ForStream("stream-a"), cancellationToken: cts.Token),
                2);

            events.Should().HaveCount(2);
            events.Select(e => e.EventType).Should().Equal("InA", "InA2");
            events.All(e => e.StreamId.Value == "stream-a").Should().BeTrue();
        } finally {
            client.Dispose();
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task FilterForStreamPrefix_YieldsMatchingStreams() {
        var (client, dir) = await CreateClientAsync();
        try {
            await client.AppendAsync("order-1", StreamRevision.NoStream, [Event("Placed", "x")]);
            await client.AppendAsync("customer-1", StreamRevision.NoStream, [Event("Created", "x")]);
            await client.AppendAsync("order-2", StreamRevision.NoStream, [Event("Placed", "x")]);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var events = await CollectAsync(
                client.SubscribeAsync(SubscriptionFilter.ForStreamPrefix("order-"), cancellationToken: cts.Token),
                2);

            events.Should().HaveCount(2);
            events.All(e => e.StreamId.Value.StartsWith("order-")).Should().BeTrue();
        } finally {
            client.Dispose();
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task FilterForEventTypePattern_YieldsMatchingTypes() {
        var (client, dir) = await CreateClientAsync();
        try {
            await client.AppendAsync("s", StreamRevision.NoStream, [Event("OrderPlaced", "x"), Event("CustomerCreated", "x"), Event("OrderShipped", "x")]);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var events = await CollectAsync(
                client.SubscribeAsync(SubscriptionFilter.ForEventTypePattern(new Regex("^Order")), cancellationToken: cts.Token),
                2);

            events.Should().HaveCount(2);
            events.Select(e => e.EventType).Should().Equal("OrderPlaced", "OrderShipped");
        } finally {
            client.Dispose();
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task AndFilter_CombinesConditions() {
        var (client, dir) = await CreateClientAsync();
        try {
            await client.AppendAsync("order-1", StreamRevision.NoStream, [Event("OrderPlaced", "x"), Event("OrderShipped", "x")]);
            await client.AppendAsync("customer-1", StreamRevision.NoStream, [Event("OrderPlaced", "x")]);

            var filter = SubscriptionFilter.ForStreamPrefix("order-")
                .And(SubscriptionFilter.ForEventType("OrderPlaced"));

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var events = await CollectAsync(client.SubscribeAsync(filter, cancellationToken: cts.Token), 1);

            events.Should().HaveCount(1);
            events[0].EventType.Should().Be("OrderPlaced");
            events[0].StreamId.Value.Should().Be("order-1");
        } finally {
            client.Dispose();
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task LiveEvent_IsReceivedAfterCatchUp() {
        var (client, dir) = await CreateClientAsync();
        try {
            await client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Historical", "x")]);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var subscription = client.SubscribeAsync(SubscriptionFilter.All, cancellationToken: cts.Token);
            var collectTask = CollectAsync(subscription, 2);

            // Give the subscription a moment to finish catch-up before writing the live event
            await Task.Delay(50);
            await client.AppendAsync("stream-a", (StreamRevision)0UL, [Event("Live", "x")]);

            var events = await collectTask;

            events.Should().HaveCount(2);
            events[0].EventType.Should().Be("Historical");
            events[1].EventType.Should().Be("Live");
        } finally {
            client.Dispose();
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task NoDuplicatesAtCatchUpToLiveBoundary() {
        var (client, dir) = await CreateClientAsync();
        try {
            await client.AppendAsync("s", StreamRevision.NoStream, [Event("A", "1"), Event("B", "2"), Event("C", "3")]);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var subscription = client.SubscribeAsync(SubscriptionFilter.All, cancellationToken: cts.Token);

            // Write live events concurrently with the subscription starting
            var collectTask = CollectAsync(subscription, 5);
            await client.AppendAsync("s", (StreamRevision)2UL, [Event("D", "4"), Event("E", "5")]);

            var events = await collectTask;

            events.Should().HaveCount(5);
            events.Select(e => e.Position.Value).Should().BeInAscendingOrder();
            events.Select(e => e.EventType).Should().OnlyHaveUniqueItems();
        } finally {
            client.Dispose();
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task MultipleSubscribers_EachReceiveLiveEvents() {
        var (client, dir) = await CreateClientAsync();
        try {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var sub1 = CollectAsync(client.SubscribeAsync(SubscriptionFilter.All, cancellationToken: cts.Token), 1);
            var sub2 = CollectAsync(client.SubscribeAsync(SubscriptionFilter.All, cancellationToken: cts.Token), 1);

            await client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Broadcast", "x")]);

            var events1 = await sub1;
            var events2 = await sub2;

            events1.Should().HaveCount(1);
            events2.Should().HaveCount(1);
            events1[0].EventType.Should().Be("Broadcast");
            events2[0].EventType.Should().Be("Broadcast");
        } finally {
            client.Dispose();
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task GlobalPositionsAreStrictlyIncreasing() {
        var (client, dir) = await CreateClientAsync();
        try {
            await client.AppendAsync("s1", StreamRevision.NoStream, [Event("A", "x"), Event("B", "x")]);
            await client.AppendAsync("s2", StreamRevision.NoStream, [Event("C", "x")]);
            await client.AppendAsync("s1", (StreamRevision)1UL, [Event("D", "x")]);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var events = await CollectAsync(client.SubscribeAsync(SubscriptionFilter.All, cancellationToken: cts.Token), 4);

            var positions = events.Select(e => e.Position.Value).ToList();
        for (int i = 1; i < positions.Count; i++)
            positions[i].Should().BeGreaterThan(positions[i - 1]);
        } finally {
            client.Dispose();
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task GlobalSequenceInitializedCorrectlyOnReopen() {
        var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(dir);

        // Write events and close
        ulong lastPos;
        {
            var client = await EmbeddedMsspClient.OpenAsync(dir, cancellationToken: TestContext.Current.CancellationToken);
            await client.AppendAsync("s", StreamRevision.NoStream, [Event("A", "x"), Event("B", "x"), Event("C", "x")]);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var events = await CollectAsync(client.SubscribeAsync(SubscriptionFilter.All, cancellationToken: cts.Token), 3);
            lastPos = events.Last().Position.Value;
            client.Dispose();
        }

        // Reopen and write more events — positions must be higher than before
        {
            var client = await EmbeddedMsspClient.OpenAsync(dir, cancellationToken: TestContext.Current.CancellationToken);
            await client.AppendAsync("s", (StreamRevision)2UL, [Event("D", "x")]);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var events = await CollectAsync(
                client.SubscribeAsync(SubscriptionFilter.All, fromPosition: new GlobalPosition(lastPos + 1), cancellationToken: cts.Token),
                1);

            events.Should().HaveCount(1);
            events[0].Position.Value.Should().BeGreaterThan(lastPos);
            client.Dispose();
        }

        Directory.Delete(dir, recursive: true);
    }

    [Fact]
    public async Task Dispose_CompletesActiveLiveSubscriptions() {
        var (client, dir) = await CreateClientAsync();
        try {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var subscription = client.SubscribeAsync(SubscriptionFilter.All, cancellationToken: cts.Token);

            var drainTask = Task.Run(async () => {
                var events = new List<SubscriptionEvent>();
                await foreach (var evt in subscription)
                    events.Add(evt);
                return events;
            });

            // Let the subscription enter the live phase
            await Task.Delay(50);

            // Dispose should complete all active channels
            client.Dispose();

            // The drain task should complete without hanging
            var completedInTime = await Task.WhenAny(drainTask, Task.Delay(2000)) == drainTask;
            completedInTime.Should().BeTrue("subscription drain should complete after client disposal");
        } finally {
            try { Directory.Delete(dir, recursive: true); } catch { /* already cleaned up */ }
        }
    }

    [Fact]
    public async Task SubscriptionLog_SurvivedReopen() {
        var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(dir);

        try {
            // Write events and close
            {
                var client = await EmbeddedMsspClient.OpenAsync(dir, cancellationToken: TestContext.Current.CancellationToken);
                await client.AppendAsync("s", StreamRevision.NoStream, [Event("A", "x"), Event("B", "x")]);
                client.Dispose();
            }

            // Reopen — catch-up should replay events from the subscription log
            {
                var client = await EmbeddedMsspClient.OpenAsync(dir, cancellationToken: TestContext.Current.CancellationToken);

                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                var events = await CollectAsync(client.SubscribeAsync(SubscriptionFilter.All, cancellationToken: cts.Token), 2);

                events.Should().HaveCount(2);
                events.Select(e => e.EventType).Should().Equal("A", "B");
                client.Dispose();
            }
        } finally {
            Directory.Delete(dir, recursive: true);
        }
    }
}
