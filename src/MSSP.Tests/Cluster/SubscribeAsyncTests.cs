using FluentAssertions;

namespace MSSP.Cluster;

public class SubscribeAsyncTests : IAsyncLifetime {
    InMemoryCluster _cluster = null!;

    public async Task InitializeAsync() => _cluster = await InMemoryCluster.CreateAsync();
    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    static EventData Event(string type, string payload) =>
        new(type, System.Text.Encoding.UTF8.GetBytes(payload));

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

    [Fact]
    public async Task Leader_CatchUp_YieldsHistoricalEvents() {
        var leader = await _cluster.WaitForLeaderAsync();
        await leader.Client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("A", "1"), Event("B", "2")]);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var events = await CollectAsync(
            leader.Client.SubscribeAsync(SubscriptionFilter.All, ct: cts.Token),
            2);

        events.Should().HaveCount(2);
        events.Select(e => e.EventType).Should().Equal("A", "B");
    }

    [Fact]
    public async Task Leader_LiveEvent_IsDelivered() {
        var leader = await _cluster.WaitForLeaderAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var collectTask = CollectAsync(leader.Client.SubscribeAsync(SubscriptionFilter.All, ct: cts.Token), 1);

        await Task.Delay(50);
        await leader.Client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("LiveEvent", "x")]);

        var events = await collectTask;

        events.Should().HaveCount(1);
        events[0].EventType.Should().Be("LiveEvent");
    }

    [Fact]
    public async Task Follower_ThrowsNotSupportedException() {
        var leader = await _cluster.WaitForLeaderAsync();
        var follower = _cluster.Nodes.First(h => h.Node != leader.Node);

        var act = async () => await follower.Client
            .SubscribeAsync(SubscriptionFilter.All)
            .GetAsyncEnumerator()
            .MoveNextAsync();

        await act.Should().ThrowAsync<NotSupportedException>();
    }
}
