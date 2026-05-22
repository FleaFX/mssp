using FluentAssertions;

namespace MSSP.Embedded;

public class EmbeddedMsspClientTests : IAsyncLifetime {
    readonly string _dataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    EmbeddedMsspClient _client = null!;

    public async ValueTask InitializeAsync() => _client = await EmbeddedMsspClient.OpenAsync(_dataDir);

    public ValueTask DisposeAsync() {
        _client.Dispose();
        Directory.Delete(_dataDir, recursive: true);
        return ValueTask.CompletedTask;
    }

    static EventData Event(string type, string payload) =>
        new(type, System.Text.Encoding.UTF8.GetBytes(payload));

    public class AppendAsync : EmbeddedMsspClientTests {
        [Fact]
        public async Task NoStream_OnNewStream_Succeeds() {
            var act = async () => await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")]);

            await act.Should().NotThrowAsync();
        }

        [Fact]
        public async Task NoStream_OnExistingStream_ThrowsOptimisticConcurrencyException() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")]);

            var act = async () => await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")]);

            await act.Should().ThrowAsync<OptimisticConcurrencyException>();
        }

        [Fact]
        public async Task Any_AlwaysSucceeds() {
            await _client.AppendAsync("stream-a", StreamRevision.Any, [Event("Foo", "first")]);

            var act = async () => await _client.AppendAsync("stream-a", StreamRevision.Any, [Event("Foo", "second")]);

            await act.Should().NotThrowAsync();
        }

        [Fact]
        public async Task StreamExists_OnExistingStream_Succeeds() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")]);

            var act = async () => await _client.AppendAsync("stream-a", StreamRevision.StreamExists, [Event("Bar", "data")]);

            await act.Should().NotThrowAsync();
        }

        [Fact]
        public async Task StreamExists_OnNewStream_ThrowsOptimisticConcurrencyException() {
            var act = async () => await _client.AppendAsync("stream-a", StreamRevision.StreamExists, [Event("Foo", "data")]);

            await act.Should().ThrowAsync<OptimisticConcurrencyException>();
        }

        [Fact]
        public async Task SpecificRevision_WhenMatches_Succeeds() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")]);

            var act = async () => await _client.AppendAsync("stream-a", 0UL, [Event("Bar", "data")]);

            await act.Should().NotThrowAsync();
        }

        [Fact]
        public async Task SpecificRevision_WhenMismatch_ThrowsOptimisticConcurrencyException() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")]);

            var act = async () => await _client.AppendAsync("stream-a", 5UL, [Event("Bar", "data")]);

            await act.Should().ThrowAsync<OptimisticConcurrencyException>();
        }

        [Fact]
        public async Task MultipleEvents_AssignsSequentialRevisions() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second"),
                Event("Baz", "third")
            ]);

            var events = await _client.ReadAsync("stream-a").ToListAsync();

            events.Should().HaveCount(3);
            events[0].Revision.Should().Be(0);
            events[1].Revision.Should().Be(1);
            events[2].Revision.Should().Be(2);
        }
    }

    public class ReadAsync : EmbeddedMsspClientTests {
        [Fact]
        public async Task EmptyStream_ReturnsNoEvents() {
            var events = await _client.ReadAsync("stream-a").ToListAsync();

            events.Should().BeEmpty();
        }

        [Fact]
        public async Task ReturnsEventsInRevisionOrder() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second")
            ]);

            var events = await _client.ReadAsync("stream-a").ToListAsync();

            events.Should().HaveCount(2);
            events[0].EventType.Should().Be("Foo");
            events[1].EventType.Should().Be("Bar");
        }

        [Fact]
        public async Task FromRevision_SkipsEarlierEvents() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second"),
                Event("Baz", "third")
            ]);

            var events = await _client.ReadAsync("stream-a", 1UL).ToListAsync();

            events.Should().HaveCount(2);
            events[0].EventType.Should().Be("Bar");
            events[1].EventType.Should().Be("Baz");
        }

        [Fact]
        public async Task DoesNotReturnEventsFromOtherStreams() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "a")]);
            await _client.AppendAsync("stream-b", StreamRevision.NoStream, [Event("Bar", "b")]);

            var events = await _client.ReadAsync("stream-a").ToListAsync();

            events.Should().HaveCount(1);
            events[0].EventType.Should().Be("Foo");
        }

        [Fact]
        public async Task PreservesEventTypeAndPayload() {
            var payload = "hello world"u8.ToArray();
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [new EventData("MyEvent", payload)]);

            var events = await _client.ReadAsync("stream-a").ToListAsync();

            events[0].EventType.Should().Be("MyEvent");
            events[0].Data.ToArray().Should().Equal(payload);
        }

        [Fact]
        public async Task ReadBackwards_ReturnsEventsInReverseRevisionOrder() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second"),
                Event("Baz", "third")
            ]);

            var events = await _client.ReadAsync("stream-a", direction: ReadDirection.Backwards).ToListAsync();

            events.Should().HaveCount(3);
            events[0].EventType.Should().Be("Baz");
            events[1].EventType.Should().Be("Bar");
            events[2].EventType.Should().Be("Foo");
        }

        [Fact]
        public async Task ReadBackwards_FromRevision_StartsFromSpecifiedRevision() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second"),
                Event("Baz", "third")
            ]);

            var events = await _client.ReadAsync("stream-a", 1UL, ReadDirection.Backwards).ToListAsync();

            events.Should().HaveCount(2);
            events[0].EventType.Should().Be("Baz");
            events[1].EventType.Should().Be("Bar");
        }

        [Fact]
        public async Task ReadForwards_Explicitly_ReturnsEventsInRevisionOrder() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second")
            ]);

            var events = await _client.ReadAsync("stream-a", direction: ReadDirection.Forwards).ToListAsync();

            events.Should().HaveCount(2);
            events[0].EventType.Should().Be("Foo");
            events[1].EventType.Should().Be("Bar");
        }
    }

    public class Flush : IAsyncLifetime {
        readonly string _dataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        EmbeddedMsspClient _tinyClient = null!;

        public async ValueTask InitializeAsync() => _tinyClient = await EmbeddedMsspClient.OpenAsync(_dataDir, memTableCapacityBytes: 128);

        public ValueTask DisposeAsync() {
            _tinyClient.Dispose();
            Directory.Delete(_dataDir, recursive: true);
            return ValueTask.CompletedTask;
        }

        static EventData Event(string type, string payload) =>
            new(type, System.Text.Encoding.UTF8.GetBytes(payload));

        [Fact]
        public async Task EventsRemainingReadableAfterFlush() {
            await _tinyClient.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", new string('x', 64)),
                Event("Bar", new string('x', 64))
            ]);

            var events = await _tinyClient.ReadAsync("stream-a").ToListAsync();

            events.Should().HaveCount(2);
            events[0].EventType.Should().Be("Foo");
            events[1].EventType.Should().Be("Bar");
        }

        [Fact]
        public async Task SstFileCreatedAfterFlush() {
            await _tinyClient.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", new string('x', 64)),
                Event("Bar", new string('x', 64))
            ]);

            Directory.EnumerateFiles(_dataDir, "*.sst").Should().HaveCount(1);
        }

        [Fact]
        public async Task EventsSpanningMultipleSstFilesAndMemTable_ReadInOrder() {
            for (var i = 0; i < 6; i++)
                await _tinyClient.AppendAsync("stream-a", i == 0 ? StreamRevision.NoStream : (ulong)(i - 1), [
                    Event($"Event{i}", new string('x', 32))
                ]);

            var events = await _tinyClient.ReadAsync("stream-a").ToListAsync();

            events.Should().HaveCount(6);
            for (var i = 0; i < 6; i++)
                events[i].EventType.Should().Be($"Event{i}");
        }
    }

    public class Recovery {
        static EventData Event(string type, string payload) =>
            new(type, System.Text.Encoding.UTF8.GetBytes(payload));

        [Fact]
        public async Task EventsReadableAfterReopen() {
            var dataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            try {
                var client1 = await EmbeddedMsspClient.OpenAsync(dataDir);
                await client1.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "first"), Event("Bar", "second")]);
                client1.Dispose();

                var client2 = await EmbeddedMsspClient.OpenAsync(dataDir);
                var events = await client2.ReadAsync("stream-a").ToListAsync();
                client2.Dispose();

                events.Should().HaveCount(2);
                events[0].EventType.Should().Be("Foo");
                events[1].EventType.Should().Be("Bar");
            } finally {
                Directory.Delete(dataDir, recursive: true);
            }
        }

        [Fact]
        public async Task StreamRevisionRestoredAfterReopen() {
            var dataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            try {
                var client1 = await EmbeddedMsspClient.OpenAsync(dataDir);
                await client1.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")]);
                client1.Dispose();

                var client2 = await EmbeddedMsspClient.OpenAsync(dataDir);
                var act = async () => await client2.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Bar", "data")]);
                await act.Should().ThrowAsync<OptimisticConcurrencyException>();
                client2.Dispose();
            } finally {
                Directory.Delete(dataDir, recursive: true);
            }
        }

        [Fact]
        public async Task EventsReadableAfterReopenFollowingFlush() {
            var dataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            try {
                var client1 = await EmbeddedMsspClient.OpenAsync(dataDir, memTableCapacityBytes: 128);
                await client1.AppendAsync("stream-a", StreamRevision.NoStream, [
                    Event("Foo", new string('x', 64)),
                    Event("Bar", new string('x', 64))
                ]);
                client1.Dispose();

                var client2 = await EmbeddedMsspClient.OpenAsync(dataDir, memTableCapacityBytes: 128);
                var events = await client2.ReadAsync("stream-a").ToListAsync();
                client2.Dispose();

                events.Should().HaveCount(2);
                events[0].EventType.Should().Be("Foo");
                events[1].EventType.Should().Be("Bar");
            } finally {
                Directory.Delete(dataDir, recursive: true);
            }
        }
    }
}
