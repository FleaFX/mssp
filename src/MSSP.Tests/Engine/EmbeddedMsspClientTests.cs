using FluentAssertions;
using MSSP.Engine.Storage;

namespace MSSP.Engine;

public class EmbeddedMsspClientTests : IAsyncLifetime {
    readonly string _dataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    EmbeddedMsspClient _client = null!;

    public async ValueTask InitializeAsync() => _client = await EmbeddedMsspClient.OpenAsync(_dataDir);

    public async ValueTask DisposeAsync() {
        await _client.DisposeAsync();
        Directory.Delete(_dataDir, recursive: true);
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
        public async Task PreservesMetadata() {
            var payload = "hello world"u8.ToArray();
            var meta = "{ \"userId\": 42 }"u8.ToArray();
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [new EventData("MyEvent", payload, meta)]);

            var events = await _client.ReadAsync("stream-a").ToListAsync();

            events[0].Metadata.ToArray().Should().Equal(meta);
        }

        [Fact]
        public async Task WithoutMetadata_ReturnsEmptySlice() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("MyEvent", "data")]);

            var events = await _client.ReadAsync("stream-a").ToListAsync();

            events[0].Metadata.IsEmpty.Should().BeTrue();
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
            events[0].EventType.Should().Be("Bar");  // revision 1
            events[1].EventType.Should().Be("Foo");  // revision 0
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

        [Fact]
        public async Task MaxCount_LimitsNumberOfEvents() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second"),
                Event("Baz", "third")
            ]);

            var events = await _client.ReadAsync("stream-a", maxCount: 2).ToListAsync();

            events.Should().HaveCount(2);
            events[0].EventType.Should().Be("Foo");
            events[1].EventType.Should().Be("Bar");
        }

        [Fact]
        public async Task MaxCount_WithBackwards_LimitsNumberOfEvents() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [
                Event("Foo", "first"),
                Event("Bar", "second"),
                Event("Baz", "third")
            ]);

            var events = await _client.ReadAsync("stream-a", direction: ReadDirection.Backwards, maxCount: 2).ToListAsync();

            events.Should().HaveCount(2);
            events[0].EventType.Should().Be("Baz");
            events[1].EventType.Should().Be("Bar");
        }
    }

    public class Flush : IAsyncLifetime {
        readonly string _dataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        EmbeddedMsspClient _tinyClient = null!;

        public async ValueTask InitializeAsync() => _tinyClient = await EmbeddedMsspClient.OpenAsync(_dataDir, memTableCapacityBytes: 128);

        public async ValueTask DisposeAsync() {
            await _tinyClient.DisposeAsync();
            Directory.Delete(_dataDir, recursive: true);
        }

        static EventData Event(string type, string payload) =>
            new(type, System.Text.Encoding.UTF8.GetBytes(payload));

        static async Task WaitForConditionAsync(Func<bool> condition, int timeoutMs = 5000) {
            var deadline = Environment.TickCount64 + timeoutMs;
            while (!condition()) {
                if (Environment.TickCount64 > deadline)
                    throw new TimeoutException("Condition was not met within the timeout.");
                await Task.Delay(10);
            }
        }

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

        [Fact]
        public async Task BurstWrites_AllEventsReadable() {
            for (var i = 0; i < 50; i++)
                await _tinyClient.AppendAsync("stream-burst", i == 0 ? StreamRevision.NoStream : (ulong)(i - 1), [
                    Event("E", new string('x', 64))
                ], TestContext.Current.CancellationToken);

            var events = await _tinyClient.ReadAsync("stream-burst", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

            events.Should().HaveCount(50);
        }
    }

    public class FlushSerialization {
        static EventData Event(string type, string payload) =>
            new(type, System.Text.Encoding.UTF8.GetBytes(payload));

        static async Task WaitForAsync(Func<bool> condition, int timeoutMs = 5000) {
            var deadline = Environment.TickCount64 + timeoutMs;
            while (!condition()) {
                if (Environment.TickCount64 > deadline)
                    throw new TimeoutException("Condition was not met within the timeout.");
                await Task.Delay(10);
            }
        }

        [Fact]
        public async Task SecondFlushDoesNotStartUntilFirstCompletes() {
            var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            var gate = new GatedSstAccess();
            var client = await EmbeddedMsspClient.OpenAsync(dir, memTableCapacityBytes: 128, sst: gate);
            try {
                var payload = new string('x', 64);

                // event0 fits in the MemTable; no flush yet.
                await client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("E0", payload)]);
                gate.WritesStarted.Should().Be(0);

                // event1 overflows → flush job1 seals {event0}; its SST write blocks on the gate.
                await client.AppendAsync("stream-a", 0UL, [Event("E1", payload)]);
                await WaitForAsync(() => gate.WritesStarted == 1);

                // event2 overflows → flush job2 seals {event1} and is queued, but must NOT start
                // its SST write while job1 is still in flight.
                await client.AppendAsync("stream-a", 1UL, [Event("E2", payload)]);
                gate.WritesStarted.Should().Be(1, "the second flush must wait for the first to complete");

                // Releasing job1 lets job2's write begin — proving the queue drains one at a time, in order.
                gate.Release(0);
                await WaitForAsync(() => gate.WritesStarted == 2);
                gate.Release(1);

                var events = await client.ReadAsync("stream-a").ToListAsync();
                events.Select(e => e.EventType).Should().Equal("E0", "E1", "E2");
            } finally {
                await client.DisposeAsync();
                Directory.Delete(dir, recursive: true);
            }
        }

        /// <summary>
        /// An <see cref="ISstAccess{TKey}"/> decorator that blocks each SST write until the test
        /// explicitly releases it, exposing the number of writes that have begun. Lets a test drive
        /// flush completion order deterministically instead of relying on disk timing.
        /// </summary>
        sealed class GatedSstAccess : ISstAccess<EventKey> {
            readonly ISstAccess<EventKey> _inner = DefaultSstAccess<EventKey>.Instance;
            readonly Lock _gate = new();
            readonly List<TaskCompletionSource> _releases = [];
            int _started;

            public int WritesStarted => Volatile.Read(ref _started);

            public ISstReader<EventKey> OpenReader(string sstPath) => _inner.OpenReader(sstPath);
            public void Delete(string sstPath) => _inner.Delete(sstPath);

            public async ValueTask WriteAsync(IEnumerable<KeyValuePair<EventKey, ReadOnlyMemory<byte>?>> entries, string sstPath, CancellationToken cancellationToken) {
                var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                lock (_gate)
                    _releases.Add(release);
                Interlocked.Increment(ref _started);
                await release.Task.WaitAsync(cancellationToken);
                await _inner.WriteAsync(entries, sstPath, cancellationToken);
            }

            public void Release(int index) {
                TaskCompletionSource release;
                lock (_gate)
                    release = _releases[index];
                release.SetResult();
            }
        }
    }

    public class Concurrency : EmbeddedMsspClientTests {
        [Fact]
        public async Task ConcurrentNoStreamAppends_ToSameStream_ExactlyOneSucceeds() {
            const int concurrency = 10;
            var exceptions = new Exception?[concurrency];

            await Task.WhenAll(Enumerable.Range(0, concurrency).Select(async i => {
                try {
                    await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "x")]);
                } catch (Exception ex) {
                    exceptions[i] = ex;
                }
            }));

            exceptions.Count(e => e is null).Should().Be(1, "exactly one concurrent NoStream append must succeed");
            exceptions.Count(e => e is OptimisticConcurrencyException).Should().Be(concurrency - 1);
        }
    }

    public class AfterDispose {
        static EventData Event(string type, string payload) =>
            new(type, System.Text.Encoding.UTF8.GetBytes(payload));

        [Fact]
        public async Task AppendAsync_ThrowsObjectDisposedException() {
            var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            var client = await EmbeddedMsspClient.OpenAsync(dir);
            await client.DisposeAsync();
            try { Directory.Delete(dir, recursive: true); } catch { }

            var act = async () => await client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "x")]);

            await act.Should().ThrowAsync<ObjectDisposedException>();
        }
    }

    public class ReloadSnapshot : EmbeddedMsspClientTests {
        [Fact]
        public async Task AfterReload_PositionAdvancesBeyondSnapshotPoint() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("A", "1"), Event("B", "2")]);
            var positionBeforeReload = _client.CurrentPosition;

            var stagingDir = Path.Combine(_dataDir, "snapshot-staging");
            Directory.CreateDirectory(stagingDir);
            await _client.ReloadSnapshotAsync(stagingDir, TestContext.Current.CancellationToken);

            await _client.AppendAsync("stream-b", StreamRevision.NoStream, [Event("C", "3")]);

            _client.CurrentPosition.Value.Should().BeGreaterThan(positionBeforeReload.Value);
        }

        [Fact]
        public async Task AfterReload_RevisionCacheIsCleared() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("A", "1")]);

            var stagingDir = Path.Combine(_dataDir, "snapshot-staging");
            Directory.CreateDirectory(stagingDir);
            await _client.ReloadSnapshotAsync(stagingDir, TestContext.Current.CancellationToken);

            // After reload from empty snapshot the store has no data and the revision cache is cleared.
            // A NoStream append to stream-a must succeed (not OCC).
            var act = async () => await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("B", "2")]);
            await act.Should().NotThrowAsync();
        }

        [Fact]
        public async Task Reload_ReplacesAllData() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "x")], TestContext.Current.CancellationToken);

            var stagingDir = Path.Combine(_dataDir, "snapshot-staging");
            Directory.CreateDirectory(stagingDir);
            await _client.ReloadSnapshotAsync(stagingDir, TestContext.Current.CancellationToken);

            var before = await _client.ReadAsync("stream-a", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
            before.Should().BeEmpty();

            await _client.AppendAsync("stream-b", StreamRevision.NoStream, [Event("Bar", "y")], TestContext.Current.CancellationToken);
            var after = await _client.ReadAsync("stream-b", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
            after.Should().ContainSingle(e => e.EventType == "Bar");
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
                await client1.DisposeAsync();

                var client2 = await EmbeddedMsspClient.OpenAsync(dataDir);
                var events = await client2.ReadAsync("stream-a").ToListAsync();
                await client2.DisposeAsync();

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
                await client1.DisposeAsync();

                var client2 = await EmbeddedMsspClient.OpenAsync(dataDir);
                var act = async () => await client2.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Bar", "data")]);
                await act.Should().ThrowAsync<OptimisticConcurrencyException>();
                await client2.DisposeAsync();
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
                await client1.DisposeAsync();

                var client2 = await EmbeddedMsspClient.OpenAsync(dataDir, memTableCapacityBytes: 128);
                var events = await client2.ReadAsync("stream-a").ToListAsync();
                await client2.DisposeAsync();

                events.Should().HaveCount(2);
                events[0].EventType.Should().Be("Foo");
                events[1].EventType.Should().Be("Bar");
            } finally {
                Directory.Delete(dataDir, recursive: true);
            }
        }
    }
}
