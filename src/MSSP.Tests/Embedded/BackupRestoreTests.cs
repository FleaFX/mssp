using FluentAssertions;

namespace MSSP.Embedded;

public class BackupRestoreTests : IAsyncLifetime {
    readonly string _dataDir   = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    readonly string _backupDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    readonly string _restoreDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    EmbeddedMsspClient _client = null!;
    bool _disposed;

    public async ValueTask InitializeAsync() => _client = await EmbeddedMsspClient.OpenAsync(_dataDir);

    public ValueTask DisposeAsync() {
        if (!_disposed) {
            _client?.Dispose();
            _disposed = true;
        }
        foreach (var dir in new[] { _dataDir, _backupDir, _restoreDir })
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        return ValueTask.CompletedTask;
    }

    static EventData Event(string type, string payload) =>
        new(type, System.Text.Encoding.UTF8.GetBytes(payload));

    public class CreateBackupAsync : BackupRestoreTests {

        [Fact]
        public async Task CreatesBackupDirectory() {
            await _client.CreateBackupAsync(_backupDir, TestContext.Current.CancellationToken);

            Directory.Exists(_backupDir).Should().BeTrue();
        }

        [Fact]
        public async Task BackupContainsWalLog() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")], TestContext.Current.CancellationToken);

            await _client.CreateBackupAsync(_backupDir, TestContext.Current.CancellationToken);

            File.Exists(Path.Combine(_backupDir, "wal.log")).Should().BeTrue();
        }

        [Fact]
        public async Task BackupContainsSstFiles_AfterFlush() {
            // Write enough events to trigger a flush (use small memTableCapacityBytes).
            _client.Dispose();
            _client = await EmbeddedMsspClient.OpenAsync(_dataDir, memTableCapacityBytes: 128, cancellationToken: TestContext.Current.CancellationToken);

            for (var i = 0; i < 20; i++)
                await _client.AppendAsync($"stream-{i}", StreamRevision.NoStream, [Event("Foo", $"payload-{i}")], TestContext.Current.CancellationToken);

            await _client.CreateBackupAsync(_backupDir, TestContext.Current.CancellationToken);

            Directory.EnumerateFiles(_backupDir, "*.sst").Should().NotBeEmpty();
        }
    }

    public class RestoreBackupAsync : BackupRestoreTests {

        [Fact]
        public async Task RoundTrip_AllEventsReadableAfterRestore() {
            // Arrange: write events and create backup.
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "first"), Event("Bar", "second")], TestContext.Current.CancellationToken);
            await _client.AppendAsync("stream-b", StreamRevision.NoStream, [Event("Baz", "only")], TestContext.Current.CancellationToken);
            await _client.CreateBackupAsync(_backupDir, TestContext.Current.CancellationToken);
            _client.Dispose();
            _disposed = true;

            // Act: restore and open.
            await EmbeddedMsspClient.RestoreBackupAsync(_backupDir, _restoreDir, TestContext.Current.CancellationToken);
            using var restored = await EmbeddedMsspClient.OpenAsync(_restoreDir, cancellationToken: TestContext.Current.CancellationToken);

            // Assert: all events from before backup are readable.
            var streamA = await restored.ReadAsync("stream-a", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
            streamA.Should().HaveCount(2);
            streamA[0].EventType.Should().Be("Foo");
            streamA[1].EventType.Should().Be("Bar");

            var streamB = await restored.ReadAsync("stream-b", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
            streamB.Should().HaveCount(1);
            streamB[0].EventType.Should().Be("Baz");
        }

        [Fact]
        public async Task Restore_OverwritesExistingDataDirectory() {
            // Arrange: write events to dataDir, backup, then write different events to restoreDir.
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Original", "data")], TestContext.Current.CancellationToken);
            await _client.CreateBackupAsync(_backupDir, TestContext.Current.CancellationToken);
            _client.Dispose();
            _disposed = true;

            // Write different data to restoreDir first.
            using (var other = await EmbeddedMsspClient.OpenAsync(_restoreDir, cancellationToken: TestContext.Current.CancellationToken))
                await other.AppendAsync("stream-z", StreamRevision.NoStream, [Event("Other", "data")], TestContext.Current.CancellationToken);

            // Act: restore backup on top of restoreDir.
            await EmbeddedMsspClient.RestoreBackupAsync(_backupDir, _restoreDir, TestContext.Current.CancellationToken);

            // Assert: after restore, only the original events are present.
            using var restored = await EmbeddedMsspClient.OpenAsync(_restoreDir, cancellationToken: TestContext.Current.CancellationToken);
            var streamA = await restored.ReadAsync("stream-a", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
            streamA.Should().HaveCount(1);

            var streamZ = await restored.ReadAsync("stream-z", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
            streamZ.Should().BeEmpty();
        }

        [Fact]
        public async Task Restore_OnEmptyTargetDirectory_Succeeds() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")], TestContext.Current.CancellationToken);
            await _client.CreateBackupAsync(_backupDir, TestContext.Current.CancellationToken);

            var act = async () => await EmbeddedMsspClient.RestoreBackupAsync(_backupDir, _restoreDir, TestContext.Current.CancellationToken);

            await act.Should().NotThrowAsync();
        }
    }
}
