using System.IO.Compression;
using FluentAssertions;

namespace MSSP.Engine;

public class BackupRestoreTests : IAsyncLifetime {
    readonly string _dataDir    = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    readonly string _backupPath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName() + ".zip");
    readonly string _restoreDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    EmbeddedMsspClient _client = null!;
    bool _disposed;

    public async ValueTask InitializeAsync() => _client = await EmbeddedMsspClient.OpenAsync(_dataDir);

    public async ValueTask DisposeAsync() {
        if (!_disposed) {
            if (_client is not null) await _client.DisposeAsync();
            _disposed = true;
        }
        if (Directory.Exists(_dataDir))   Directory.Delete(_dataDir,   recursive: true);
        if (File.Exists(_backupPath))     File.Delete(_backupPath);
        if (Directory.Exists(_restoreDir)) Directory.Delete(_restoreDir, recursive: true);
    }

    static EventData Event(string type, string payload) =>
        new(type, System.Text.Encoding.UTF8.GetBytes(payload));

    public class CreateBackupAsync : BackupRestoreTests {

        [Fact]
        public async Task CreatesBackupFile() {
            await _client.CreateBackupAsync(_backupPath, TestContext.Current.CancellationToken);

            File.Exists(_backupPath).Should().BeTrue();
        }

        [Fact]
        public async Task BackupContainsWalLog() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")], TestContext.Current.CancellationToken);

            await _client.CreateBackupAsync(_backupPath, TestContext.Current.CancellationToken);

            using var zip = ZipFile.OpenRead(_backupPath);
            zip.Entries.Should().Contain(e => e.Name == "wal.log");
        }

        [Fact]
        public async Task IncludesWalPrevLog_WhenRotationHasOccurred() {
            // With capacity=128 each minimal event is ~60 bytes:
            //   stream-a + stream-b = 120 bytes (fits); stream-c = 180 bytes (overflows).
            // The third write flushes {stream-a, stream-b} to SST and lands stream-c in the new
            // MemTable, setting _rotationRequested. The fourth write triggers the actual rotation
            // (wal.log → wal_prev.log) before committing stream-d.
            await _client.DisposeAsync();
            _client = await EmbeddedMsspClient.OpenAsync(_dataDir, memTableCapacityBytes: 128, cancellationToken: TestContext.Current.CancellationToken);

            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Pre", "payload-a")], TestContext.Current.CancellationToken);
            await _client.AppendAsync("stream-b", StreamRevision.NoStream, [Event("Pre", "payload-b")], TestContext.Current.CancellationToken);
            await _client.AppendAsync("stream-c", StreamRevision.NoStream, [Event("Pre", "payload-c")], TestContext.Current.CancellationToken);
            await _client.AppendAsync("stream-d", StreamRevision.NoStream, [Event("Pre", "payload-d")], TestContext.Current.CancellationToken);

            File.Exists(Path.Combine(_dataDir, "wal_prev.log")).Should().BeTrue("the fourth write must have triggered the rotation");

            await _client.CreateBackupAsync(_backupPath, TestContext.Current.CancellationToken);

            using var zip = ZipFile.OpenRead(_backupPath);
            zip.Entries.Should().Contain(e => e.Name == "wal_prev.log");
        }
    }

    public class RestoreBackupAsync : BackupRestoreTests {

        [Fact]
        public async Task RoundTrip_AllEventsReadableAfterRestore() {
            // Arrange: write events and create backup.
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "first"), Event("Bar", "second")], TestContext.Current.CancellationToken);
            await _client.AppendAsync("stream-b", StreamRevision.NoStream, [Event("Baz", "only")], TestContext.Current.CancellationToken);
            await _client.CreateBackupAsync(_backupPath, TestContext.Current.CancellationToken);
            await _client.DisposeAsync();
            _disposed = true;

            // Act: restore and open.
            await EmbeddedMsspClient.RestoreBackupAsync(_backupPath, _restoreDir, TestContext.Current.CancellationToken);
            await using var restored = await EmbeddedMsspClient.OpenAsync(_restoreDir, cancellationToken: TestContext.Current.CancellationToken);

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
            await _client.CreateBackupAsync(_backupPath, TestContext.Current.CancellationToken);
            await _client.DisposeAsync();
            _disposed = true;

            // Write different data to restoreDir first.
            await using (var other = await EmbeddedMsspClient.OpenAsync(_restoreDir, cancellationToken: TestContext.Current.CancellationToken))
                await other.AppendAsync("stream-z", StreamRevision.NoStream, [Event("Other", "data")], TestContext.Current.CancellationToken);

            // Act: restore backup on top of restoreDir.
            await EmbeddedMsspClient.RestoreBackupAsync(_backupPath, _restoreDir, TestContext.Current.CancellationToken);

            // Assert: after restore, only the original events are present.
            await using var restored = await EmbeddedMsspClient.OpenAsync(_restoreDir, cancellationToken: TestContext.Current.CancellationToken);
            var streamA = await restored.ReadAsync("stream-a", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
            streamA.Should().HaveCount(1);

            var streamZ = await restored.ReadAsync("stream-z", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
            streamZ.Should().BeEmpty();
        }

        [Fact]
        public async Task Restore_OnEmptyTargetDirectory_Succeeds() {
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "data")], TestContext.Current.CancellationToken);
            await _client.CreateBackupAsync(_backupPath, TestContext.Current.CancellationToken);

            var act = async () => await EmbeddedMsspClient.RestoreBackupAsync(_backupPath, _restoreDir, TestContext.Current.CancellationToken);

            await act.Should().NotThrowAsync();
        }

        [Fact]
        public async Task Restore_GlobalPositionContinuesAfterBackupPosition() {
            // Arrange: write events so GlobalPosition advances, then backup.
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("A", "1"), Event("B", "2"), Event("C", "3")], TestContext.Current.CancellationToken);
            var positionBeforeBackup = _client.CurrentPosition;
            await _client.CreateBackupAsync(_backupPath, TestContext.Current.CancellationToken);
            await _client.DisposeAsync();
            _disposed = true;

            // Act: restore and open.
            await EmbeddedMsspClient.RestoreBackupAsync(_backupPath, _restoreDir, TestContext.Current.CancellationToken);
            await using var restored = await EmbeddedMsspClient.OpenAsync(_restoreDir, cancellationToken: TestContext.Current.CancellationToken);

            // Assert: CurrentPosition after restore matches position at backup time.
            restored.CurrentPosition.Should().Be(positionBeforeBackup);

            // Writing a new event must advance GlobalPosition beyond the backup position,
            // not reset to 1 (which would collide with pre-backup events).
            await restored.AppendAsync("stream-b", StreamRevision.NoStream, [Event("D", "4")], TestContext.Current.CancellationToken);
            restored.CurrentPosition.Value.Should().BeGreaterThan(positionBeforeBackup.Value);
        }

        [Fact]
        public async Task AfterRotation_AllEventsReadableAfterRestore() {
            // Controlled rotation scenario — see capacity comment in IncludesWalPrevLog_WhenRotationHasOccurred.
            // After the rotation: SST = {stream-a, stream-b}, wal_prev.log = {stream-a..stream-c},
            // wal.log = {stream-d}. stream-c is the "A-record": in wal_prev.log but not yet in SST.
            // Without the fix, stream-c would be lost on restore.
            await _client.DisposeAsync();
            _client = await EmbeddedMsspClient.OpenAsync(_dataDir, memTableCapacityBytes: 128, cancellationToken: TestContext.Current.CancellationToken);

            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Pre", "payload-a")], TestContext.Current.CancellationToken);
            await _client.AppendAsync("stream-b", StreamRevision.NoStream, [Event("Pre", "payload-b")], TestContext.Current.CancellationToken);
            await _client.AppendAsync("stream-c", StreamRevision.NoStream, [Event("Pre", "payload-c")], TestContext.Current.CancellationToken);
            await _client.AppendAsync("stream-d", StreamRevision.NoStream, [Event("Pre", "payload-d")], TestContext.Current.CancellationToken);

            File.Exists(Path.Combine(_dataDir, "wal_prev.log")).Should().BeTrue("the fourth write must have triggered the rotation");

            await _client.CreateBackupAsync(_backupPath, TestContext.Current.CancellationToken);
            await _client.DisposeAsync();
            _disposed = true;

            await EmbeddedMsspClient.RestoreBackupAsync(_backupPath, _restoreDir, TestContext.Current.CancellationToken);
            await using var restored = await EmbeddedMsspClient.OpenAsync(_restoreDir, cancellationToken: TestContext.Current.CancellationToken);

            (await restored.ReadAsync("stream-a", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken)).Should().ContainSingle();
            (await restored.ReadAsync("stream-b", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken)).Should().ContainSingle();
            (await restored.ReadAsync("stream-c", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken)).Should().ContainSingle();
            (await restored.ReadAsync("stream-d", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken)).Should().ContainSingle();
        }

        [Fact]
        public async Task DeletesStaleWalPrevLog_BeforeExtractingArchive() {
            // Arrange: back up the primary store, then place a stale wal_prev.log in the restore target.
            await _client.AppendAsync("stream-a", StreamRevision.NoStream, [Event("Foo", "original")], TestContext.Current.CancellationToken);
            await _client.CreateBackupAsync(_backupPath, TestContext.Current.CancellationToken);

            Directory.CreateDirectory(_restoreDir);
            var staleWalPrev = Path.Combine(_restoreDir, "wal_prev.log");
            await File.WriteAllBytesAsync(staleWalPrev, [0xFF, 0xFF, 0xFF, 0xFF], TestContext.Current.CancellationToken);

            // Act: restore on top of a directory that contains a stale wal_prev.log.
            await EmbeddedMsspClient.RestoreBackupAsync(_backupPath, _restoreDir, TestContext.Current.CancellationToken);

            // Assert: stale file is gone and the store opens cleanly with only the backed-up events.
            File.Exists(staleWalPrev).Should().BeFalse();
            await using var restored = await EmbeddedMsspClient.OpenAsync(_restoreDir, cancellationToken: TestContext.Current.CancellationToken);
            var events = await restored.ReadAsync("stream-a", cancellationToken: TestContext.Current.CancellationToken)
                .ToListAsync(TestContext.Current.CancellationToken);
            events.Should().ContainSingle();
        }
    }
}
