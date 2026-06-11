using System.Text;
using FluentAssertions;

namespace MSSP.Embedded;

public class WalManagerTests : IAsyncLifetime {
    readonly string _dataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    WalManager _wal = null!;

    public ValueTask InitializeAsync() {
        Directory.CreateDirectory(_dataDir);
        _wal = WalManager.Open(_dataDir);
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() {
        _wal.Dispose();
        Directory.Delete(_dataDir, recursive: true);
        return ValueTask.CompletedTask;
    }

    static ReadOnlyMemory<byte> Bytes(string s) => Encoding.UTF8.GetBytes(s);
    static string Text(ReadOnlyMemory<byte> bytes) => Encoding.UTF8.GetString(bytes.Span);

    public class Open : WalManagerTests {
        [Fact]
        public void CreatesWalFileInDataDirectory() =>
            File.Exists(Path.Combine(_dataDir, "wal.log")).Should().BeTrue();
    }

    public class AppendAndRead : WalManagerTests {
        [Fact]
        public async Task SingleRecord_RoundTrips() {
            var payload = Bytes("hello");
            await _wal.AppendAsync(payload, TestContext.Current.CancellationToken);

            var records = await _wal.ReadAllForRecoveryAsync().ToListAsync();

            records.Should().ContainSingle();
            records[0].ToArray().Should().Equal(payload.ToArray());
        }

        [Fact]
        public async Task MultipleRecords_ReturnedInAppendOrder() {
            await _wal.AppendAsync(Bytes("first"), TestContext.Current.CancellationToken);
            await _wal.AppendAsync(Bytes("second"), TestContext.Current.CancellationToken);
            await _wal.AppendAsync(Bytes("third"), TestContext.Current.CancellationToken);

            var records = await _wal.ReadAllForRecoveryAsync().ToListAsync();

            records.Should().HaveCount(3);
            Text(records[0]).Should().Be("first");
            Text(records[1]).Should().Be("second");
            Text(records[2]).Should().Be("third");
        }
    }

    public class RotateAsync : WalManagerTests {
        [Fact]
        public async Task AfterRotate_OldRecordsStillAccessibleForRecovery() {
            await _wal.AppendAsync(Bytes("before"), TestContext.Current.CancellationToken);
            await _wal.RotateAsync(TestContext.Current.CancellationToken);

            var records = await _wal.ReadAllForRecoveryAsync().ToListAsync();

            records.Should().ContainSingle();
            Text(records[0]).Should().Be("before");
        }

        [Fact]
        public async Task AfterRotate_NewRecordsReadableAlongsideOldOnes() {
            await _wal.AppendAsync(Bytes("before"), TestContext.Current.CancellationToken);
            await _wal.RotateAsync(TestContext.Current.CancellationToken);
            await _wal.AppendAsync(Bytes("after"), TestContext.Current.CancellationToken);

            var records = await _wal.ReadAllForRecoveryAsync().ToListAsync();

            records.Should().HaveCount(2);
            Text(records[0]).Should().Be("before");
            Text(records[1]).Should().Be("after");
        }

        [Fact]
        public async Task AfterDeletePrev_OldRecordsGone() {
            await _wal.AppendAsync(Bytes("before"), TestContext.Current.CancellationToken);
            await _wal.RotateAsync(TestContext.Current.CancellationToken);
            _wal.DeletePrevWalIfExists();
            await _wal.AppendAsync(Bytes("after"), TestContext.Current.CancellationToken);

            var records = await _wal.ReadAllForRecoveryAsync().ToListAsync();

            records.Should().ContainSingle();
            Text(records[0]).Should().Be("after");
        }

        [Fact]
        public async Task SecondRotate_ReplacesFirstArchive() {
            await _wal.AppendAsync(Bytes("first"), TestContext.Current.CancellationToken);
            await _wal.RotateAsync(TestContext.Current.CancellationToken);
            await _wal.AppendAsync(Bytes("second"), TestContext.Current.CancellationToken);
            await _wal.RotateAsync(TestContext.Current.CancellationToken);

            var records = await _wal.ReadAllForRecoveryAsync().ToListAsync();

            records.Should().ContainSingle();
            Text(records[0]).Should().Be("second");
        }
    }
}
