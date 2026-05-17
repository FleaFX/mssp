using System.Text;
using FluentAssertions;

namespace MSSP.Embedded;

public class WalManagerTests : IAsyncLifetime {
    readonly string _dataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    WalManager _wal = null!;

    public Task InitializeAsync() {
        Directory.CreateDirectory(_dataDir);
        _wal = WalManager.Open(_dataDir);
        return Task.CompletedTask;
    }

    public Task DisposeAsync() {
        _wal.Dispose();
        Directory.Delete(_dataDir, recursive: true);
        return Task.CompletedTask;
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
            await _wal.AppendAsync(payload, default);

            var records = await _wal.ReadAllAsync().ToListAsync();

            records.Should().ContainSingle();
            records[0].ToArray().Should().Equal(payload.ToArray());
        }

        [Fact]
        public async Task MultipleRecords_ReturnedInAppendOrder() {
            await _wal.AppendAsync(Bytes("first"), default);
            await _wal.AppendAsync(Bytes("second"), default);
            await _wal.AppendAsync(Bytes("third"), default);

            var records = await _wal.ReadAllAsync().ToListAsync();

            records.Should().HaveCount(3);
            Text(records[0]).Should().Be("first");
            Text(records[1]).Should().Be("second");
            Text(records[2]).Should().Be("third");
        }
    }

    public class RotateAsync : WalManagerTests {
        [Fact]
        public async Task AfterRotate_PreviousRecordsNotReadable() {
            await _wal.AppendAsync(Bytes("before"), default);
            await _wal.RotateAsync(default);

            var records = await _wal.ReadAllAsync().ToListAsync();

            records.Should().BeEmpty();
        }

        [Fact]
        public async Task AfterRotate_NewRecordsAreReadable() {
            await _wal.AppendAsync(Bytes("before"), default);
            await _wal.RotateAsync(default);
            await _wal.AppendAsync(Bytes("after"), default);

            var records = await _wal.ReadAllAsync().ToListAsync();

            records.Should().ContainSingle();
            Text(records[0]).Should().Be("after");
        }
    }
}
