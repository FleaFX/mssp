using System.Runtime.CompilerServices;
using System.Text;
using FluentAssertions;

namespace MSSP.LsmTree;

public class LsmStoreTests : IAsyncLifetime {
    readonly string _dataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    readonly List<ReadOnlyMemory<byte>> _captured = [];
    LsmStore<StringKey> _store = null!;

    public async Task InitializeAsync() {
        Directory.CreateDirectory(_dataDir);
        _store = await LsmStore<StringKey>.OpenAsync(Options(), Empty(), default);
    }

    public Task DisposeAsync() {
        _store.Dispose();
        Directory.Delete(_dataDir, recursive: true);
        return Task.CompletedTask;
    }

    LsmStoreOptions Options(int capacityBytes = 4096) =>
        new(_dataDir, capacityBytes, AppendToWal, _ => ValueTask.CompletedTask);

    ValueTask<bool> AppendToWal(ReadOnlyMemory<byte> record, CancellationToken _) {
        _captured.Add(record.ToArray());
        return ValueTask.FromResult(true);
    }

    static async IAsyncEnumerable<ReadOnlyMemory<byte>> Empty(
            [EnumeratorCancellation] CancellationToken ct = default) {
        await Task.Yield();
        yield break;
    }

    async IAsyncEnumerable<ReadOnlyMemory<byte>> Replay(
            [EnumeratorCancellation] CancellationToken ct = default) {
        await Task.Yield();
        foreach (var record in _captured)
            yield return record;
    }

    static ReadOnlyMemory<byte> Bytes(string s) => Encoding.UTF8.GetBytes(s);

    public class OpenAsync : LsmStoreTests {
        [Fact]
        public void EmptyDirectory_StoreStartsEmpty() =>
            _store.ScanAllFrom(new StringKey("")).Should().BeEmpty();

        [Fact]
        public async Task WithWalRecords_ReplaysMissingEntries() {
            await _store.WriteAsync(new StringKey("a"), Bytes("1"), default);
            await _store.WriteAsync(new StringKey("b"), Bytes("2"), default);

            using var recovered = await LsmStore<StringKey>.OpenAsync(Options(), Replay(), default);

            recovered.ScanAllFrom(new StringKey(""))
                     .Select(e => e.Key.Value)
                     .Should().Equal("a", "b");
        }

        [Fact]
        public async Task Recovery_SkipsWalRecordsAlreadyInSst() {
            // capacity 4: a(1b key)+1b value = 2; b = 2 → Size=4; c triggers flush → a,b in SST, c in MemTable
            var tinyStore = await LsmStore<StringKey>.OpenAsync(Options(4), Empty(), default);
            await tinyStore.WriteAsync(new StringKey("a"), Bytes("1"), default);
            await tinyStore.WriteAsync(new StringKey("b"), Bytes("2"), default);
            await tinyStore.WriteAsync(new StringKey("c"), Bytes("3"), default);
            tinyStore.Dispose();

            // WAL has a,b,c; SST has a,b → RecoverAsync must apply only c, not duplicate a or b
            using var recovered = await LsmStore<StringKey>.OpenAsync(Options(4), Replay(), default);

            recovered.ScanAllFrom(new StringKey(""))
                     .Select(e => e.Key.Value)
                     .Should().Equal("a", "b", "c");
        }
    }

    public class WriteAsync : LsmStoreTests {
        [Fact]
        public async Task StoresEntry_VisibleInScan() {
            await _store.WriteAsync(new StringKey("key"), Bytes("value"), default);

            var entry = _store.ScanAllFrom(new StringKey("key")).Single();
            Encoding.UTF8.GetString(entry.Value!.Value.Span).Should().Be("value");
        }

        [Fact]
        public async Task FullMemTable_FlushesToSst() {
            // capacity 4: a+b fills MemTable; c triggers flush
            var tinyStore = await LsmStore<StringKey>.OpenAsync(Options(4), Empty(), default);
            await tinyStore.WriteAsync(new StringKey("a"), Bytes("1"), default);
            await tinyStore.WriteAsync(new StringKey("b"), Bytes("2"), default);
            await tinyStore.WriteAsync(new StringKey("c"), Bytes("3"), default);
            tinyStore.Dispose();

            Directory.EnumerateFiles(_dataDir, "*.sst").Should().HaveCount(1);
        }

        [Fact]
        public async Task EntryExceedingCapacity_Throws() {
            var act = async () => await _store.WriteAsync(new StringKey("key"), new byte[4097], default);

            await act.Should().ThrowAsync<InvalidOperationException>();
        }
    }

    public class ScanAllFromTests : LsmStoreTests {
        [Fact]
        public void EmptyStore_ReturnsEmpty() =>
            _store.ScanAllFrom(new StringKey("")).Should().BeEmpty();

        [Fact]
        public async Task FromExistingKey_IncludesThatKeyAndAfter() {
            await _store.WriteAsync(new StringKey("a"), Bytes("1"), default);
            await _store.WriteAsync(new StringKey("b"), Bytes("2"), default);
            await _store.WriteAsync(new StringKey("c"), Bytes("3"), default);

            _store.ScanAllFrom(new StringKey("b"))
                  .Select(e => e.Key.Value)
                  .Should().Equal("b", "c");
        }

        [Fact]
        public async Task FromBetweenKeys_StartsAtNextKey() {
            await _store.WriteAsync(new StringKey("a"), Bytes("1"), default);
            await _store.WriteAsync(new StringKey("c"), Bytes("3"), default);

            _store.ScanAllFrom(new StringKey("b"))
                  .Should().ContainSingle(e => e.Key == new StringKey("c"));
        }

        [Fact]
        public async Task SpansSstAndMemTable_ReturnsAllEntriesInOrder() {
            // capacity 4: a+b fills MemTable; c triggers flush → a,b in SST, c in MemTable
            var crossStore = await LsmStore<StringKey>.OpenAsync(Options(4), Empty(), default);
            await crossStore.WriteAsync(new StringKey("a"), Bytes("1"), default);
            await crossStore.WriteAsync(new StringKey("b"), Bytes("2"), default);
            await crossStore.WriteAsync(new StringKey("c"), Bytes("3"), default);

            crossStore.ScanAllFrom(new StringKey(""))
                      .Select(e => e.Key.Value)
                      .Should().Equal("a", "b", "c");
            crossStore.Dispose();
        }
    }

    public class ScanSnapshotFromTests : LsmStoreTests {
        [Fact]
        public async Task ReturnsCurrentEntries() {
            await _store.WriteAsync(new StringKey("a"), Bytes("1"), default);
            await _store.WriteAsync(new StringKey("b"), Bytes("2"), default);

            _store.ScanSnapshotFrom(new StringKey(""))
                  .Select(e => e.Key.Value)
                  .Should().Equal("a", "b");
        }

        [Fact]
        public async Task Snapshot_IsLazy_SeesWritesToSameMemTableAfterCapture() {
            // Snapshot captures the MemTable reference, not a copy of its data.
            // Writes to the same MemTable after the snapshot is captured are visible when iterated.
            await _store.WriteAsync(new StringKey("a"), Bytes("1"), default);
            var snapshot = _store.ScanSnapshotFrom(new StringKey(""));
            await _store.WriteAsync(new StringKey("b"), Bytes("2"), default);

            snapshot.Select(e => e.Key.Value).Should().Equal("a", "b");
        }

        [Fact]
        public async Task Snapshot_DoesNotIncludeEntriesFromNewSstFilesCreatedAfterCapture() {
            // capacity 4: a+b fills MemTable to 4 bytes; capturing snapshot before flush
            var tinyStore = await LsmStore<StringKey>.OpenAsync(Options(4), Empty(), default);
            await tinyStore.WriteAsync(new StringKey("a"), Bytes("1"), default);
            await tinyStore.WriteAsync(new StringKey("b"), Bytes("2"), default);

            // Materialize snapshot before flush so we get a stable baseline
            var snapshot = tinyStore.ScanSnapshotFrom(new StringKey("")).ToList();

            // Writing c triggers flush: a,b→SST, c→new MemTable
            await tinyStore.WriteAsync(new StringKey("c"), Bytes("3"), default);

            // Full store now has a,b (SST) and c (MemTable)
            tinyStore.ScanAllFrom(new StringKey(""))
                     .Select(e => e.Key.Value)
                     .Should().Equal("a", "b", "c");

            // Snapshot was captured and materialized before the flush — contains only a and b
            snapshot.Select(e => e.Key.Value).Should().Equal("a", "b");
            tinyStore.Dispose();
        }
    }
}
