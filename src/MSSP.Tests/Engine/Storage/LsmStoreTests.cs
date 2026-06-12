using System.Runtime.CompilerServices;
using System.Text;
using FluentAssertions;
using MSSP.Storage;

namespace MSSP.Engine.Storage;

public class LsmStoreTests : IAsyncLifetime {
    readonly string _dataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    LsmStore<StringKey> _store = null!;

    public async ValueTask InitializeAsync() {
        Directory.CreateDirectory(_dataDir);
        _store = await LsmStore<StringKey>.OpenAsync(LsmOptions(), Empty(), TestContext.Current.CancellationToken);
    }

    public ValueTask DisposeAsync() {
        _store.Dispose();
        Directory.Delete(_dataDir, recursive: true);
        return ValueTask.CompletedTask;
    }

    LsmStoreOptions<StringKey> LsmOptions(int capacityBytes = 4096, long baseLevelSizeBytes = -1, int levelSizeMultiplier = 10) =>
        new(_dataDir, capacityBytes, _ => ValueTask.CompletedTask, baseLevelSizeBytes, levelSizeMultiplier);

    static async IAsyncEnumerable<ReadOnlyMemory<byte>> Empty([EnumeratorCancellation] CancellationToken cancellationToken = default) {
        await Task.Yield();
        yield break;
    }

    static async IAsyncEnumerable<ReadOnlyMemory<byte>> Replay(List<ReadOnlyMemory<byte>> captured, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        await Task.Yield();
        foreach (var record in captured)
            yield return record;
    }

    static Memory<byte> Bytes(string s) => Encoding.UTF8.GetBytes(s);

    static void CaptureWalRecord(List<ReadOnlyMemory<byte>> captured, StringKey key, Memory<byte> value) =>
        captured.Add(((ReadOnlyMemory<byte>)WalRecord.From(key, value)).ToArray());

    public class OpenAsync : LsmStoreTests {
        [Fact]
        public void EmptyDirectory_StoreStartsEmpty() =>
            _store.ScanAllFrom(new StringKey("")).Should().BeEmpty();

        [Fact]
        public async Task WithWalRecords_ReplaysMissingEntries() {
            var captured = new List<ReadOnlyMemory<byte>>();
            using var lsm = await LsmStore<StringKey>.OpenAsync(LsmOptions(), Empty(), TestContext.Current.CancellationToken);
            CaptureWalRecord(captured, new StringKey("a"), Bytes("1"));
            await lsm.WriteAsync(new StringKey("a"), Bytes("1"), TestContext.Current.CancellationToken);
            CaptureWalRecord(captured, new StringKey("b"), Bytes("2"));
            await lsm.WriteAsync(new StringKey("b"), Bytes("2"), TestContext.Current.CancellationToken);

            using var recovered = await LsmStore<StringKey>.OpenAsync(LsmOptions(), Replay(captured), TestContext.Current.CancellationToken);

            recovered.ScanAllFrom(new StringKey(""))
                     .Select(e => e.Key.Value)
                     .Should().Equal("a", "b");
        }

        [Fact]
        public async Task Recovery_SkipsWalRecordsAlreadyInSst() {
            // capacity 4: a(1b key)+1b value = 2; b = 2 → Size=4; c triggers flush → a,b in SST, c in MemTable
            var captured = new List<ReadOnlyMemory<byte>>();
            using var lsm = await LsmStore<StringKey>.OpenAsync(LsmOptions(4), Empty(), TestContext.Current.CancellationToken);
            CaptureWalRecord(captured, new StringKey("a"), Bytes("1"));
            await lsm.WriteAsync(new StringKey("a"), Bytes("1"), TestContext.Current.CancellationToken);
            CaptureWalRecord(captured, new StringKey("b"), Bytes("2"));
            await lsm.WriteAsync(new StringKey("b"), Bytes("2"), TestContext.Current.CancellationToken);
            CaptureWalRecord(captured, new StringKey("c"), Bytes("3"));
            await lsm.WriteAsync(new StringKey("c"), Bytes("3"), TestContext.Current.CancellationToken);

            // WAL has a,b,c; SST has a,b → RecoverAsync must apply only c, not duplicate a or b
            using var recovered = await LsmStore<StringKey>.OpenAsync(LsmOptions(4), Replay(captured), TestContext.Current.CancellationToken);

            recovered.ScanAllFrom(new StringKey(""))
                     .Select(e => e.Key.Value)
                     .Should().Equal("a", "b", "c");
        }
    }

    public class WriteAsync : LsmStoreTests {
        [Fact]
        public async Task StoresEntry_VisibleInScan() {
            await _store.WriteAsync(new StringKey("key"), Bytes("value"), TestContext.Current.CancellationToken);

            var entry = _store.ScanAllFrom(new StringKey("key")).Single();
            Encoding.UTF8.GetString(entry.Value!.Value.Span).Should().Be("value");
        }

        [Fact]
        public async Task FullMemTable_FlushesToSst() {
            // capacity 4: a+b fills MemTable; c triggers flush
            var tinyStore = await LsmStore<StringKey>.OpenAsync(LsmOptions(4), Empty(), TestContext.Current.CancellationToken);
            await tinyStore.WriteAsync(new StringKey("a"), Bytes("1"), TestContext.Current.CancellationToken);
            await tinyStore.WriteAsync(new StringKey("b"), Bytes("2"), TestContext.Current.CancellationToken);
            await tinyStore.WriteAsync(new StringKey("c"), Bytes("3"), TestContext.Current.CancellationToken);
            tinyStore.Dispose();

            Directory.EnumerateFiles(_dataDir, "*.sst").Should().HaveCount(1);
        }

        [Fact]
        public async Task EntryExceedingCapacity_Throws() {
            var act = async () => await _store.WriteAsync(new StringKey("key"), new byte[4097], TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<InvalidOperationException>();
        }
    }

    public class ScanAllFromTests : LsmStoreTests {
        [Fact]
        public void EmptyStore_ReturnsEmpty() =>
            _store.ScanAllFrom(new StringKey("")).Should().BeEmpty();

        [Fact]
        public async Task FromExistingKey_IncludesThatKeyAndAfter() {
            await _store.WriteAsync(new StringKey("a"), Bytes("1"), TestContext.Current.CancellationToken);
            await _store.WriteAsync(new StringKey("b"), Bytes("2"), TestContext.Current.CancellationToken);
            await _store.WriteAsync(new StringKey("c"), Bytes("3"), TestContext.Current.CancellationToken);

            _store.ScanAllFrom(new StringKey("b"))
                  .Select(e => e.Key.Value)
                  .Should().Equal("b", "c");
        }

        [Fact]
        public async Task FromBetweenKeys_StartsAtNextKey() {
            await _store.WriteAsync(new StringKey("a"), Bytes("1"), TestContext.Current.CancellationToken);
            await _store.WriteAsync(new StringKey("c"), Bytes("3"), TestContext.Current.CancellationToken);

            _store.ScanAllFrom(new StringKey("b"))
                  .Should().ContainSingle(e => e.Key == new StringKey("c"));
        }

        [Fact]
        public async Task SpansSstAndMemTable_ReturnsAllEntriesInOrder() {
            // capacity 4: a+b fills MemTable; c triggers flush → a,b in SST, c in MemTable
            var crossStore = await LsmStore<StringKey>.OpenAsync(LsmOptions(4), Empty(), TestContext.Current.CancellationToken);
            await crossStore.WriteAsync(new StringKey("a"), Bytes("1"), TestContext.Current.CancellationToken);
            await crossStore.WriteAsync(new StringKey("b"), Bytes("2"), TestContext.Current.CancellationToken);
            await crossStore.WriteAsync(new StringKey("c"), Bytes("3"), TestContext.Current.CancellationToken);

            crossStore.ScanAllFrom(new StringKey(""))
                      .Select(e => e.Key.Value)
                      .Should().Equal("a", "b", "c");
            crossStore.Dispose();
        }
    }

    public class CompactAsyncTests : LsmStoreTests {
        // Disable auto-compaction by default so tests can assert exact SST file counts.
        LsmStoreOptions<StringKey> Options(int capacityBytes = 4096, long baseLevelSizeBytes = long.MaxValue) =>
            new(_dataDir, capacityBytes, _ => ValueTask.CompletedTask, baseLevelSizeBytes, LevelSizeMultiplier: 10);

        [Fact]
        public async Task NoOp_WhenLevelSizeBelowThreshold() {
            // With large baseLevelSizeBytes, compaction won't be triggered
            await _store.WriteAsync(new StringKey("a"), Bytes("1"), TestContext.Current.CancellationToken);

            await _store.CompactAsync(TestContext.Current.CancellationToken);

            Directory.EnumerateFiles(_dataDir, "*.sst").Should().BeEmpty();
            _store.ScanAllFrom(new StringKey("")).Select(e => e.Key.Value).Should().Equal("a");
        }

        [Fact]
        public async Task MergesMultipleSstFilesInL1IntoL2() {
            // With small baseLevelSizeBytes, multiple flushes to L1 will trigger compaction to L2
            var store = await LsmStore<StringKey>.OpenAsync(
                Options(capacityBytes: 4, baseLevelSizeBytes: 50), 
                Empty(), TestContext.Current.CancellationToken);
            
            for (int i = 0; i < 15; i++) {
                await store.WriteAsync(new StringKey($"a{i}"), Bytes("v"), TestContext.Current.CancellationToken);
            }
            
            // Should have files in L2 after compaction
            var sstFiles = Directory.EnumerateFiles(_dataDir, "*.sst").ToList();
            sstFiles.Should().NotBeEmpty();
            
            // Check that we have level-named files
            sstFiles.Any(f => f.Contains("_L2.sst")).Should().BeTrue();
            store.Dispose();
        }

        [Fact]
        public async Task PreservesAllEntriesAfterCompaction() {
            var store = await LsmStore<StringKey>.OpenAsync(
                Options(capacityBytes: 4, baseLevelSizeBytes: 50), 
                Empty(), TestContext.Current.CancellationToken);
            
            for (int i = 0; i < 10; i++) {
                await store.WriteAsync(new StringKey($"k{i}"), Bytes("v"), TestContext.Current.CancellationToken);
            }

            await store.CompactAsync(TestContext.Current.CancellationToken);

            store.ScanAllFrom(new StringKey(""))
                 .Select(e => e.Key.Value)
                 .Should().Equal(Enumerable.Range(0, 10).Select(i => $"k{i}").OrderBy(k => k));
            store.Dispose();
        }

        [Fact]
        public async Task AutoCompacts_WhenLevelSizeReachesThreshold() {
            // With small baseLevelSizeBytes, compaction will be triggered based on size
            // Write enough data to trigger L1 compaction to L2
            var store = await LsmStore<StringKey>.OpenAsync(
                Options(capacityBytes: 4, baseLevelSizeBytes: 50), 
                Empty(), TestContext.Current.CancellationToken);
            
            // Zero-pad keys so ordinal and numeric order agree: k00 < k01 < ... < k14
            for (int i = 0; i < 15; i++) {
                await store.WriteAsync(new StringKey($"k{i:D2}"), Bytes("v"), TestContext.Current.CancellationToken);
            }

            // With size-based compaction, we should have files in L2 after compaction
            Directory.EnumerateFiles(_dataDir, "*.sst")
                     .Should().Contain(f => f.Contains("_L2.sst"), "expected at least one L2 file after auto-compaction");

            // Scan returns results in ascending key order.
            // Keys use D2 zero-padding so ordinal and numeric order agree (k00 < k01 < ... < k14).
            store.ScanAllFrom(new StringKey(""))
                 .Select(e => e.Key.Value)
                 .Should().Equal(Enumerable.Range(0, 15).Select(i => $"k{i:D2}"));
            store.Dispose();
        }
    }

    public class ScanSnapshotFromTests : LsmStoreTests {
        [Fact]
        public async Task ReturnsCurrentEntries() {
            await _store.WriteAsync(new StringKey("a"), Bytes("1"), TestContext.Current.CancellationToken);
            await _store.WriteAsync(new StringKey("b"), Bytes("2"), TestContext.Current.CancellationToken);

            _store.ScanSnapshotFrom(new StringKey(""))
                  .Select(e => e.Key.Value)
                  .Should().Equal("a", "b");
        }

        [Fact]
        public async Task Snapshot_IsLazy_SeesWritesToSameMemTableAfterCapture() {
            // Snapshot captures the MemTable reference, not a copy of its data.
            // Writes to the same MemTable after the snapshot is captured are visible when iterated.
            await _store.WriteAsync(new StringKey("a"), Bytes("1"), TestContext.Current.CancellationToken);
            var snapshot = _store.ScanSnapshotFrom(new StringKey(""));
            await _store.WriteAsync(new StringKey("b"), Bytes("2"), TestContext.Current.CancellationToken);

            snapshot.Select(e => e.Key.Value).Should().Equal("a", "b");
        }

        [Fact]
        public async Task Snapshot_DoesNotIncludeEntriesFromNewSstFilesCreatedAfterCapture() {
            // capacity 4: a+b fills MemTable to 4 bytes; capturing snapshot before flush
            var tinyStore = await LsmStore<StringKey>.OpenAsync(LsmOptions(4), Empty(), TestContext.Current.CancellationToken);
            await tinyStore.WriteAsync(new StringKey("a"), Bytes("1"), TestContext.Current.CancellationToken);
            await tinyStore.WriteAsync(new StringKey("b"), Bytes("2"), TestContext.Current.CancellationToken);

            // Materialize snapshot before flush so we get a stable baseline
            var snapshot = tinyStore.ScanSnapshotFrom(new StringKey("")).ToList();

            // Writing c triggers flush: a,b→SST, c→new MemTable
            await tinyStore.WriteAsync(new StringKey("c"), Bytes("3"), TestContext.Current.CancellationToken);

            // Full store now has a,b (SST) and c (MemTable)
            tinyStore.ScanAllFrom(new StringKey(""))
                     .Select(e => e.Key.Value)
                     .Should().Equal("a", "b", "c");

            // Snapshot was captured and materialized before the flush — contains only a and b
            snapshot.Select(e => e.Key.Value).Should().Equal("a", "b");
            tinyStore.Dispose();
        }
    }

    public class MultiLevelCompactionTests : LsmStoreTests {
        // Use small thresholds to trigger multi-level compaction in tests
        LsmStoreOptions<StringKey> MultiLevelOptions(int capacityBytes = 4, long baseLevelSizeBytes = 15, int levelSizeMultiplier = 10) =>
            new(_dataDir, capacityBytes, _ => ValueTask.CompletedTask, baseLevelSizeBytes, levelSizeMultiplier);

        [Fact]
        public async Task Compaction_CreatesLevelNamedFiles() {
            // Use small capacity and baseLevelSizeBytes to trigger compaction
            var store = await LsmStore<StringKey>.OpenAsync(
                MultiLevelOptions(capacityBytes: 8, baseLevelSizeBytes: 20), 
                Empty(), TestContext.Current.CancellationToken);
            
            // Write enough to trigger flush to L1 and then compaction to L2
            for (int i = 0; i < 10; i++) {
                await store.WriteAsync(new StringKey($"k{i}"), Bytes("v"), TestContext.Current.CancellationToken);
            }

            var sstFiles = Directory.EnumerateFiles(_dataDir, "*.sst").ToList();
            sstFiles.Should().NotBeEmpty("Expected at least one SST file");
            // With small threshold, should have L2 files from compaction
            sstFiles.Any(f => f.Contains("_L")).Should().BeTrue("Expected level-named files. Files: " + string.Join(", ", sstFiles));
            
            store.Dispose();
        }

        [Fact]
        public async Task Compaction_Level1_TriggersAtSizeTarget() {
            // Use a small baseLevelSizeBytes that will be triggered by a few SST files
            // With capacity=4, each flush creates a small SST file
            var store = await LsmStore<StringKey>.OpenAsync(
                MultiLevelOptions(capacityBytes: 4, baseLevelSizeBytes: 50), 
                Empty(), TestContext.Current.CancellationToken);
            
            // Write enough to create multiple SST files in L1 and trigger compaction to L2
            // Each flush creates one SST file in L1; when L1 size >= 50 bytes, compact to L2
            for (int i = 0; i < 15; i++) {
                await store.WriteAsync(new StringKey($"k{i}"), Bytes("v"), TestContext.Current.CancellationToken);
            }

            // Should have L2 file after compaction
            var sstFiles = Directory.EnumerateFiles(_dataDir, "*.sst").ToList();
            sstFiles.Any(f => f.Contains("_L2.sst")).Should().BeTrue(
                "Expected at least one L2 file after compaction was triggered");
            
            store.Dispose();
        }

        [Fact]
        public async Task Compaction_Cascade_WhenNextLevelFull() {
            // Keys use D2 zero-padding so ordinal and numeric order agree: k00 < k01 < ... < k14.
            // This is important because StringKey uses ordinal comparison, so "k10" < "k2" without padding.
            // With capacity=8, each entry is 3+1=4 bytes; MemTable holds 2 entries (8 bytes) before flushing.
            // With baseLevelSizeBytes=20, each SST file (~30-50 bytes) immediately triggers L1→L2 compaction.
            var store = await LsmStore<StringKey>.OpenAsync(
                new LsmStoreOptions<StringKey>(_dataDir, 8, _ => ValueTask.CompletedTask,
                    BaseLevelSizeBytes: 20, LevelSizeMultiplier: 2),
                Empty(), TestContext.Current.CancellationToken);

            for (int i = 0; i < 15; i++) {
                await store.WriteAsync(new StringKey($"k{i:D2}"), Bytes("v"), TestContext.Current.CancellationToken);
            }

            // Should have files in multiple levels (L2 or L3) after cascade compaction
            Directory.EnumerateFiles(_dataDir, "*.sst")
                     .Should().Contain(f => f.Contains("_L2.sst") || f.Contains("_L3.sst"),
                         "Expected cascade compaction to create L2 or L3 files");

            // Verify all 15 entries are present and in ascending key order
            store.ScanAllFrom(new StringKey(""))
                 .Select(e => e.Key.Value)
                 .Should().Equal(Enumerable.Range(0, 15).Select(i => $"k{i:D2}"));

            store.Dispose();
        }

        [Fact]
        public async Task Compaction_CreatesNewLevel_WhenNeeded() {
            // capacity=8: each entry (3-byte D2 key + 1-byte value = 4 bytes) fits, two entries trigger flush.
            // baseLevelSizeBytes=10: every SST file immediately compacts out of L1.
            // Keys use D2 zero-padding so ordinal and numeric order agree: k00 < k01 < ... < k99.
            var store = await LsmStore<StringKey>.OpenAsync(
                new LsmStoreOptions<StringKey>(_dataDir, 8, _ => ValueTask.CompletedTask,
                    BaseLevelSizeBytes: 10, LevelSizeMultiplier: 10),
                Empty(), TestContext.Current.CancellationToken);

            for (int i = 0; i < 100; i++) {
                await store.WriteAsync(new StringKey($"k{i:D2}"), Bytes("v"), TestContext.Current.CancellationToken);
            }

            // Should have files in L2, L3, or L4 (L1→L2→L3→L4 with multiplier=10)
            Directory.EnumerateFiles(_dataDir, "*.sst")
                     .Should().Contain(f => f.Contains("_L2.sst") || f.Contains("_L3.sst") || f.Contains("_L4.sst"),
                         "Expected multi-level compaction to create L2, L3, or L4 files");

            // All 100 entries must be present and in ascending key order
            store.ScanAllFrom(new StringKey(""))
                 .Select(e => e.Key.Value)
                 .Should().Equal(Enumerable.Range(0, 100).Select(i => $"k{i:D2}"));

            store.Dispose();
        }

        [Fact]
        public async Task Scan_ReturnsAllLevels_InOrder() {
            var store = await LsmStore<StringKey>.OpenAsync(MultiLevelOptions(), Empty(), TestContext.Current.CancellationToken);
            
            // Write to create multiple levels
            await store.WriteAsync(new StringKey("a"), Bytes("1"), TestContext.Current.CancellationToken);
            await store.WriteAsync(new StringKey("b"), Bytes("2"), TestContext.Current.CancellationToken);
            await store.WriteAsync(new StringKey("c"), Bytes("3"), TestContext.Current.CancellationToken);
            await store.WriteAsync(new StringKey("d"), Bytes("4"), TestContext.Current.CancellationToken);
            await store.WriteAsync(new StringKey("e"), Bytes("5"), TestContext.Current.CancellationToken);

            store.ScanAllFrom(new StringKey(""))
                 .Select(e => e.Key.Value)
                 .Should().Equal("a", "b", "c", "d", "e");

            store.Dispose();
        }

        [Fact]
        public async Task ExistingFiles_WithoutLevelSuffix_DefaultToL1() {
            // First, create an LsmStore to get a valid SST file
            // Use capacity that fits 2 entries, write exactly 2 to get 1 SST file
            var tempStore = await LsmStore<StringKey>.OpenAsync(
                MultiLevelOptions(capacityBytes: 10, baseLevelSizeBytes: long.MaxValue), 
                Empty(), TestContext.Current.CancellationToken);
            // entrySize = 2 bytes (1 byte key + 1 byte value), capacity=10
            // Write 5 entries: Size = 10, next write triggers flush
            await tempStore.WriteAsync(new StringKey("a"), Bytes("1"), TestContext.Current.CancellationToken);
            await tempStore.WriteAsync(new StringKey("b"), Bytes("2"), TestContext.Current.CancellationToken);
            await tempStore.WriteAsync(new StringKey("c"), Bytes("3"), TestContext.Current.CancellationToken);
            await tempStore.WriteAsync(new StringKey("d"), Bytes("4"), TestContext.Current.CancellationToken);
            await tempStore.WriteAsync(new StringKey("e"), Bytes("5"), TestContext.Current.CancellationToken);
            // Size is now 10 (5 entries x 2 bytes). Next write will trigger flush.
            await tempStore.WriteAsync(new StringKey("f"), Bytes("6"), TestContext.Current.CancellationToken);
            // Now Size = 12 > 10, so a, b, c, d, e are flushed to SST, f is in MemTable
            // But we want all entries in SST, so write one more to trigger another flush
            tempStore.Dispose();
            
            // Rename the file to remove the level suffix
            var sstFiles = Directory.EnumerateFiles(_dataDir, "*.sst").ToList();
            sstFiles.Should().HaveCount(1, "Expected 1 SST file from tempStore. Files: " + string.Join(", ", sstFiles.Select(f => Path.GetFileName(f))));
            var oldPath = sstFiles[0];
            var newPath = Path.Combine(_dataDir, "oldfile.sst");
            File.Move(oldPath, newPath);
            
            // Now open a new store - it should load the old-style file into L1
            var store = await LsmStore<StringKey>.OpenAsync(
                MultiLevelOptions(capacityBytes: 4096, baseLevelSizeBytes: long.MaxValue), 
                Empty(), TestContext.Current.CancellationToken);
            
            // The old file should be in L1 and readable (contains a, b, c, d, e from the flush; f was in MemTable and not flushed)
            var entries = store.ScanAllFrom(new StringKey("")).ToList();
            entries.Should().HaveCount(5, "Expected 5 entries from the tempStore flush");
            entries.Select(e => e.Key.Value).Should().BeEquivalentTo(new[] { "a", "b", "c", "d", "e" });
            
            store.Dispose();
        }

        [Fact]
        public async Task BloomFilterPath_ReturnsCorrectSuffix() {
            var info = new SstFileInfo("file_L2.sst", 2, 100);
            info.BloomFilterPath.Should().Be("file_L2.sst.bf");
        }
    }
}
