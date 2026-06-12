using FluentAssertions;
using MSSP.Engine.Storage;

namespace MSSP.Cluster;

public class LsmSnapshotTests {
    static LsmStoreOptions<StringKey> Opts(string dir) =>
        new(dir, 4, _ => ValueTask.CompletedTask);

    static async IAsyncEnumerable<ReadOnlyMemory<byte>> NoWal(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken _ = default) {
        await Task.Yield();
        yield break;
    }

    static Memory<byte> Bytes(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    public class RoundTrip {
        [Fact]
        public void PreservesAllSstAndBfFiles() {
            var source = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            var target = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            try {
                Directory.CreateDirectory(source);
                var sstBytes = new byte[] { 1, 2, 3, 4, 5 };
                var bfBytes  = new byte[] { 10, 20 };
                File.WriteAllBytes(Path.Combine(source, "00001.sst"), sstBytes);
                File.WriteAllBytes(Path.Combine(source, "00001.bf"),  bfBytes);

                var archive = LsmSnapshot.Serialize(source);
                LsmSnapshot.Deserialize(archive, target);

                File.ReadAllBytes(Path.Combine(target, "00001.sst")).Should().Equal(sstBytes);
                File.ReadAllBytes(Path.Combine(target, "00001.bf")).Should().Equal(bfBytes);
            } finally {
                if (Directory.Exists(source)) Directory.Delete(source, recursive: true);
                if (Directory.Exists(target)) Directory.Delete(target, recursive: true);
            }
        }

        [Fact]
        public void EmptyDirectory_ProducesArchiveThatDeserializesClean() {
            var source = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            var target = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            try {
                Directory.CreateDirectory(source);
                var archive = LsmSnapshot.Serialize(source);
                LsmSnapshot.Deserialize(archive, target);
                Directory.GetFiles(target).Should().BeEmpty();
            } finally {
                if (Directory.Exists(source)) Directory.Delete(source, recursive: true);
                if (Directory.Exists(target)) Directory.Delete(target, recursive: true);
            }
        }
    }

    public class ReloadAsync {
        [Fact]
        public async Task ReplacesSstFilesAndResetsMemTable() {
            var storeDir  = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            var sourceDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            var stagingDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            try {
                Directory.CreateDirectory(storeDir);
                Directory.CreateDirectory(sourceDir);

                // store under test: write a, b → fills 4-byte MemTable → flush to SST;
                // c stays in MemTable (unflushed)
                using var store = await LsmStore<StringKey>.OpenAsync(Opts(storeDir), NoWal(), TestContext.Current.CancellationToken);
                await store.WriteAsync(new StringKey("a"), Bytes("1"), TestContext.Current.CancellationToken);
                await store.WriteAsync(new StringKey("b"), Bytes("2"), TestContext.Current.CancellationToken);
                await store.WriteAsync(new StringKey("c"), Bytes("3"), TestContext.Current.CancellationToken);

                store.ScanAllFrom(new StringKey(""))
                     .Select(e => e.Key.Value)
                     .Should().Contain("a", "setup: old data must be visible before reload");

                // source store: write x, y → flush to SST in sourceDir
                using var source = await LsmStore<StringKey>.OpenAsync(Opts(sourceDir), NoWal(), TestContext.Current.CancellationToken);
                await source.WriteAsync(new StringKey("x"), Bytes("10"), TestContext.Current.CancellationToken);
                await source.WriteAsync(new StringKey("y"), Bytes("20"), TestContext.Current.CancellationToken);
                await source.WriteAsync(new StringKey("z"), Bytes("30"), TestContext.Current.CancellationToken);
                source.Dispose();

                // serialize source SST files into an archive and unpack to staging
                var archive = LsmSnapshot.Serialize(sourceDir);
                LsmSnapshot.Deserialize(archive, stagingDir);

                // reload: replace store's SST files with those from staging
                await store.ReloadAsync(stagingDir, TestContext.Current.CancellationToken);

                var keys = store.ScanAllFrom(new StringKey("")).Select(e => e.Key.Value).ToList();
                keys.Should().Contain("x").And.Contain("y");
                keys.Should().NotContain("a", "old SST entry must be gone after reload");
                keys.Should().NotContain("c", "unflushed MemTable entry must be gone after reload");
            } finally {
                if (Directory.Exists(storeDir))   Directory.Delete(storeDir,   recursive: true);
                if (Directory.Exists(sourceDir))  Directory.Delete(sourceDir,  recursive: true);
                if (Directory.Exists(stagingDir)) Directory.Delete(stagingDir, recursive: true);
            }
        }
    }
}
