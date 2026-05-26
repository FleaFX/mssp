using System.Text;
using FluentAssertions;
using MSSP.Storage;

namespace MSSP.BloomFilters;

public class BloomFilteredSstAccessTests : IDisposable {
    readonly string _dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

    public BloomFilteredSstAccessTests() => Directory.CreateDirectory(_dir);

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    string SstPath(string name) => Path.Combine(_dir, name + ".sst");
    string BfPath(string name) => Path.Combine(_dir, name + ".bf");

    static KeyValuePair<StringKey, ReadOnlyMemory<byte>?> Entry(string key, string value) =>
        new(new StringKey(key), (ReadOnlyMemory<byte>)Encoding.UTF8.GetBytes(value));

    [Fact]
    public async Task WriteAsync_CreatesBfSidecar() {
        var sst = new BloomFilteredSstAccess<StringKey>(new DefaultSstAccess<StringKey>());

        await sst.WriteAsync([Entry("a", "1"), Entry("b", "2")], SstPath("test"), TestContext.Current.CancellationToken);

        File.Exists(BfPath("test")).Should().BeTrue();
    }

    [Fact]
    public async Task WriteAsync_SstFileIsReadable() {
        var sst = new BloomFilteredSstAccess<StringKey>(new DefaultSstAccess<StringKey>());
        await sst.WriteAsync([Entry("a", "1"), Entry("b", "2")], SstPath("test"), TestContext.Current.CancellationToken);

        using var reader = sst.OpenReader(SstPath("test"));
        reader.TryGet(new StringKey("a"), out var value).Should().BeTrue();
        Encoding.UTF8.GetString(value!.Value.Span).Should().Be("1");
    }

    [Fact]
    public async Task OpenReader_SkipsDiskRead_WhenKeyDefinitelyAbsent() {
        var sst = new BloomFilteredSstAccess<StringKey>(new DefaultSstAccess<StringKey>());
        await sst.WriteAsync([Entry("a", "1"), Entry("b", "2")], SstPath("test"), TestContext.Current.CancellationToken);

        using var reader = sst.OpenReader(SstPath("test"));
        reader.TryGet(new StringKey("zzz-not-present"), out var value).Should().BeFalse();
        value.Should().BeNull();
    }

    [Fact]
    public async Task OpenReader_FallsBackToUnfilteredReader_WhenSidecarAbsent() {
        var inner = new DefaultSstAccess<StringKey>();
        await inner.WriteAsync([Entry("a", "1")], SstPath("no-bf"), TestContext.Current.CancellationToken);

        var sst = new BloomFilteredSstAccess<StringKey>(inner);
        using var reader = sst.OpenReader(SstPath("no-bf"));

        reader.TryGet(new StringKey("a"), out var value).Should().BeTrue();
        Encoding.UTF8.GetString(value!.Value.Span).Should().Be("1");
    }

    [Fact]
    public async Task Delete_RemovesBothSstAndBfSidecar() {
        var sst = new BloomFilteredSstAccess<StringKey>(new DefaultSstAccess<StringKey>());
        await sst.WriteAsync([Entry("a", "1")], SstPath("del"), TestContext.Current.CancellationToken);

        sst.Delete(SstPath("del"));

        File.Exists(SstPath("del")).Should().BeFalse();
        File.Exists(BfPath("del")).Should().BeFalse();
    }

    [Fact]
    public async Task ScanReturnsAllEntries() {
        var sst = new BloomFilteredSstAccess<StringKey>(new DefaultSstAccess<StringKey>());
        await sst.WriteAsync([Entry("a", "1"), Entry("b", "2"), Entry("c", "3")], SstPath("scan"), TestContext.Current.CancellationToken);

        using var reader = sst.OpenReader(SstPath("scan"));
        reader.Scan()
              .Select(e => e.Key.Value)
              .Should().Equal("a", "b", "c");
    }
}
