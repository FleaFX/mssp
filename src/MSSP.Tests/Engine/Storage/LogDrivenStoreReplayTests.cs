using System.Text;
using System.Threading.Channels;
using FluentAssertions;
using MSSP.Storage;

namespace MSSP.Engine.Storage;

public class LogDrivenStoreReplayTests : IAsyncLifetime {
    readonly string _dataDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

    public ValueTask InitializeAsync() {
        Directory.CreateDirectory(_dataDir);
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() {
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Log that feeds entries into a channel for the apply loop and counts how many
    /// times <see cref="TryAppendAsync"/> was called, letting tests verify that
    /// <see cref="LogDrivenStore{TKey}.ReplayAsync"/> never calls through the log.
    /// </summary>
    sealed class CountingLog : ILog<WalRecord> {
        readonly Channel<WalRecord[]> _channel = Channel.CreateUnbounded<WalRecord[]>(
            new UnboundedChannelOptions { SingleReader = true });

        public int AppendCount { get; private set; }

        public ValueTask<bool> TryAppendAsync(WalRecord record, CancellationToken cancellationToken = default) {
            AppendCount++;
            _channel.Writer.TryWrite([record]);
            return ValueTask.FromResult(true);
        }

        public IAsyncEnumerator<WalRecord[]> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
            _channel.Reader.ReadAllAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);
    }

    async Task<(LsmStore<StringKey> lsm, LogDrivenStore<StringKey> logDriven, CountingLog log)> CreateAsync(int capacity = 4096) {
        var lsm = await LsmStore<StringKey>.OpenAsync(
            new LsmStoreOptions<StringKey>(_dataDir, capacity, _ => ValueTask.CompletedTask, BaseLevelSizeBytes: -1, LevelSizeMultiplier: 10),
            AsyncEnumerable.Empty<ReadOnlyMemory<byte>>(),
            TestContext.Current.CancellationToken);
        var log = new CountingLog();
        // LogDrivenStore.Dispose() also disposes lsm via _inner.Dispose().
        var logDriven = LogDrivenStore<StringKey>.Create(log, lsm, capacity);
        return (lsm, logDriven, log);
    }

    static Memory<byte> Bytes(string s) => Encoding.UTF8.GetBytes(s);

    [Fact]
    public async Task ReplayAsync_WritesEntryToInnerStore() {
        var (lsm, logDriven, _) = await CreateAsync();
        using (logDriven) {
            var payload = (ReadOnlyMemory<byte>)WalRecord.From(new StringKey("replayed"), Bytes("val"));
            await logDriven.ReplayAsync(payload, TestContext.Current.CancellationToken);

            lsm.ScanAllFrom(new StringKey("replayed"))
               .Should().ContainSingle(e => e.Key == new StringKey("replayed"));
        }
    }

    [Fact]
    public async Task ReplayAsync_DoesNotCallTryAppendAsync() {
        // ReplayAsync must bypass the log entirely — writing to the log would put
        // an entry in the channel and risk prematurely resolving a pending write TCS.
        var (_, logDriven, log) = await CreateAsync();
        using (logDriven) {
            var payload = (ReadOnlyMemory<byte>)WalRecord.From(new StringKey("key"), Bytes("val"));
            await logDriven.ReplayAsync(payload, TestContext.Current.CancellationToken);

            log.AppendCount.Should().Be(0,
                "ReplayAsync must write directly to the inner store, not through the log");
        }
    }

    [Fact]
    public async Task ReplayAsync_MultipleEntries_AllVisibleInScan() {
        var (lsm, logDriven, _) = await CreateAsync();
        using (logDriven) {
            for (var i = 0; i < 5; i++) {
                var payload = (ReadOnlyMemory<byte>)WalRecord.From(new StringKey($"replay-{i}"), Bytes($"val-{i}"));
                await logDriven.ReplayAsync(payload, TestContext.Current.CancellationToken);
            }

            lsm.ScanAllFrom(new StringKey(""))
               .Select(e => e.Key.Value)
               .Should().BeEquivalentTo(["replay-0", "replay-1", "replay-2", "replay-3", "replay-4"]);
        }
    }

    [Fact]
    public async Task ReplayAsync_IgnoresMalformedPayload() {
        var (_, logDriven, _) = await CreateAsync();
        using (logDriven) {
            // A payload shorter than the 5-byte minimum header must be silently skipped.
            var act = async () => await logDriven.ReplayAsync(new byte[] { 0x01, 0x00 }, TestContext.Current.CancellationToken);
            await act.Should().NotThrowAsync();
        }
    }

    [Fact]
    public async Task ReplayAsync_ThenWrite_WriteCompletesSuccessfully() {
        // The core regression guard: replay entries bypass the channel, so the apply loop
        // never processes them. A subsequent WriteAsync therefore has exactly one TCS in
        // _pending and exactly one channel entry — they match correctly and the write
        // completes rather than hanging or resolving prematurely.
        var (lsm, logDriven, _) = await CreateAsync();
        using (logDriven) {
            var replayPayload = (ReadOnlyMemory<byte>)WalRecord.From(new StringKey("replayed"), Bytes("old"));
            await logDriven.ReplayAsync(replayPayload, TestContext.Current.CancellationToken);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await logDriven.WriteAsync(new StringKey("written"), Bytes("new"), cts.Token);

            lsm.ScanAllFrom(new StringKey(""))
               .Select(e => e.Key.Value)
               .Should().Contain("replayed").And.Contain("written");
        }
    }

    [Fact]
    public async Task ReplayAsync_ManyReplays_ThenManyWrites_AllDataPresent() {
        var (lsm, logDriven, _) = await CreateAsync();
        using (logDriven) {
            for (var i = 0; i < 10; i++) {
                var payload = (ReadOnlyMemory<byte>)WalRecord.From(new StringKey($"r-{i}"), Bytes($"rv-{i}"));
                await logDriven.ReplayAsync(payload, TestContext.Current.CancellationToken);
            }

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            for (var i = 0; i < 5; i++)
                await logDriven.WriteAsync(new StringKey($"w-{i}"), Bytes($"wv-{i}"), cts.Token);

            var keys = lsm.ScanAllFrom(new StringKey("")).Select(e => e.Key.Value).ToList();
            keys.Should().HaveCount(15);
            for (var i = 0; i < 10; i++) keys.Should().Contain($"r-{i}");
            for (var i = 0;  i < 5; i++) keys.Should().Contain($"w-{i}");
        }
    }
}
