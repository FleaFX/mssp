using FluentAssertions;
using Log.Extensions;

namespace Log;

public class LogIndexTests {
    [Fact]
    public void Constructor() {
        using var index = new LogIndex();
        index.Head.Should().Be(Index.Start);
    }

    [Fact]
    public void CopyConstructor() {
        using var index = new LogIndex(new Index[] { 0, 0x356, 0x3fbe, 0x3fd3, 0, 0, 0, 0, 0, 0, 0 }); // notice the zeroes, these should be trimmed out
        index.Length.Should().Be(3);
        index.Head.Should().Be(new Index(0x3fd3));
    }

    [Fact]
    public void Indexer() {
        using var index = new LogIndex();
        index.Advance(0x356);
        index.Advance(0x3c68);
        index.Advance(0x15);
        index[0].Should().Be(new Index(0x356));
        index[1].Should().Be(new Index(0x356 + 0x3c68));
        index[2].Should().Be(new Index(0x356 + 0x3c68 + 0x15));
        var outsideOfBounds = () => { var _ = index[3]; };
        outsideOfBounds.Should().Throw<IndexOutOfRangeException>();
    }

    [Fact]
    public async Task Enumeration() {
        using var index = new LogIndex();
        index.Length.Should().Be(0);
        index.Head.Should().Be(Index.Start);
        (await index.EnumerateAsync()).Should().BeEquivalentTo(Array.Empty<Index>());

        index.Advance(10);
        index.Length.Should().Be(1);
        index.Head.Should().Be(new Index(10));
        (await index.EnumerateAsync()).Should().BeEquivalentTo(new Index[] { 10 });

        index.Advance(5);
        index.Length.Should().Be(2);
        index.Head.Should().Be(new Index(15));
        (await index.EnumerateAsync()).Should().BeEquivalentTo(new Index[] { 10, 15 });

        index.Advance(3);
        index.Length.Should().Be(3);
        index.Head.Should().Be(new Index(18));
        (await index.EnumerateAsync()).Should().BeEquivalentTo(new Index[] { 10, 15, 18 });

        index.Advance(25);
        index.Length.Should().Be(4);
        index.Head.Should().Be(new Index(43));
        (await index.EnumerateAsync()).Should().BeEquivalentTo(new Index[] { 10, 15, 18, 43 });
    }

#if RELEASE
    [Fact]
#else
    [Fact(Skip = "Long running test. Run in Release config to run it.")]
#endif
    public async Task LongRunningEnumeration() {
        using var index = new LogIndex();

        var cts = new CancellationTokenSource();

        var consumeTask = Task.Run(() => index.ToBlockingEnumerable(cts.Token));

        var produceTask = Task.Run(async () => {
            // produce some values synchronously
            foreach (var value in Enumerable.Range(1, 10)) {
                index.Advance(value);
            }

            // wait a bit
            await Task.Delay(TimeSpan.FromSeconds(5));

            // and produce some more
            index.Advance(11);
            index.Advance(12);

            cts.Cancel();
        });

        await Task.WhenAll(produceTask, consumeTask);

        consumeTask.Result.Should().BeEquivalentTo(new[] {
            new Index(1), new Index(3), new Index(6), new Index(10), new Index(15), new Index(21), new Index(28), new Index(36), new Index(45), new Index(55),
            // delay went here
            new Index(66), new Index(78)
        });
    }
}