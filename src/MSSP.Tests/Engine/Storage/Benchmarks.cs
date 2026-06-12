using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using MSSP.Storage;

namespace MSSP.Engine.Storage;

public class Benchmarks {
#if RELEASE
    [Fact]
#else
    [Fact(Skip = "To run the benchmark, build in Release config.")]
#endif
    public void LogIndex() => BenchmarkRunner.Run<LogIndexBenchmarks>();
}

[MemoryDiagnoser]
public class LogIndexBenchmarks : IDisposable {
    readonly LogIndex _index;
    readonly Task[] _enumerationTasks = null!;
    readonly CancellationTokenSource _cts = null!;

    [Params(1, 4, 8)]
    public int ListenerCount { get; set; }

    /// <summary>
    /// Initializes a new <see cref="LogIndexBenchmarks"/>.
    /// </summary>
    public LogIndexBenchmarks() {
        _index = new LogIndex();

        _cts = new CancellationTokenSource();

        _enumerationTasks = [
            ..
            from i in Enumerable.Range(0, ListenerCount)
            select Task.Run(async () => {
                try {
                    await foreach (var item in _index.WithCancellation(_cts.Token)) {
                        _ = item;
                    }
                } catch (OperationCanceledException) { }
            })
        ];
    }

    /// <summary>
    /// Benchmarks advancing the index for a single payload.
    /// </summary>
    [Benchmark]
    public void Advance() {
        _index.Advance(0x3c00); // simulate 15k payload
        _index.Truncate();
    }

    /// <summary>
    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
    /// </summary>
    public void Dispose() {
        _cts.Cancel();
        try {
            Task.WaitAll(_enumerationTasks);
        } catch (AggregateException ex) when (ex.InnerExceptions.All(e => e is TaskCanceledException)) { }

        _index.Dispose();
    }
}