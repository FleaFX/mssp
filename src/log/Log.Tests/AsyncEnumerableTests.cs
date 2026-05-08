using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using FluentAssertions;

namespace Log;

public class AsyncEnumerableTests {
    [Fact]
    [SuppressMessage("ReSharper", "MethodSupportsCancellation")]
    public async Task FromEventPattern_ConcurrentEventFirings_NeverThrow() {
        for (var i = 0; i < 1000; i++) {
            Action<object, int>? handler = null;
            void Add(Action<object, int> h) => handler = h;
            void Remove(Action<object, int> h) { }

            using var cts = new CancellationTokenSource();
            var enumerator = Extensions.AsyncEnumerable
                .FromEventPattern<Action<object, int>, int>(Add, Remove, cts.Token)
                .GetAsyncEnumerator();

            var moveNext = enumerator.MoveNextAsync().AsTask();
            while (handler == null) await Task.Yield(); // wait until handler is attached

            // synchronize both threads to maximise the chance of a concurrent SetResult call
            using var barrier = new Barrier(2);
            var exceptions = new ConcurrentBag<Exception>();

            var t1 = Task.Run(() => { barrier.SignalAndWait(); try { handler!.Invoke(null!, 1); } catch (Exception ex) { exceptions.Add(ex); } });
            var t2 = Task.Run(() => { barrier.SignalAndWait(); try { handler!.Invoke(null!, 2); } catch (Exception ex) { exceptions.Add(ex); } });

            await Task.WhenAll(t1, t2);
            exceptions.Should().BeEmpty("concurrent event firings must not throw InvalidOperationException");

            await moveNext;
            await cts.CancelAsync();
            try { await enumerator.MoveNextAsync(); } catch (OperationCanceledException) { }
            await enumerator.DisposeAsync();
        }
    }

    [Fact]
    [SuppressMessage("ReSharper", "MethodSupportsCancellation")]
    public async Task FromEventPattern_RemovesHandlerOnCancellation() {
        var attached = 0;

        void Add(Action<object, int> h) => attached++;
        void Remove(Action<object, int> h) => attached--;

        using var cts = new CancellationTokenSource();
        var enumerator = Extensions.AsyncEnumerable.FromEventPattern<Action<object, int>, int>(Add, Remove, cts.Token).GetAsyncEnumerator();

        var moveNext = enumerator.MoveNextAsync().AsTask();
        await Task.Delay(50); // let it attach the handler and start waiting

        attached.Should().Be(1);
        await cts.CancelAsync();

        try { await moveNext; } catch (OperationCanceledException) { }

        attached.Should().Be(0, "handler should be detached after cancellation");
    }
}
