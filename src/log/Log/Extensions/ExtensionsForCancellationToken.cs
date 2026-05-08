using System.Runtime.CompilerServices;

namespace Log.Extensions;

static class ExtensionsForCancellationToken {
    /// <summary>
    /// Makes a <see cref="CancellationToken"/> directly awaitable, completing when the token is cancelled.
    /// </summary>
    /// <returns><see langword="true"/> when cancellation was requested.</returns>
    public static ValueTaskAwaiter<bool> GetAwaiter(this CancellationToken cancellationToken) {
        async ValueTask<bool> AsValueTask(CancellationToken ct) {
            if (ct.IsCancellationRequested) return true;
            var tcs = new TaskCompletionSource<bool>();
            // double-check after allocation to close the race between the first check and Register
            if (ct.IsCancellationRequested) tcs.SetResult(true);
            else ct.Register(s => ((TaskCompletionSource<bool>)s!).SetResult(true), tcs);
            return await tcs.Task;
        }

        return AsValueTask(cancellationToken).GetAwaiter();
    }
}
