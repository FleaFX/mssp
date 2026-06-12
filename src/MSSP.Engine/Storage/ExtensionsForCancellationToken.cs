using System.Runtime.CompilerServices;

namespace MSSP.Engine.Storage;

static class ExtensionsForCancellationToken {
    /// <summary>
    /// Makes a <see cref="CancellationToken"/> directly awaitable, completing when the token is cancelled.
    /// </summary>
    /// <returns><see langword="true"/> when cancellation was requested.</returns>
    public static ValueTaskAwaiter<bool> GetAwaiter(this CancellationToken cancellationToken) {
        async ValueTask<bool> AsValueTask(CancellationToken token) {
            if (token.IsCancellationRequested) return true;
            var tcs = new TaskCompletionSource<bool>();
            // double-check after allocation to close the race between the first check and Register
            if (token.IsCancellationRequested) tcs.SetResult(true);
            else token.Register(s => ((TaskCompletionSource<bool>)s!).SetResult(true), tcs);
            return await tcs.Task;
        }

        return AsValueTask(cancellationToken).GetAwaiter();
    }
}
