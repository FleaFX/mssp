using System.Runtime.CompilerServices;

namespace Log.Extensions;

static class ExtensionsForCancellationToken {
    public static ValueTaskAwaiter<bool> GetAwaiter(this CancellationToken cancellationToken) {
        async ValueTask<bool> AsValueTask(CancellationToken ct) {
            if (ct.IsCancellationRequested) return true;
            var tcs = new TaskCompletionSource<bool>();
            if (ct.IsCancellationRequested) tcs.SetResult(true);
            else ct.Register(s => ((TaskCompletionSource<bool>)s!).SetResult(true), tcs);
            return await tcs.Task;
        }

        return AsValueTask(cancellationToken).GetAwaiter();
    }
}