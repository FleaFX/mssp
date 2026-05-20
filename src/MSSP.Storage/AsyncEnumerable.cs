using System.Runtime.CompilerServices;

namespace MSSP.Storage;

static class StorageAsyncEnumerable {
    /// <summary>
    /// Creates a <see cref="IAsyncEnumerable{T}"/> that produces a <typeparamref name="TEventArgs"/> every time an event fires.
    /// </summary>
    /// <typeparam name="TDelegate">The type of the event.</typeparam>
    /// <typeparam name="TEventArgs">The type of the produced event arguments.</typeparam>
    /// <param name="addHandler">A <see cref="Action"/> that attaches the given <typeparamref name="TDelegate"/> to the event to subscribe to.</param>
    /// <param name="removeHandler">A <see cref="Action"/> that detaches the given <typeparamref name="TDelegate"/> from the event to subscribe to.</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> that, when signaled, completes the enumeration.</param>
    /// <returns>A <see cref="IAsyncEnumerable{T}"/>.</returns>
    public static async IAsyncEnumerable<TEventArgs> FromEventPattern<TDelegate, TEventArgs>(Action<TDelegate> addHandler, Action<TDelegate> removeHandler, [EnumeratorCancellation] CancellationToken cancellationToken = new()) where TDelegate : Delegate {
        while (!cancellationToken.IsCancellationRequested) {
            // a TaskCompletionSource only transitions to RanToCompletion once, so we'll make a new one in each iteration
            var taskCompletionSource = new TaskCompletionSource<TEventArgs>();

            // create a delegate that passes the event args to the TaskCompletionSource and attach it to the event
            var @delegate = typeof(Action<object, TEventArgs>).GetMethod("Invoke")!
                .CreateDelegate<TDelegate>(new Action<object, TEventArgs>((_, args) => {
                    taskCompletionSource.TrySetResult(args);
                }));
            addHandler(@delegate);

            // now wait for the TaskCompletionSource to be signaled and yield the result
            try {
                yield return await taskCompletionSource.Task.WaitAsync(cancellationToken);
            } finally {
                // clean up the event handler before the next iteration
                removeHandler(@delegate);
            }
        };
    }
}
