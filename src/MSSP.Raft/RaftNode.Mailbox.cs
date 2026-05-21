using System.Threading.Channels;

namespace MSSP.Raft;

public sealed partial class RaftNode {
    readonly Channel<Func<Task>> _mailbox = Channel.CreateUnbounded<Func<Task>>(
        new UnboundedChannelOptions { SingleReader = true });
    Task? _mailboxTask;
    CancellationTokenSource? _cts;

    void Post(Func<Task> work) => _mailbox.Writer.TryWrite(work);

    void Post(Action work) => _mailbox.Writer.TryWrite(() => {
        work();
        return Task.CompletedTask;
    });

    async Task RunMailboxAsync(CancellationToken cancellationToken) {
        await foreach (var work in _mailbox.Reader.ReadAllAsync(cancellationToken))
            try { await work(); } catch { /* individual work items handle their own errors */ }
    }
}
