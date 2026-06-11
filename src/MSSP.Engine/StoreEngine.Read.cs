using MSSP.Storage;

namespace MSSP.Engine;

sealed partial class StoreEngine {
    /// <summary>
    /// Schedules a read snapshot on the actor thread.
    /// The returned snapshot holds open SST file handles and must be disposed by the caller.
    /// </summary>
    public ValueTask<LsmStoreSnapshot<EventKey>> CaptureSnapshotAsync(CancellationToken cancellationToken) {
        var tcs = new TaskCompletionSource<LsmStoreSnapshot<EventKey>>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_mailbox.Writer.TryWrite(new CaptureSnapshotCommand(tcs)))
            tcs.TrySetException(new ObjectDisposedException(nameof(StoreEngine)));
        return new ValueTask<LsmStoreSnapshot<EventKey>>(tcs.Task.WaitAsync(cancellationToken));
    }

    /// <summary>
    /// Opens SST file handles on the actor thread suitable for streaming into a backup archive.
    /// The caller is responsible for disposing each returned stream.
    /// </summary>
    public ValueTask<IReadOnlyList<FileStream>> OpenBackupStreamsAsync(CancellationToken cancellationToken) {
        var tcs = new TaskCompletionSource<IReadOnlyList<FileStream>>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_mailbox.Writer.TryWrite(new OpenBackupStreamsCommand(tcs)))
            tcs.TrySetException(new ObjectDisposedException(nameof(StoreEngine)));
        return new ValueTask<IReadOnlyList<FileStream>>(tcs.Task.WaitAsync(cancellationToken));
    }

    ValueTask HandleCaptureSnapshot(CaptureSnapshotCommand cmd) {
        try {
            cmd.Reply.TrySetResult(pipeline.TakeReadSnapshot());
        } catch (Exception ex) {
            cmd.Reply.TrySetException(ex);
        }
        return ValueTask.CompletedTask;
    }

    ValueTask HandleOpenBackupStreams(OpenBackupStreamsCommand cmd) {
        try {
            cmd.Reply.TrySetResult(pipeline.OpenBackupStreams());
        } catch (Exception ex) {
            cmd.Reply.TrySetException(ex);
        }
        return ValueTask.CompletedTask;
    }
}
