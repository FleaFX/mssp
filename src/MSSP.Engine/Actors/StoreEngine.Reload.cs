namespace MSSP.Engine;

sealed partial class StoreEngine {
    /// <summary>
    /// Replaces the store contents from a snapshot directory and resets in-memory state.
    /// </summary>
    public ValueTask ReloadSnapshotAsync(string stagingDirectory, CancellationToken cancellationToken) {
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_mailbox.Writer.TryWrite(new ReloadSnapshotCommand(stagingDirectory, tcs)))
            tcs.TrySetException(new ObjectDisposedException(nameof(StoreEngine)));
        return new ValueTask(tcs.Task.WaitAsync(cancellationToken));
    }

    async ValueTask HandleReloadSnapshot(ReloadSnapshotCommand cmd, CancellationToken ct) {
        try {
            await store.ReloadAsync(cmd.StagingDirectory, ct);

            _currentPosition = subscriptionLog.GetLastPosition().Value;
            _nextPosition = (long)_currentPosition;
            _revisions.Clear();

            cmd.Reply.TrySetResult(true);
        } catch (Exception ex) {
            cmd.Reply.TrySetException(ex);
        }
    }
}
