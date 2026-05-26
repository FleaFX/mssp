namespace MSSP.Raft;

public sealed partial class RaftNode {

    /// <summary>
    /// Applies all committed but not yet applied log entries to the state machine, in order.
    /// </summary>
    /// <remarks>
    /// <see cref="IRaftStateMachine.LastAppliedIndex"/> is the authoritative source of progress;
    /// the actor does not track a separate <c>_lastApplied</c> field to avoid divergence.
    /// </remarks>
    async Task ApplyCommittedEntriesAsync() {
        while (_stateMachine.LastAppliedIndex < _commitIndex) {
            var idx   = _stateMachine.LastAppliedIndex + 1;
            var entry = await _log.GetEntryAsync(idx);
            var ok    = await _stateMachine.ApplyAsync(entry);

            if (_role == NodeRole.Leader && _pendingProposals!.Remove(idx, out var tcs)) {
                if (entry.Type == RaftLogEntryType.NoOp) {
                    _noOpCommitted = true;
                    tcs.TrySetResult(new RaftApplyResult(IsOccConflict: false));
                } else {
                    tcs.TrySetResult(new RaftApplyResult(IsOccConflict: !ok));
                }
            } else if (entry.Type == RaftLogEntryType.NoOp && _role == NodeRole.Leader) {
                _noOpCommitted = true;
            }
        }
    }

    /// <summary>
    /// Persists <see cref="_currentTerm"/> and <see cref="_votedFor"/> to durable storage.
    /// </summary>
    /// <remarks>
    /// Called after granting a vote and after every term change (step-down or election start).
    /// </remarks>
    ValueTask PersistStateAsync() =>
        _stateStorage.SaveAsync(new RaftPersistentState(_currentTerm, _votedFor));

    /// <summary>
    /// Fails all pending proposals with <see cref="NotLeaderException"/>. Safe to call when
    /// <see cref="_pendingProposals"/> is <see langword="null"/> (no-op).
    /// </summary>
    void FailPendingProposals() {
        if (_pendingProposals is null) return;
        foreach (var tcs in _pendingProposals.Values)
            tcs.TrySetException(new NotLeaderException(_leaderHint));
        _pendingProposals.Clear();
    }

    /// <summary>
    /// Injects a message directly into the actor channel. Only used by tests.
    /// </summary>
    internal void Inject(RaftMessage message) => _channel.Writer.TryWrite(message);

    /// <summary>
    /// Returns a task that completes after the actor has processed all previously injected messages.
    /// Achieved by posting a <see cref="DrainSentinel"/> and awaiting its completion source.
    /// Only used by tests.
    /// </summary>
    internal Task WhenIdleAsync(CancellationToken ct = default) {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // TryWrite returns false when the channel is already completed (node stopped).
        // In that case complete the TCS immediately so the caller doesn't hang.
        if (!_channel.Writer.TryWrite(new DrainSentinel(tcs)))
            tcs.TrySetCanceled();
        return tcs.Task.WaitAsync(ct);
    }

    /// <summary>
    /// Simulates the election timer firing at the current generation without waiting for the
    /// real delay. Waits until the actor has finished processing the resulting transition.
    /// Only used by tests.
    /// </summary>
    internal async Task TriggerElectionTimerAsync() {
        Inject(new ElectionTimerFired(_electionTimerGeneration));
        await WhenIdleAsync();
    }
}
