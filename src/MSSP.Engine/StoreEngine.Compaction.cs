namespace MSSP.Engine;

sealed partial class StoreEngine {
    bool _compactionPlanPending;

    ValueTask HandleCompactionPlanRequest(CompactionPlanRequest msg) {
        if (store.PlanCompaction() is { } job)
            _compaction!.Respond(job);
        else
            _compactionPlanPending = true;
        return ValueTask.CompletedTask;
    }

    void TryFulfillPendingPlanRequest() {
        if (!_compactionPlanPending) return;
        if (store.PlanCompaction() is not { } job) return;
        _compactionPlanPending = false;
        _compaction!.Respond(job);
    }

    async ValueTask HandleCompactionCompletedAsync(CompactionCompleted msg, CancellationToken cancellationToken) {
        if (msg.Error is OperationCanceledException) return;
        if (msg.Error is not null) throw msg.Error;
        await msg.Job.CompleteAsync(cancellationToken);
        TryFulfillPendingPlanRequest();
    }
}
