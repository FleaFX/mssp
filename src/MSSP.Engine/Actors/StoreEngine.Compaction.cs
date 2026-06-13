using System.Runtime.ExceptionServices;

namespace MSSP.Engine;

sealed partial class StoreEngine {
    bool _compactionPlanPending;

    ValueTask HandleCompactionPlanRequest() {
        if (store.PlanCompaction() is { } job)
            _compaction!.Respond(job, _epoch);
        else
            _compactionPlanPending = true;
        return ValueTask.CompletedTask;
    }

    void TryFulfillPendingPlanRequest() {
        if (!_compactionPlanPending) return;
        if (store.PlanCompaction() is not { } job) return;
        _compactionPlanPending = false;
        _compaction!.Respond(job, _epoch);
    }

    async ValueTask HandleCompactionCompletedAsync(CompactionCompleted msg, CancellationToken cancellationToken) {
        if (msg.Error is OperationCanceledException) return;
        if (msg.Error is not null) ExceptionDispatchInfo.Capture(msg.Error).Throw();
        if (msg.Epoch != _epoch) { msg.Job.Abandon(); return; }
        await msg.Job.CompleteAsync(cancellationToken);
        TryFulfillPendingPlanRequest();
    }
}
