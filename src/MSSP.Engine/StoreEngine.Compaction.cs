namespace MSSP.Engine;

sealed partial class StoreEngine {
    void MaybeStartCompaction() {
        if (store.PlanCompaction() is not { } job) return;
        Interlocked.Increment(ref _maintenanceInFlight);
        _compaction!.Enqueue(job);
    }

    async ValueTask HandleCompactionCompletedAsync(CompactionCompleted msg, CancellationToken cancellationToken) {
        if (msg.Error is OperationCanceledException) { Interlocked.Decrement(ref _maintenanceInFlight); return; }
        if (msg.Error is not null) { Interlocked.Decrement(ref _maintenanceInFlight); throw msg.Error; }
        await msg.Job.CompleteAsync(cancellationToken);
        MaybeStartCompaction();
        Interlocked.Decrement(ref _maintenanceInFlight);
    }
}
