namespace MSSP.Raft;

public sealed partial class RaftNode {
    PeriodicTimer? _heartbeatTimer;
    Task? _heartbeatTask;

    async Task StartHeartbeatAsync() {
        StopElectionTimer();
        _heartbeatTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(config.HeartbeatIntervalMs));
        _heartbeatTask = Task.Run(async () => {
            while (await _heartbeatTimer.WaitForNextTickAsync()) {
                Post(async () => {
                    if (_role == RaftRole.Leader)
                        await ReplicateToAllPeersAsync();
                });
            }
        });
        await Task.CompletedTask;
    }

    async Task StopHeartbeatAsync() {
        _heartbeatTimer?.Dispose();
        _heartbeatTimer = null;
        if (_heartbeatTask is not null) {
            await _heartbeatTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            _heartbeatTask = null;
        }
    }
}
