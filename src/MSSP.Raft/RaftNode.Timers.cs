using System.Security.Cryptography;

namespace MSSP.Raft;

public sealed partial class RaftNode {

    /// <summary>
    /// Arms (or re-arms) the election timer with a fresh randomised delay and increments the
    /// generation counter so any previously scheduled <see cref="ElectionTimerFired"/> message
    /// is recognised as stale and discarded.
    /// </summary>
    void RestartElectionTimer() {
        var gen     = ++_electionTimerGeneration;
        var delayMs = RandomNumberGenerator.GetInt32(_config.ElectionTimeoutMinMs, _config.ElectionTimeoutMaxMs + 1);
        PostTimerAsync(delayMs, new ElectionTimerFired(gen));
    }

    /// <summary>
    /// Invalidates any in-flight <see cref="ElectionTimerFired"/> message by incrementing the
    /// generation counter. No timer object is cancelled; the next fired message simply carries
    /// a stale generation and is discarded.
    /// </summary>
    void StopElectionTimer() => _electionTimerGeneration++;

    /// <summary>
    /// Arms (or re-arms) the heartbeat timer with the configured fixed interval and increments
    /// the generation counter so stale <see cref="HeartbeatTimerFired"/> messages are discarded.
    /// </summary>
    void RestartHeartbeatTimer() {
        var gen = ++_heartbeatTimerGeneration;
        PostTimerAsync(_config.HeartbeatIntervalMs, new HeartbeatTimerFired(gen));
    }

    /// <summary>
    /// Invalidates any in-flight <see cref="HeartbeatTimerFired"/> message by incrementing
    /// the generation counter.
    /// </summary>
    void StopHeartbeatTimer() => _heartbeatTimerGeneration++;

    /// <summary>
    /// Schedules <paramref name="message"/> to be posted to the actor channel after
    /// <paramref name="delayMs"/> milliseconds. The delay task is cancelled automatically when
    /// the node stops (via <see cref="_cts"/>), so no timer resource is leaked on shutdown.
    /// </summary>
    void PostTimerAsync(int delayMs, RaftMessage message) {
        var ct = _cts?.Token ?? CancellationToken.None;
        _ = Task.Run(async () => {
            try {
                await Task.Delay(delayMs, ct);
                _channel.Writer.TryWrite(message);
            } catch (OperationCanceledException) { }
        }, ct);
    }
}
