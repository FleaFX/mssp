namespace MSSP.Raft;

public sealed partial class RaftNode {

    /// <summary>
    /// Transitions this node to the leader role.
    /// </summary>
    /// <remarks>
    /// Initialises per-peer replication state, appends the mandatory no-op entry (Raft Figure 8),
    /// starts the heartbeat timer, and triggers the first replication round.
    /// </remarks>
    async Task BecomeLeaderAsync() {
        _role = NodeRole.Leader;
        _leaderHint = _config.NodeId;

        _nextIndex = new Dictionary<string, ulong>();
        _matchIndex = new Dictionary<string, ulong>();
        var nextIdx = _log.LastIndex + 1;
        foreach (var peerId in _config.PeerIds) {
            _nextIndex[peerId] = nextIdx;
            _matchIndex[peerId] = 0;
        }

        _pendingProposals = new Dictionary<ulong, TaskCompletionSource<RaftApplyResult>>();
        _noOpCommitted = false;

        StopElectionTimer();

        // Raft Figure 8: append a no-op in the new leader's term so entries from previous
        // terms are committed indirectly rather than by counting replicas for old terms.
        var noOp = new RaftLogEntry(_currentTerm, _log.LastIndex + 1, RaftLogEntryType.NoOp, ReadOnlyMemory<byte>.Empty);
        await _log.AppendAsync([noOp]);

        RestartHeartbeatTimer();
        ReplicateToAllPeers();

        // In a single-node cluster the no-op is immediately at quorum; try to commit it now so
        // IsLeader becomes true without waiting for the first heartbeat.
        await TryAdvanceCommitIndexAsync();
    }

    /// <summary>
    /// Handles a <see cref="HeartbeatTimerFired"/> message. Drives a replication round to all
    /// peers and re-arms the timer for the next interval.
    /// </summary>
    Task OnHeartbeatTimerFiredAsync(ulong generation) {
        if (generation != _heartbeatTimerGeneration || _role != NodeRole.Leader)
            return Task.CompletedTask;

        ReplicateToAllPeers();
        RestartHeartbeatTimer();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Handles a <see cref="ProposeReceived"/> message. Appends the command to the log and
    /// triggers immediate replication.
    /// </summary>
    /// <remarks>
    /// Rejected immediately if the no-op has not yet been committed: a newly elected leader
    /// must not accept client writes before establishing which entries from prior terms are safe
    /// to commit (Raft Figure 8).
    /// </remarks>
    async Task OnProposeReceivedAsync(ReadOnlyMemory<byte> payload, TaskCompletionSource<RaftApplyResult> reply) {
        if (_role != NodeRole.Leader || !_noOpCommitted) {
            reply.TrySetException(new NotLeaderException(_leaderHint));
            return;
        }

        var index = _log.LastIndex + 1;
        var entry = new RaftLogEntry(_currentTerm, index, RaftLogEntryType.Command, payload);
        await _log.AppendAsync([entry]);
        _pendingProposals![index] = reply;

        // Trigger replication immediately; peers may commit before the next heartbeat.
        // Also try to advance the commit index now: in a single-node cluster there are no peers
        // to replicate to, so TryAdvanceCommitIndexAsync is the only opportunity to commit this
        // entry (quorum = 1, replicaCount = 1 ≥ quorum).  For multi-node clusters it is a
        // no-op here because matchIndex for peers has not been updated yet.
        ReplicateToAllPeers();
        await TryAdvanceCommitIndexAsync();
    }

    /// <summary>
    /// Fires a background replication task to every peer.
    /// </summary>
    void ReplicateToAllPeers() {
        foreach (var peerId in _config.PeerIds)
            ReplicateToPeer(peerId);
    }

    /// <summary>
    /// Determines whether to send a snapshot or log entries to <paramref name="peerId"/> and
    /// fires the appropriate background task.
    /// </summary>
    void ReplicateToPeer(string peerId) {
        var nextIdx = _nextIndex![peerId];

        // Peer is behind our snapshot boundary; send the snapshot first.
        _ = nextIdx <= _log.LastIncludedIndex
            ? SendSnapshotToPeerAsync(peerId)
            : SendAppendEntriesToPeerAsync(peerId, nextIdx);
    }

    /// <summary>
    /// Reads log entries from <paramref name="fromIndex"/> and sends them to <paramref name="peerId"/>
    /// as an <see cref="AppendEntriesRequest"/>. Posts the result back to the actor channel.
    /// </summary>
    /// <remarks>
    /// All mutable node state is captured into local variables before the first <c>await</c> to
    /// avoid TOCTOU issues while the actor continues processing other messages.
    /// </remarks>
    async Task SendAppendEntriesToPeerAsync(string peerId, ulong fromIndex) {
        var ct = _cts?.Token ?? CancellationToken.None;
        var capturedTerm = _currentTerm;              // captured before first await
        var leaderCommit = _commitIndex;              // captured before first await
        var prevLogIndex = fromIndex - 1;
        var prevLogTerm = prevLogIndex == 0
            ? 0UL
            : await _log.GetTermAtAsync(prevLogIndex, ct);

        var entries = new List<RaftLogEntry>();
        await foreach (var entry in _log.GetEntriesFromAsync(fromIndex, ct))
            entries.Add(entry);

        var sentUpToIdx = entries.Count > 0 ? entries[^1].Index : prevLogIndex;

        try {
            var response = await _transport.AppendEntriesAsync(peerId, new AppendEntriesRequest(capturedTerm, _config.NodeId, prevLogIndex, prevLogTerm, entries, leaderCommit), ct);
            _channel.Writer.TryWrite(new AppendEntriesResponseReceived(peerId, response, capturedTerm, sentUpToIdx));
        } catch {
            // Peer unavailable; the next heartbeat will retry automatically.
        }
    }

    /// <summary>
    /// Sends the current snapshot to <paramref name="peerId"/> in fixed-size chunks. Posts each
    /// chunk response back to the actor channel.
    /// </summary>
    async Task SendSnapshotToPeerAsync(string peerId) {
        var ct = _cts?.Token ?? CancellationToken.None;
        var capturedTerm = _currentTerm;               // captured before first await
        var sentMatchIndex = _log.LastIncludedIndex;     // captured before first await

        var snapshotData = await _stateMachine.CreateSnapshotAsync(ct);
        var chunkSize = _config.SnapshotChunkSizeBytes;
        var totalBytes = (ulong)snapshotData.Length;
        var offset = 0UL;

        while (true) {
            var remaining = totalBytes - offset;
            var size = (int)Math.Min((ulong)chunkSize, remaining);
            var done = offset + (ulong)size >= totalBytes;

            var request = new InstallSnapshotRequest(
                capturedTerm, _config.NodeId,
                sentMatchIndex, _log.LastIncludedTerm,
                offset, snapshotData.Slice((int)offset, size), done);

            InstallSnapshotResponse response;
            try {
                response = await _transport.InstallSnapshotAsync(peerId, request, ct);
            } catch {
                return;  // peer unavailable; retry on next heartbeat
            }

            // Always post the response so the actor can handle term-bump step-down or advance
            // matchIndex/nextIndex on success. Use sentMatchIndex = 0 for non-final chunks so
            // the actor only updates replication state when the full snapshot has been delivered.
            _channel.Writer.TryWrite(new InstallSnapshotResponseReceived(peerId, response, capturedTerm,done ? sentMatchIndex : 0));

            if (done || response.Term > capturedTerm)
                return;

            offset += (ulong)size;
        }
    }

    /// <summary>
    /// Scans the log backwards and advances <see cref="_commitIndex"/> to the highest entry
    /// replicated to a quorum in the current term (Raft §5.3, §5.4.2).
    /// </summary>
    async Task TryAdvanceCommitIndexAsync() {
        var quorum = (_config.PeerIds.Length + 1) / 2 + 1;

        for (var n = _log.LastIndex; n > _commitIndex; n--) {
            // Raft §5.4.2: only commit entries from the current term by counting replicas.
            var termAtN = await _log.GetTermAtAsync(n);
            if (termAtN != _currentTerm)
                break;

            var replicaCount = _config.PeerIds.Count(peerId => _matchIndex![peerId] >= n) + 1; // self

            if (replicaCount < quorum)
                continue;

            _commitIndex = n;
            await ApplyCommittedEntriesAsync();
            break;  // commit index is monotonically increasing; stop after the highest
        }
    }

    /// <summary>
    /// Handles an <see cref="AppendEntriesResponseReceived"/> message. Advances per-peer
    /// replication state on success, or fast-backtracks <c>nextIndex</c> on failure and retries.
    /// </summary>
    async Task OnAppendEntriesResponseReceivedAsync(
        string peerId, AppendEntriesResponse response, ulong sentTerm, ulong sentUpToIndex) {
        if (sentTerm != _currentTerm || _role != NodeRole.Leader) return;

        if (response.Term > _currentTerm) {
            await BecomeFollowerAsync(response.Term);
            return;
        }

        if (response.Success) {
            _matchIndex![peerId] = Math.Max(_matchIndex[peerId], sentUpToIndex);
            _nextIndex![peerId] = _matchIndex[peerId] + 1;
            await TryAdvanceCommitIndexAsync();
        } else {
            // Log inconsistency: use the conflict hint to skip back efficiently.
            if (response.ConflictTerm > 0) {
                // Find the last entry in our log with the conflicting term; if none, fall back
                // to the conflict index returned by the follower.
                var newNext = response.ConflictIndex;
                for (var i = _log.LastIndex; i >= 1; i--) {
                    if (await _log.GetTermAtAsync(i) != response.ConflictTerm)
                        continue;

                    newNext = i + 1;
                    break;
                }
                _nextIndex![peerId] = newNext;
            } else {
                _nextIndex![peerId] = Math.Max(1, response.ConflictIndex);
            }

            // Retry immediately — do not wait for the next heartbeat.
            ReplicateToPeer(peerId);
        }
    }

    /// <summary>
    /// Handles an <see cref="InstallSnapshotResponseReceived"/> message. Advances replication
    /// state for the peer after the final snapshot chunk has been delivered.
    /// </summary>
    async Task OnInstallSnapshotResponseReceivedAsync(
        string peerId, InstallSnapshotResponse response, ulong sentTerm, ulong sentMatchIndex) {
        if (sentTerm != _currentTerm || _role != NodeRole.Leader) return;

        if (response.Term > _currentTerm) {
            await BecomeFollowerAsync(response.Term);
            return;
        }

        // Only advance replication state when sentMatchIndex > 0 (i.e. this was the final chunk).
        if (sentMatchIndex == 0) return;

        _matchIndex![peerId] = Math.Max(_matchIndex[peerId], sentMatchIndex);
        _nextIndex![peerId] = _matchIndex[peerId] + 1;
        await TryAdvanceCommitIndexAsync();
    }
}
