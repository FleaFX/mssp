namespace MSSP.Raft;

public sealed partial class RaftNode {
    readonly Dictionary<string, ulong> _nextIndex = new();
    readonly Dictionary<string, ulong> _matchIndex = new();
    readonly Dictionary<ulong, TaskCompletionSource<RaftApplyResult>> _pending = new();

    async Task ReplicateToAllPeersAsync() {
        var nodeToken = _cts?.Token ?? CancellationToken.None;
        foreach (var peerId in config.PeerIds) {
            var pid = peerId;
            _ = Task.Run(() => ReplicateToPeerAsync(pid, nodeToken));
        }
        await Task.CompletedTask;
    }

    async Task ReplicateToPeerAsync(string peerId, CancellationToken ct = default) {
        if (_role != RaftRole.Leader) return;
        if (ct.IsCancellationRequested) return;

        ulong nextIdx;
        lock (_nextIndex) nextIdx = _nextIndex.GetValueOrDefault(peerId, log.LastIndex + 1);

        ulong prevLogIndex = nextIdx - 1;
        ulong prevLogTerm = prevLogIndex == 0 ? 0 : await log.GetTermAtAsync(prevLogIndex);

        var entries = new List<RaftLogEntry>();
        await foreach (var entry in log.GetEntriesFromAsync(nextIdx))
            entries.Add(entry);

        var request = new AppendEntriesRequest(
            _currentTerm, config.NodeId,
            prevLogIndex, prevLogTerm,
            entries, _commitIndex);

        try {
            var response = await transport.AppendEntriesAsync(peerId, request, ct);
            Post(async () => {
                if (_role != RaftRole.Leader) return;
                if (response.Term > _currentTerm) { await BecomeFollowerAsync(response.Term); return; }
                if (response.Success) {
                    if (entries.Count > 0) {
                        _matchIndex[peerId] = entries[^1].Index;
                        _nextIndex[peerId] = entries[^1].Index + 1;
                    }
                    await TryAdvanceCommitIndexAsync();
                } else {
                    if (response.ConflictTerm > 0) {
                        ulong newNext = response.ConflictIndex;
                        for (var i = log.LastIndex; i >= 1; i--) {
                            if (await log.GetTermAtAsync(i) == response.ConflictTerm) {
                                newNext = i + 1;
                                break;
                            }
                        }
                        _nextIndex[peerId] = newNext;
                    } else {
                        _nextIndex[peerId] = Math.Max(1, response.ConflictIndex);
                    }
                    _ = Task.Run(() => ReplicateToPeerAsync(peerId, _cts?.Token ?? CancellationToken.None));
                }
            });
        } catch { /* peer unavailable, will retry on next heartbeat */ }
    }

    async Task TryAdvanceCommitIndexAsync() {
        if (_role != RaftRole.Leader) return;

        var quorum = (config.PeerIds.Length + 1) / 2 + 1;
        for (var n = log.LastIndex; n > _commitIndex; n--) {
            var termAtN = await log.GetTermAtAsync(n);
            if (termAtN != _currentTerm) break;

            var matchCount = 1;
            foreach (var peerId in config.PeerIds)
                if (_matchIndex.GetValueOrDefault(peerId) >= n)
                    matchCount++;

            if (matchCount >= quorum) {
                _commitIndex = n;
                await ApplyCommittedEntriesAsync();
                break;
            }
        }
    }

    async Task ApplyCommittedEntriesAsync() {
        while (stateMachine.LastAppliedIndex < _commitIndex) {
            var idx = stateMachine.LastAppliedIndex + 1;
            var entry = await log.GetEntryAsync(idx);
            var success = await stateMachine.ApplyAsync(entry);

            if (_pending.TryGetValue(idx, out var tcs)) {
                _pending.Remove(idx);
                if (entry.Type == RaftLogEntryType.NoOp) {
                    _noOpCommitted = true;
                    tcs.TrySetResult(new RaftApplyResult(false));
                } else {
                    tcs.TrySetResult(new RaftApplyResult(!success));
                }
            } else if (entry.Type == RaftLogEntryType.NoOp && _role == RaftRole.Leader) {
                _noOpCommitted = true;
            }
        }
    }

    void FailAllPendingProposals() {
        foreach (var tcs in _pending.Values)
            tcs.TrySetException(new NotLeaderException(_leaderId));
        _pending.Clear();
    }
}
