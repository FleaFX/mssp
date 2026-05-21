namespace MSSP.Raft;

public sealed partial class RaftNode {
    /// <summary>
    /// The leader role: drives log replication to all peers, advances the commit index once
    /// a quorum acknowledges an entry, and accepts client proposals via <see cref="ProposeAsync"/>.
    /// </summary>
    /// <remarks>
    /// The leader appends a no-op entry immediately on election (Raft Figure 8) and only begins
    /// accepting client commands once that entry is committed, indicated by
    /// <see cref="NoOpCommitted"/> becoming <see langword="true"/>.
    /// </remarks>
    sealed class LeaderRole : RaftRole {
        readonly Dictionary<string, ulong> _nextIndex = new();
        readonly Dictionary<string, ulong> _matchIndex = new();
        readonly Dictionary<ulong, TaskCompletionSource<RaftApplyResult>> _pending = new();

        /// <summary>
        /// Gets or sets whether the initial no-op entry has been committed by a quorum,
        /// indicating the leader is ready to accept client proposals.
        /// </summary>
        public bool NoOpCommitted { get; set; }

        PeriodicTimer? _heartbeatTimer;
        Task? _heartbeatTask;

        /// <summary>
        /// Initialises per-peer replication state, setting <c>nextIndex</c> to one past the
        /// current log end and <c>matchIndex</c> to zero for every peer.
        /// </summary>
        public LeaderRole(RaftNode node) : base(node) {
            var nextIdx = node._log.LastIndex + 1;
            foreach (var peerId in node._config.PeerIds) {
                _nextIndex[peerId] = nextIdx;
                _matchIndex[peerId] = 0;
            }
        }

        /// <summary>
        /// Appends the command to the log and triggers replication. Rejects with
        /// <see cref="NotLeaderException"/> if the initial no-op has not yet been committed.
        /// </summary>
        public override async Task ProposeAsync(ReadOnlyMemory<byte> command, TaskCompletionSource<RaftApplyResult> tcs) {
            if (!NoOpCommitted) {
                tcs.TrySetException(new NotLeaderException(Node._leaderId));
                return;
            }
            var entry = new RaftLogEntry(Node._currentTerm, Node._log.LastIndex + 1, RaftLogEntryType.Command, command);
            await Node._log.AppendAsync([entry]);
            _pending[entry.Index] = tcs;
            await ReplicateToAllPeersAsync();
            await TryAdvanceCommitIndexAsync();
        }

        /// <inheritdoc/>
        internal override void OnEntryApplied(ulong index, RaftLogEntry entry, bool success) {
            if (_pending.TryGetValue(index, out var tcs)) {
                _pending.Remove(index);
                if (entry.Type == RaftLogEntryType.NoOp) {
                    NoOpCommitted = true;
                    tcs.TrySetResult(new RaftApplyResult(false));
                } else {
                    tcs.TrySetResult(new RaftApplyResult(!success));
                }
            } else if (entry.Type == RaftLogEntryType.NoOp) {
                NoOpCommitted = true;
            }
        }

        /// <summary>
        /// Starts the periodic heartbeat that triggers <see cref="ReplicateToAllPeersAsync"/>
        /// at each tick to maintain leadership and keep followers up to date.
        /// </summary>
        public async Task StartHeartbeatAsync() {
            _heartbeatTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(Node._config.HeartbeatIntervalMs));
            _heartbeatTask = Task.Run(async () => {
                while (await _heartbeatTimer.WaitForNextTickAsync()) {
                    Node.Post(async () => {
                        if (Node._role is LeaderRole)
                            await ReplicateToAllPeersAsync();
                    });
                }
            });
        }

        /// <summary>
        /// Fires off a concurrent replication attempt to every peer.
        /// </summary>
        public async Task ReplicateToAllPeersAsync() {
            var nodeToken = Node._cts?.Token ?? CancellationToken.None;
            foreach (var peerId in Node._config.PeerIds) {
                var pid = peerId;
                _ = Task.Run(() => ReplicateToPeerAsync(pid, nodeToken));
            }
            await Task.CompletedTask;
        }

        async Task ReplicateToPeerAsync(string peerId, CancellationToken cancellationToken = default) {
            if (Node._role is not LeaderRole) return;
            if (cancellationToken.IsCancellationRequested) return;

            ulong nextIdx;
            lock (_nextIndex) nextIdx = _nextIndex.GetValueOrDefault(peerId, Node._log.LastIndex + 1);

            var prevLogIndex = nextIdx - 1;
            var prevLogTerm = prevLogIndex == 0 ? 0 : await Node._log.GetTermAtAsync(prevLogIndex);

            var entries = new List<RaftLogEntry>();
            await foreach (var entry in Node._log.GetEntriesFromAsync(nextIdx))
                entries.Add(entry);

            var request = new AppendEntriesRequest(
                Node._currentTerm, Node._config.NodeId,
                prevLogIndex, prevLogTerm,
                entries, Node._commitIndex);

            try {
                var response = await Node._transport.AppendEntriesAsync(peerId, request, cancellationToken);
                Node.Post(async () => {
                    if (Node._role is not LeaderRole leader) return;
                    if (response.Term > Node._currentTerm) { await Node.TransitionToFollowerAsync(response.Term); return; }
                    if (response.Success) {
                        if (entries.Count > 0) {
                            leader._matchIndex[peerId] = entries[^1].Index;
                            leader._nextIndex[peerId] = entries[^1].Index + 1;
                        }
                        await leader.TryAdvanceCommitIndexAsync();
                    } else {
                        if (response.ConflictTerm > 0) {
                            var newNext = response.ConflictIndex;
                            for (var i = Node._log.LastIndex; i >= 1; i--) {
                                if (await Node._log.GetTermAtAsync(i) == response.ConflictTerm) {
                                    newNext = i + 1;
                                    break;
                                }
                            }
                            leader._nextIndex[peerId] = newNext;
                        } else {
                            leader._nextIndex[peerId] = Math.Max(1, response.ConflictIndex);
                        }
                        _ = Task.Run(() => leader.ReplicateToPeerAsync(peerId, Node._cts?.Token ?? CancellationToken.None));
                    }
                });
            } catch { /* peer unavailable, will retry on next heartbeat */ }
        }

        /// <summary>
        /// Scans the log backwards from <c>LastIndex</c> and advances <c>commitIndex</c>
        /// to the highest index replicated to a quorum in the current term.
        /// </summary>
        public async Task TryAdvanceCommitIndexAsync() {
            if (Node._role is not LeaderRole) return;

            var quorum = (Node._config.PeerIds.Length + 1) / 2 + 1;
            for (var n = Node._log.LastIndex; n > Node._commitIndex; n--) {
                var termAtN = await Node._log.GetTermAtAsync(n);
                if (termAtN != Node._currentTerm) break;

                var matchCount = 1;
                foreach (var peerId in Node._config.PeerIds)
                    if (_matchIndex.GetValueOrDefault(peerId) >= n)
                        matchCount++;

                if (matchCount >= quorum) {
                    Node._commitIndex = n;
                    await Node.ApplyCommittedEntriesAsync();
                    break;
                }
            }
        }

        void FailAll() {
            foreach (var tcs in _pending.Values)
                tcs.TrySetException(new NotLeaderException(Node._leaderId));
            _pending.Clear();
        }

        /// <summary>
        /// Disposes the heartbeat timer, awaits the heartbeat loop task, and fails all
        /// pending proposals. Used when stepping down gracefully via <see cref="RaftNode.StopAsync"/>.
        /// </summary>
        public async Task StopAsync() {
            _heartbeatTimer?.Dispose();
            _heartbeatTimer = null;
            if (_heartbeatTask is not null) {
                await _heartbeatTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                _heartbeatTask = null;
            }
            FailAll();
        }

        /// <inheritdoc/>
        public override void Dispose() {
            _heartbeatTimer?.Dispose();
            _heartbeatTask = null;
            FailAll();
        }
    }
}
