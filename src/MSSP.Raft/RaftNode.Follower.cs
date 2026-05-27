namespace MSSP.Raft;

public sealed partial class RaftNode {

    /// <summary>
    /// Transitions this node to the follower role for <paramref name="newTerm"/>.
    /// </summary>
    /// <remarks>
    /// If <paramref name="newTerm"/> exceeds the current term, the persistent state is updated
    /// and <see cref="_votedFor"/> is cleared. Any pending leader proposals are failed immediately
    /// because this node can no longer guarantee they will be committed.
    /// </remarks>
    /// <param name="newTerm">The term to adopt, or the current term when stepping down within the same term.</param>
    async Task BecomeFollowerAsync(ulong newTerm) {
        // Raft §5.1: if we see a higher term, update and clear the vote.
        if (newTerm > _currentTerm) {
            _currentTerm = newTerm;
            _votedFor    = null;
            await PersistStateAsync();

            // A new leader may start an entirely different snapshot transfer; discard any
            // partially-received data so stale chunks from the old leader are not mixed in.
            // (Same-term step-downs preserve the buffer: the transfer may still be in progress.)
            _snapshotBuffer?.Dispose();
            _snapshotBuffer       = null;
            _pendingSnapshotIndex = null;
        }

        // Fail any proposals queued while we were leader — we can no longer commit them.
        FailPendingProposals();

        // Clear leader state.
        _nextIndex        = null;
        _matchIndex       = null;
        _pendingProposals = null;
        _noOpCommitted    = false;

        // Clear candidate state.
        _votesGranted = 0;

        StopHeartbeatTimer();
        RestartElectionTimer();

        _role = NodeRole.Follower;
    }

    /// <summary>
    /// Handles an <see cref="ElectionTimerFired"/> message. Starts a new election if the
    /// generation matches and the node is not already the leader.
    /// </summary>
    /// <param name="generation">The timer generation at which this message was scheduled.</param>
    async Task OnElectionTimerFiredAsync(ulong generation) {
        // Stale message from a superseded timer.
        if (generation != _electionTimerGeneration) return;

        // Leaders do not start elections (Raft §5.2).
        if (_role == NodeRole.Leader) return;

        await BecomeCandidateAsync();
    }

    /// <summary>
    /// Handles an inbound <see cref="VoteRequest"/>. Implements Raft §5.2 (vote granting) and
    /// §5.4.1 (election restriction).
    /// </summary>
    async Task OnVoteRequestReceivedAsync(VoteRequest request, TaskCompletionSource<VoteResponse> reply) {
        // Step down if we see a higher term.
        if (request.Term > _currentTerm)
            await BecomeFollowerAsync(request.Term);

        // Reject requests from stale terms.
        if (request.Term < _currentTerm) {
            reply.TrySetResult(new VoteResponse(_currentTerm, VoteGranted: false));
            return;
        }

        // Reject if already voted for a different candidate in this term.
        if (_votedFor is not null && _votedFor != request.CandidateId) {
            reply.TrySetResult(new VoteResponse(_currentTerm, VoteGranted: false));
            return;
        }

        // Raft §5.4.1: candidate's log must be at least as up-to-date as ours.
        var logOk = request.LastLogTerm > _log.LastTerm
                 || (request.LastLogTerm == _log.LastTerm && request.LastLogIndex >= _log.LastIndex);
        if (!logOk) {
            reply.TrySetResult(new VoteResponse(_currentTerm, VoteGranted: false));
            return;
        }

        // Grant the vote.
        _votedFor = request.CandidateId;
        await PersistStateAsync();

        // Raft §5.2: reset election timer when granting a vote.
        RestartElectionTimer();
        reply.TrySetResult(new VoteResponse(_currentTerm, VoteGranted: true));
    }

    /// <summary>
    /// Handles an inbound <see cref="AppendEntriesRequest"/>. Implements Raft §5.3 (log
    /// replication) and §5.2 (step-down on higher term).
    /// </summary>
    async Task OnAppendEntriesReceivedAsync(AppendEntriesRequest request, TaskCompletionSource<AppendEntriesResponse> reply) {
        if (request.Term > _currentTerm)
            await BecomeFollowerAsync(request.Term);

        if (request.Term < _currentTerm) {
            reply.TrySetResult(new AppendEntriesResponse(_currentTerm, Success: false, 0, 0));
            return;
        }

        // Valid AppendEntries from the current leader.
        if (_role == NodeRole.Candidate)
            await BecomeFollowerAsync(_currentTerm);  // a new leader was elected

        _leaderHint = request.LeaderId;
        RestartElectionTimer();

        // §5.3 consistency check.
        if (request.PrevLogIndex > 0) {
            // Reject if prevLogIndex falls inside our snapshot; the leader must send a snapshot.
            if (request.PrevLogIndex < _log.LastIncludedIndex) {
                reply.TrySetResult(new AppendEntriesResponse(_currentTerm, false, _log.LastIncludedIndex + 1, 0));
                return;
            }

            if (request.PrevLogIndex == _log.LastIncludedIndex) {
                // Snapshot boundary: the term is guaranteed by the snapshot meta-data; calling
                // GetTermAtAsync here is fragile because compacted logs may not retain that entry.
                if (request.PrevLogTerm != _log.LastIncludedTerm) {
                    reply.TrySetResult(new AppendEntriesResponse(_currentTerm, false, _log.LastIncludedIndex + 1, 0));
                    return;
                }
                // Terms match at snapshot boundary — proceed to entry append below.
            } else {
                if (request.PrevLogIndex > _log.LastIndex) {
                    reply.TrySetResult(new AppendEntriesResponse(_currentTerm, false, _log.LastIndex + 1, 0));
                    return;
                }

                var termAtPrev = await _log.GetTermAtAsync(request.PrevLogIndex);
                if (termAtPrev != request.PrevLogTerm) {
                    // Return the conflict term and the first index of that term so the leader can
                    // skip the entire term in one round-trip (optimised fast back-step).
                    var conflictTerm = termAtPrev;
                    var conflictIndex = request.PrevLogIndex;
                    while (conflictIndex > 1 && await _log.GetTermAtAsync(conflictIndex - 1) == conflictTerm)
                        conflictIndex--;
                    reply.TrySetResult(new AppendEntriesResponse(_currentTerm, false, conflictIndex, conflictTerm));
                    return;
                }
            }
        }

        // Append new entries, resolving any conflicts with our existing log.
        foreach (var entry in request.Entries) {
            if (entry.Index <= _log.LastIncludedIndex) continue;  // already part of a snapshot
            if (entry.Index <= _log.LastIndex) {
                var existing = await _log.GetEntryAsync(entry.Index);
                if (existing.Term != entry.Term)
                    await _log.TruncateFromAsync(entry.Index);
            }
            if (entry.Index > _log.LastIndex)
                await _log.AppendAsync([entry]);
        }

        // Advance commit index.
        if (request.LeaderCommit > _commitIndex) {
            _commitIndex = Math.Min(request.LeaderCommit, _log.LastIndex);
            await ApplyCommittedEntriesAsync();
        }

        reply.TrySetResult(new AppendEntriesResponse(_currentTerm, Success: true, 0, 0));
    }

    /// <summary>
    /// Handles an inbound <see cref="InstallSnapshotRequest"/> chunk from the leader. Implements
    /// Raft §7 (log compaction / snapshot installation). Chunks are buffered until
    /// <see cref="InstallSnapshotRequest.Done"/> is <see langword="true"/>, then installed atomically.
    /// </summary>
    /// <remarks>
    /// The buffer fields (<see cref="_snapshotBuffer"/>, <see cref="_pendingSnapshotIndex"/>) live
    /// on the node rather than on a role object. They survive same-term role transitions (e.g. a
    /// candidate stepping down to follower within the same term) so that a multi-chunk transfer
    /// in progress is not interrupted. On a term increase they are cleared in
    /// <see cref="BecomeFollowerAsync"/> because the new leader may start a different transfer.
    /// </remarks>
    async Task OnInstallSnapshotReceivedAsync(InstallSnapshotRequest request, TaskCompletionSource<InstallSnapshotResponse> reply) {
        if (request.Term > _currentTerm)
            await BecomeFollowerAsync(request.Term);

        if (request.Term < _currentTerm) {
            reply.TrySetResult(new InstallSnapshotResponse(_currentTerm));
            return;
        }

        if (_role == NodeRole.Candidate)
            await BecomeFollowerAsync(_currentTerm);

        _leaderHint = request.LeaderId;
        RestartElectionTimer();

        // Stale snapshot: we already have a more recent compaction boundary.
        if (request.LastIncludedIndex <= _log.LastIncludedIndex) {
            _snapshotBuffer?.Dispose();
            _snapshotBuffer = null;
            _pendingSnapshotIndex = null;
            reply.TrySetResult(new InstallSnapshotResponse(_currentTerm));
            return;
        }

        // New snapshot or different boundary: start fresh.
        if (_pendingSnapshotIndex != request.LastIncludedIndex) {
            _snapshotBuffer?.Dispose();
            _snapshotBuffer = new MemoryStream();
            _pendingSnapshotIndex = request.LastIncludedIndex;
        }

        // Write this chunk at its byte offset within the reassembly buffer.
        _snapshotBuffer!.Seek((long)request.Offset, SeekOrigin.Begin);
        _snapshotBuffer.Write(request.Data.Span);

        if (!request.Done) {
            reply.TrySetResult(new InstallSnapshotResponse(_currentTerm));
            return;
        }

        // All chunks received — install the snapshot atomically.
        var data = new ReadOnlyMemory<byte>(_snapshotBuffer.ToArray());
        _snapshotBuffer.Dispose();
        _snapshotBuffer = null;
        _pendingSnapshotIndex = null;

        var ct = _cts?.Token ?? CancellationToken.None;
        await _log.CompactToAsync(request.LastIncludedIndex, request.LastIncludedTerm, ct);
        await _stateMachine.InstallSnapshotAsync(request.LastIncludedIndex, request.LastIncludedTerm, data, ct);

        if (_commitIndex < request.LastIncludedIndex)
            _commitIndex = request.LastIncludedIndex;

        await ApplyCommittedEntriesAsync();
        reply.TrySetResult(new InstallSnapshotResponse(_currentTerm));
    }
}
