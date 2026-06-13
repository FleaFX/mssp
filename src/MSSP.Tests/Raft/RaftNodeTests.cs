using FluentAssertions;

namespace MSSP.Raft;

public class RaftNodeTests {
    
    static RaftNode CreateNode(
        string nodeId,
        string[] peers,
        IRaftTransport transport,
        IRaftLog? log = null,
        int electionTimeoutMinMs = 10_000,
        int electionTimeoutMaxMs = 20_000,
        int heartbeatMs = 5_000) {
        var config = new RaftNodeConfig(nodeId, peers, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatMs);
        return new RaftNode(config, log ?? new InMemoryRaftLog(), transport, new NullStateMachine(), new InMemoryRaftStateStorage());
    }

    static RaftNode CreateNodeWithTransport(
        string nodeId,
        string[] peers,
        InMemoryRaftTransport transport,
        IRaftLog? log = null,
        int electionTimeoutMinMs = 10_000,
        int electionTimeoutMaxMs = 20_000,
        int heartbeatMs = 5_000) {
        var node = CreateNode(nodeId, peers, transport, log, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatMs);
        transport.Register(node);
        return node;
    }

    public class Election {
        [Fact]
        public async Task SingleNode_becomesLeader_after_electionTimer() {
            var transport = new RecordingRaftTransport();
            var node = CreateNode("n1", [], transport);

            await node.StartAsync();
            try {
                await node.TriggerElectionTimerAsync();

                node.IsLeader.Should().BeTrue("single-node cluster wins election immediately");
                node.LeaderHint.Should().Be("n1");
            } finally {
                await node.DisposeAsync();
            }
        }

        [Fact]
        public async Task SingleNode_doesNotStartElection_on_staleTimerGeneration() {
            var transport = new RecordingRaftTransport();
            var node = CreateNode("n1", [], transport);

            await node.StartAsync();
            try {
                // Inject a timer message with generation 0 (stale — StartAsync already incremented to 1).
                node.Inject(new ElectionTimerFired(0));
                await node.WhenIdleAsync();

                node.IsLeader.Should().BeFalse("stale timer message must be silently discarded");
            } finally {
                await node.DisposeAsync();
            }
        }

        [Fact]
        public async Task ThreeNodes_elects_single_leader_deterministically() {
            var transport = new InMemoryRaftTransport();
            var n1 = CreateNodeWithTransport("n1", ["n2", "n3"], transport);
            var n2 = CreateNodeWithTransport("n2", ["n1", "n3"], transport);
            var n3 = CreateNodeWithTransport("n3", ["n1", "n2"], transport);

            await Task.WhenAll(n1.StartAsync(), n2.StartAsync(), n3.StartAsync());
            try {
                // Force n1 into candidacy without waiting for the real election timer.
                // TriggerElectionTimerAsync posts VoteRequests synchronously to n2 and n3's
                // channels before returning, so the background actor loops of n2 and n3 pick
                // them up without any test-side pumping.  Responses travel back via fire-and-
                // forget tasks, so we wait for n1 to commit the no-op rather than stepping
                // through individual node drains.
                await n1.TriggerElectionTimerAsync();

                var leader = await WaitForLeader([n1, n2, n3], TimeSpan.FromSeconds(5));

                leader.Should().Be(n1, "n1 was the only candidate");
                n2.IsLeader.Should().BeFalse();
                n3.IsLeader.Should().BeFalse();
            } finally {
                await Task.WhenAll(n1.DisposeAsync().AsTask(), n2.DisposeAsync().AsTask(), n3.DisposeAsync().AsTask());
            }
        }

        [Fact]
        public async Task ThreeNodes_exactlyOneLeader_with_realTimers() {
            var transport = new InMemoryRaftTransport();
            var n1 = CreateNodeWithTransport("n1", ["n2", "n3"], transport, electionTimeoutMinMs: 50,
                electionTimeoutMaxMs: 100, heartbeatMs: 20);
            var n2 = CreateNodeWithTransport("n2", ["n1", "n3"], transport, electionTimeoutMinMs: 50,
                electionTimeoutMaxMs: 100, heartbeatMs: 20);
            var n3 = CreateNodeWithTransport("n3", ["n1", "n2"], transport, electionTimeoutMinMs: 50,
                electionTimeoutMaxMs: 100, heartbeatMs: 20);

            await Task.WhenAll(n1.StartAsync(), n2.StartAsync(), n3.StartAsync());
            try {
                await WaitForLeader([n1, n2, n3], TimeSpan.FromSeconds(5));

                new[] { n1, n2, n3 }.Count(n => n.IsLeader).Should().Be(1);
            } finally {
                await Task.WhenAll(n1.DisposeAsync().AsTask(), n2.DisposeAsync().AsTask(), n3.DisposeAsync().AsTask());
            }
        }

        [Fact]
        public async Task VoteResponse_fromPreviousTerm_isIgnored() {
            var transport = new RecordingRaftTransport();
            var node = CreateNode("n1", ["n2"], transport);

            await node.StartAsync();
            try {
                await node.TriggerElectionTimerAsync();
                // n1 is now a Candidate in term 1, with 1 vote (self).

                // Inject a vote response for an earlier term — must be discarded.
                node.Inject(new VoteResponseReceived("n2", new VoteResponse(Term: 1, VoteGranted: true), SentTerm: 0));
                await node.WhenIdleAsync();

                node.IsLeader.Should().BeFalse("vote response with stale SentTerm must be ignored");
            } finally {
                await node.DisposeAsync();
            }
        }

        [Fact]
        public async Task VoteResponse_afterRoleChange_isIgnored() {
            var transport = new RecordingRaftTransport();
            var node = CreateNode("n1", ["n2"], transport);

            await node.StartAsync();
            try {
                // Trigger election so the node is in Candidate role at term 1.
                await node.TriggerElectionTimerAsync();

                // Force the node back to Follower by injecting a higher-term AppendEntries.
                node.Inject(new AppendEntriesReceived(
                    new AppendEntriesRequest(Term: 5, LeaderId: "n2", PrevLogIndex: 0, PrevLogTerm: 0, Entries: [],
                        LeaderCommit: 0),
                    new TaskCompletionSource<AppendEntriesResponse>()));
                await node.WhenIdleAsync();

                // Now inject a vote response for term 1 — the node is a Follower, so it must be ignored.
                node.Inject(new VoteResponseReceived("n2", new VoteResponse(Term: 1, VoteGranted: true), SentTerm: 1));
                await node.WhenIdleAsync();

                node.IsLeader.Should().BeFalse("vote response after role change must be discarded");
            } finally {
                await node.DisposeAsync();
            }
        }

        [Fact]
        public async Task VoteDenied_whenCandidateLogIsBehind() {
            var transport = new InMemoryRaftTransport();
            var log = new InMemoryRaftLog();
            await log.AppendAsync([new RaftLogEntry(1, 1, RaftLogEntryType.Command, "data"u8.ToArray())]);

            var n1 = CreateNodeWithTransport("n1", ["n2"], transport, log: log);
            await n1.StartAsync();
            try {
                var response = await n1.ReceiveVoteRequestAsync(new VoteRequest(Term: 1, CandidateId: "n2", LastLogIndex: 0, LastLogTerm: 0));

                response.VoteGranted.Should().BeFalse("candidate with a shorter log must not receive a vote");
            } finally {
                await n1.DisposeAsync();
            }
        }

        [Fact]
        public async Task VoteGranted_resetsElectionTimer() {
            var transport = new RecordingRaftTransport();
            var node = CreateNode("n1", ["n2"], transport);

            await node.StartAsync();
            try {
                var generationBefore = node._electionTimerGeneration;

                var response = await node.ReceiveVoteRequestAsync(new VoteRequest(Term: 1, CandidateId: "n2", LastLogIndex: 0, LastLogTerm: 0));

                response.VoteGranted.Should().BeTrue();
                node._electionTimerGeneration.Should().BeGreaterThan(generationBefore,
                    "granting a vote must restart the election timer");
            } finally {
                await node.DisposeAsync();
            }
        }
    }

    public class ProposeAppendEntries {
        [Fact]
        public async Task Propose_commitsAfterQuorum() {
            var transport = new InMemoryRaftTransport();
            var n1 = CreateNodeWithTransport("n1", ["n2", "n3"], transport, electionTimeoutMinMs: 50, electionTimeoutMaxMs: 100, heartbeatMs: 20);
            var n2 = CreateNodeWithTransport("n2", ["n1", "n3"], transport, electionTimeoutMinMs: 50, electionTimeoutMaxMs: 100, heartbeatMs: 20);
            var n3 = CreateNodeWithTransport("n3", ["n1", "n2"], transport, electionTimeoutMinMs: 50, electionTimeoutMaxMs: 100, heartbeatMs: 20);

            await Task.WhenAll(n1.StartAsync(), n2.StartAsync(), n3.StartAsync());
            try {
                var leader = await WaitForLeader([n1, n2, n3], TimeSpan.FromSeconds(5));
                var result = await leader.ProposeAsync("hello"u8.ToArray());

                result.IsOccConflict.Should().BeFalse();
            } finally {
                await Task.WhenAll(n1.DisposeAsync().AsTask(), n2.DisposeAsync().AsTask(), n3.DisposeAsync().AsTask());
            }
        }

        [Fact]
        public async Task Propose_failsImmediately_whenNotLeader() {
            var transport = new RecordingRaftTransport();
            var node = CreateNode("n1", ["n2"], transport);

            await node.StartAsync();
            try {
                var act = async () => await node.ProposeAsync("hello"u8.ToArray());

                await act.Should().ThrowAsync<NotLeaderException>();
            } finally {
                await node.DisposeAsync();
            }
        }

        [Fact]
        public async Task Propose_failsImmediately_whenLeaderButNoOpNotYetCommitted() {
            // Use a two-node cluster with a RecordingTransport so we can prevent the no-op from
            // being committed: the node becomes leader but the peer never acknowledges AppendEntries.
            var transport = new RecordingRaftTransport();
            var node = CreateNode("n1", ["n2"], transport);

            await node.StartAsync();
            try {
                await node.TriggerElectionTimerAsync();
                // n1 is now Candidate and sent a VoteRequest to n2; grant it so n1 becomes Leader.
                transport.VoteRequests.TryDequeue(out var voteCall).Should().BeTrue();
                voteCall.Reply.SetResult(new VoteResponse(Term: 1, VoteGranted: true));
                await node.WhenIdleAsync();

                // n1 is Leader but the no-op AppendEntries to n2 is pending (not acknowledged).
                // ProposeAsync must throw NotLeaderException because IsLeader returns false (no-op uncommitted).
                var act = async () => await node.ProposeAsync("hello"u8.ToArray());
                await act.Should().ThrowAsync<NotLeaderException>("no-op is not yet committed");
            } finally {
                await node.DisposeAsync();
            }
        }
    }

    public class LeaderStepDown {
        [Fact]
        public async Task HigherTerm_causesLeaderToStepDown() {
            var transport = new InMemoryRaftTransport();
            var n1 = CreateNodeWithTransport("n1", ["n2", "n3"], transport, electionTimeoutMinMs: 50, electionTimeoutMaxMs: 100, heartbeatMs: 20);
            var n2 = CreateNodeWithTransport("n2", ["n1", "n3"], transport, electionTimeoutMinMs: 50, electionTimeoutMaxMs: 100, heartbeatMs: 20);
            var n3 = CreateNodeWithTransport("n3", ["n1", "n2"], transport, electionTimeoutMinMs: 50, electionTimeoutMaxMs: 100, heartbeatMs: 20);

            await Task.WhenAll(n1.StartAsync(), n2.StartAsync(), n3.StartAsync());
            try {
                var leader = await WaitForLeader([n1, n2, n3], TimeSpan.FromSeconds(5));

                await leader.ReceiveAppendEntriesAsync(
                    new AppendEntriesRequest(Term: 999, LeaderId: "fake", PrevLogIndex: 0, PrevLogTerm: 0, Entries: [], LeaderCommit: 0));

                leader.IsLeader.Should().BeFalse("a higher-term AppendEntries must cause the leader to step down");
            } finally {
                await Task.WhenAll(n1.DisposeAsync().AsTask(), n2.DisposeAsync().AsTask(), n3.DisposeAsync().AsTask());
            }
        }

        [Fact]
        public async Task HigherTerm_onVoteResponse_causesStepDown() {
            var transport = new RecordingRaftTransport();
            var node = CreateNode("n1", ["n2"], transport);

            await node.StartAsync();
            try {
                // Start an election so the node is a Candidate at term 1.
                await node.TriggerElectionTimerAsync();

                // Peer responds with a higher term — node must step down to Follower.
                node.Inject(new VoteResponseReceived("n2", new VoteResponse(Term: 5, VoteGranted: false), SentTerm: 1));
                await node.WhenIdleAsync();

                node.IsLeader.Should().BeFalse();
                node._currentTerm.Should().Be(5UL);
                node._role.Should().Be(NodeRole.Follower);
            } finally {
                await node.DisposeAsync();
            }
        }
    }

    public class Reelection {
        [Fact]
        public async Task ReElection_afterLeaderStop() {
            var transport = new InMemoryRaftTransport();
            var n1 = CreateNodeWithTransport("n1", ["n2", "n3"], transport, electionTimeoutMinMs: 50, electionTimeoutMaxMs: 100, heartbeatMs: 20);
            var n2 = CreateNodeWithTransport("n2", ["n1", "n3"], transport, electionTimeoutMinMs: 50, electionTimeoutMaxMs: 100, heartbeatMs: 20);
            var n3 = CreateNodeWithTransport("n3", ["n1", "n2"], transport, electionTimeoutMinMs: 50, electionTimeoutMaxMs: 100, heartbeatMs: 20);

            await Task.WhenAll(n1.StartAsync(), n2.StartAsync(), n3.StartAsync());
            RaftNode? firstLeader = null;
            try {
                firstLeader = await WaitForLeader([n1, n2, n3], TimeSpan.FromSeconds(5));
                await firstLeader.StopAsync();

                var remaining = new[] { n1, n2, n3 }.Where(n => n != firstLeader).ToArray();
                var newLeader = await WaitForLeader(remaining, TimeSpan.FromSeconds(5));
                newLeader.Should().NotBe(firstLeader);
            } finally {
                foreach (var n in new[] { n1, n2, n3 }.Where(n => n != firstLeader))
                    await n.DisposeAsync();
            }
        }
    }

    public class InstallSnapshot {
        [Fact]
        public async Task AdvancesLogAndStateMachine() {
            var transport = new RecordingRaftTransport();
            var log = new InMemoryRaftLog();
            var stateMachine = new NullStateMachine();
            var config = new RaftNodeConfig("n1", [], 10_000, 20_000, 5_000);
            var node = new RaftNode(config, log, transport, stateMachine, new InMemoryRaftStateStorage());

            await node.StartAsync();
            try {
                var resp = await node.ReceiveInstallSnapshotAsync(
                    new InstallSnapshotRequest(5, "leader", 5, 2, 0, ReadOnlyMemory<byte>.Empty, Done: true));

                resp.Term.Should().Be(5);
                log.LastIncludedIndex.Should().Be(5);
                stateMachine.LastAppliedIndex.Should().Be(5);
            } finally {
                await node.DisposeAsync();
            }
        }

        [Fact]
        public async Task StaleTerm_isIgnored() {
            var transport = new RecordingRaftTransport();
            var log = new InMemoryRaftLog();
            var config = new RaftNodeConfig("n1", [], 10_000, 20_000, 5_000);
            var node = new RaftNode(config, log, transport, new NullStateMachine(), new InMemoryRaftStateStorage());

            await node.StartAsync();
            try {
                await node.ReceiveInstallSnapshotAsync(
                    new InstallSnapshotRequest(5, "leader", 3, 2, 0, ReadOnlyMemory<byte>.Empty, Done: true));

                var resp = await node.ReceiveInstallSnapshotAsync(
                    new InstallSnapshotRequest(1, "oldleader", 1, 1, 0, ReadOnlyMemory<byte>.Empty, Done: true));

                resp.Term.Should().Be(5);
                log.LastIncludedIndex.Should().Be(3, "stale snapshot must not roll back the log");
            } finally {
                await node.DisposeAsync();
            }
        }

        [Fact]
        public async Task AlreadyAhead_isNoop() {
            var transport = new RecordingRaftTransport();
            var log = new InMemoryRaftLog();
            var config = new RaftNodeConfig("n1", [], 10_000, 20_000, 5_000);
            var node = new RaftNode(config, log, transport, new NullStateMachine(), new InMemoryRaftStateStorage());

            await node.StartAsync();
            try {
                await node.ReceiveInstallSnapshotAsync(
                    new InstallSnapshotRequest(5, "leader", 10, 3, 0, ReadOnlyMemory<byte>.Empty, Done: true));
                log.LastIncludedIndex.Should().Be(10);

                var resp = await node.ReceiveInstallSnapshotAsync(
                    new InstallSnapshotRequest(5, "leader", 5, 2, 0, ReadOnlyMemory<byte>.Empty, Done: true));

                resp.Term.Should().Be(5);
                log.LastIncludedIndex.Should().Be(10, "a lower-boundary snapshot must not roll back the log");
            } finally {
                await node.DisposeAsync();
            }
        }

        [Fact]
        public async Task HigherTerm_causesStepDown() {
            var transport = new InMemoryRaftTransport();
            var n1 = CreateNodeWithTransport("n1", [], transport, electionTimeoutMinMs: 50, electionTimeoutMaxMs: 100);

            await n1.StartAsync();
            try {
                await WaitForLeader([n1], TimeSpan.FromSeconds(2));
                n1.IsLeader.Should().BeTrue();

                var resp = await n1.ReceiveInstallSnapshotAsync(
                    new InstallSnapshotRequest(99, "n2", 5, 2, 0, ReadOnlyMemory<byte>.Empty, Done: true));

                resp.Term.Should().Be(99);
                n1.IsLeader.Should().BeFalse("a higher-term snapshot must cause step-down");
            } finally {
                await n1.DisposeAsync();
            }
        }

        [Fact]
        public async Task MultipleChunks_assemblesAndInstalls() {
            var transport = new RecordingRaftTransport();
            var log = new InMemoryRaftLog();
            var stateMachine = new NullStateMachine();
            var config = new RaftNodeConfig("n1", [], 10_000, 20_000, 5_000);
            var node = new RaftNode(config, log, transport, stateMachine, new InMemoryRaftStateStorage());

            var payload = new byte[] { 10, 20, 30, 40, 50, 60 };

            await node.StartAsync();
            try {
                await node.ReceiveInstallSnapshotAsync(new InstallSnapshotRequest(
                    5, "leader", 5, 2, 0, new ReadOnlyMemory<byte>(payload, 0, 2), Done: false));
                log.LastIncludedIndex.Should().Be(0, "partial chunk must not install");

                await node.ReceiveInstallSnapshotAsync(new InstallSnapshotRequest(
                    5, "leader", 5, 2, 2, new ReadOnlyMemory<byte>(payload, 2, 2), Done: false));
                log.LastIncludedIndex.Should().Be(0, "partial chunk must not install");

                var resp = await node.ReceiveInstallSnapshotAsync(new InstallSnapshotRequest(
                    5, "leader", 5, 2, 4, new ReadOnlyMemory<byte>(payload, 4, 2), Done: true));

                resp.Term.Should().Be(5);
                log.LastIncludedIndex.Should().Be(5);
                stateMachine.LastAppliedIndex.Should().Be(5);
                stateMachine.InstalledData!.Value.ToArray().Should().Equal(payload,
                    "all chunks must be reassembled into the original payload before installation");
            } finally {
                await node.DisposeAsync();
            }
        }

        [Fact]
        public async Task NewSnapshotAbandonsPreviousBuffer() {
            var transport = new RecordingRaftTransport();
            var log = new InMemoryRaftLog();
            var stateMachine = new NullStateMachine();
            var config = new RaftNodeConfig("n1", [], 10_000, 20_000, 5_000);
            var node = new RaftNode(config, log, transport, stateMachine, new InMemoryRaftStateStorage());

            await node.StartAsync();
            try {
                var staleChunk = new byte[] { 0xFF };
                var finalPayload = new byte[] { 0xAB, 0xCD };

                await node.ReceiveInstallSnapshotAsync(new InstallSnapshotRequest(
                    5, "leader", 5, 2, 0, staleChunk, Done: false));

                var resp = await node.ReceiveInstallSnapshotAsync(new InstallSnapshotRequest(
                    5, "leader", 10, 3, 0, finalPayload, Done: true));

                resp.Term.Should().Be(5);
                log.LastIncludedIndex.Should().Be(10);
                stateMachine.InstalledData!.Value.ToArray().Should().Equal(finalPayload,
                    "only the completed snapshot must be installed; the abandoned chunk must be discarded");
            } finally {
                await node.DisposeAsync();
            }
        }
    }

    public class AppendEntries {
        [Fact]
        public async Task CandidateSteepsDown_onValidCurrentTermMessage() {
            var transport = new RecordingRaftTransport();
            var node = CreateNode("n1", ["n2"], transport);

            await node.StartAsync();
            try {
                // Trigger election; node is now a Candidate in term 1.
                await node.TriggerElectionTimerAsync();
                node._role.Should().Be(NodeRole.Candidate);

                // Receive AppendEntries from the new leader in the same term.
                await node.ReceiveAppendEntriesAsync(
                    new AppendEntriesRequest(Term: 1, LeaderId: "n2", PrevLogIndex: 0, PrevLogTerm: 0, Entries: [], LeaderCommit: 0));

                node._role.Should().Be(NodeRole.Follower, "a Candidate must step down when it receives AppendEntries from the current term's leader");
            } finally {
                await node.DisposeAsync();
            }
        }

        [Fact]
        public async Task ResetsElectionTimer_onValidMessage() {
            var transport = new RecordingRaftTransport();
            var node = CreateNode("n1", [], transport);  // single node — no peers needed for this test

            await node.StartAsync();
            try {
                var generationBefore = node._electionTimerGeneration;

                // Receive a valid AppendEntries from a node claiming term 1.
                await node.ReceiveAppendEntriesAsync(
                    new AppendEntriesRequest(Term: 1, LeaderId: "n2", PrevLogIndex: 0, PrevLogTerm: 0, Entries: [], LeaderCommit: 0));

                node._electionTimerGeneration.Should().BeGreaterThan(generationBefore,
                    "a valid AppendEntries must restart the election timer");
            } finally {
                await node.DisposeAsync();
            }
        }
    }

    static async Task<RaftNode> WaitForLeader(RaftNode[] nodes, TimeSpan timeout) {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline) {
            var leader = nodes.FirstOrDefault(n => n.IsLeader);
            if (leader is not null) return leader;
            await Task.Delay(50);
        }
        throw new TimeoutException($"No leader elected within {timeout}.");
    }
}
