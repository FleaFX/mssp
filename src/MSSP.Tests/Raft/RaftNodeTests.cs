using FluentAssertions;

namespace MSSP.Raft;

public class RaftNodeTests {
    static RaftNode CreateNode(string nodeId, string[] peers, InMemoryRaftTransport transport,
        int electionTimeoutMinMs = 50, int electionTimeoutMaxMs = 100, int heartbeatMs = 20) {
        var config = new RaftNodeConfig(nodeId, peers, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatMs);
        var node = new RaftNode(config, new InMemoryRaftLog(), transport, new NullStateMachine(), new InMemoryRaftStateStorage());
        transport.Register(node);
        return node;
    }

    [Fact]
    public async Task SingleNode_BecomesLeaderWithinTimeout() {
        var transport = new InMemoryRaftTransport();
        var node = CreateNode("n1", [], transport);

        await node.StartAsync();
        try {
            await WaitForLeader([node], TimeSpan.FromSeconds(2));
            node.IsLeader.Should().BeTrue();
        } finally {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task ThreeNodes_ExactlyOneLeader() {
        var transport = new InMemoryRaftTransport();
        var n1 = CreateNode("n1", ["n2", "n3"], transport);
        var n2 = CreateNode("n2", ["n1", "n3"], transport);
        var n3 = CreateNode("n3", ["n1", "n2"], transport);

        await Task.WhenAll(n1.StartAsync(), n2.StartAsync(), n3.StartAsync());
        try {
            await WaitForLeader([n1, n2, n3], TimeSpan.FromSeconds(5));

            var leaders = new[] { n1, n2, n3 }.Count(n => n.IsLeader);
            leaders.Should().Be(1);
        } finally {
            await Task.WhenAll(n1.StopAsync(), n2.StopAsync(), n3.StopAsync());
        }
    }

    [Fact]
    public async Task Propose_CommitsAfterQuorum() {
        var transport = new InMemoryRaftTransport();
        var n1 = CreateNode("n1", ["n2", "n3"], transport);
        var n2 = CreateNode("n2", ["n1", "n3"], transport);
        var n3 = CreateNode("n3", ["n1", "n2"], transport);

        await Task.WhenAll(n1.StartAsync(), n2.StartAsync(), n3.StartAsync());
        try {
            var leader = await WaitForLeader([n1, n2, n3], TimeSpan.FromSeconds(5));
            var result = await leader.ProposeAsync("hello"u8.ToArray());
            result.IsOccConflict.Should().BeFalse();
        } finally {
            await Task.WhenAll(n1.StopAsync(), n2.StopAsync(), n3.StopAsync());
        }
    }

    [Fact]
    public async Task HigherTerm_CausesLeaderToStepDown() {
        var transport = new InMemoryRaftTransport();
        var n1 = CreateNode("n1", ["n2", "n3"], transport);
        var n2 = CreateNode("n2", ["n1", "n3"], transport);
        var n3 = CreateNode("n3", ["n1", "n2"], transport);

        await Task.WhenAll(n1.StartAsync(), n2.StartAsync(), n3.StartAsync());
        try {
            var leader = await WaitForLeader([n1, n2, n3], TimeSpan.FromSeconds(5));

            // send AppendEntries with a higher term → leader must step down
            var higherTerm = new AppendEntriesRequest(999, "fake", 0, 0, [], 0);
            await leader.ReceiveAppendEntriesAsync(higherTerm);

            // IsLeader is false immediately after the await: BecomeFollowerAsync runs before the TCS is set
            leader.IsLeader.Should().BeFalse();
        } finally {
            await Task.WhenAll(n1.StopAsync(), n2.StopAsync(), n3.StopAsync());
        }
    }

    [Fact]
    public async Task VoteDenied_WhenCandidateLogIsBehind() {
        var transport = new InMemoryRaftTransport();
        // n1 has a longer log; n2 tries to request vote
        var logN1 = new InMemoryRaftLog();
        await logN1.AppendAsync([new RaftLogEntry(1, 1, RaftLogEntryType.Command, "data"u8.ToArray())]);

        var config = new RaftNodeConfig("n1", ["n2"], 50, 100, 20);
        var n1 = new RaftNode(config, logN1, transport, new NullStateMachine(), new InMemoryRaftStateStorage());
        transport.Register(n1);
        await n1.StartAsync();

        try {
            // n2 candidate with empty log requests vote from n1 with term 1
            var voteRequest = new VoteRequest(1, "n2", 0, 0);
            var response = await n1.ReceiveVoteRequestAsync(voteRequest);

            response.VoteGranted.Should().BeFalse();
        } finally {
            await n1.StopAsync();
        }
    }

    [Fact]
    public async Task ReElection_AfterLeaderStop() {
        var transport = new InMemoryRaftTransport();
        var n1 = CreateNode("n1", ["n2", "n3"], transport);
        var n2 = CreateNode("n2", ["n1", "n3"], transport);
        var n3 = CreateNode("n3", ["n1", "n2"], transport);

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
                await n.StopAsync();
        }
    }

    [Fact]
    public async Task InstallSnapshot_AdvancesLogAndStateMachine() {
        var transport = new InMemoryRaftTransport();
        var log = new InMemoryRaftLog();
        var stateMachine = new NullStateMachine();
        var config = new RaftNodeConfig("n1", [], 10_000, 20_000, 5_000);
        var node = new RaftNode(config, log, transport, stateMachine, new InMemoryRaftStateStorage());
        transport.Register(node);

        await node.StartAsync();
        try {
            var resp = await node.ReceiveInstallSnapshotAsync(new InstallSnapshotRequest(5, "leader", 5, 2, 0, ReadOnlyMemory<byte>.Empty, Done: true));
            resp.Term.Should().Be(5);
            log.LastIncludedIndex.Should().Be(5);
            stateMachine.LastAppliedIndex.Should().Be(5);
        } finally {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task InstallSnapshot_StaleTerm_IsIgnored() {
        var transport = new InMemoryRaftTransport();
        var log = new InMemoryRaftLog();
        var stateMachine = new NullStateMachine();
        var config = new RaftNodeConfig("n1", [], 10_000, 20_000, 5_000);
        var node = new RaftNode(config, log, transport, stateMachine, new InMemoryRaftStateStorage());
        transport.Register(node);

        await node.StartAsync();
        try {
            // Advance the node's term to 5 via a valid snapshot.
            await node.ReceiveInstallSnapshotAsync(new InstallSnapshotRequest(5, "leader", 3, 2, 0, ReadOnlyMemory<byte>.Empty, Done: true));

            // A stale snapshot with term=1 should be rejected.
            var resp = await node.ReceiveInstallSnapshotAsync(new InstallSnapshotRequest(1, "oldleader", 1, 1, 0, ReadOnlyMemory<byte>.Empty, Done: true));
            resp.Term.Should().Be(5);
            log.LastIncludedIndex.Should().Be(3); // unchanged
        } finally {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task InstallSnapshot_AlreadyAhead_IsNoop() {
        var transport = new InMemoryRaftTransport();
        var log = new InMemoryRaftLog();
        var stateMachine = new NullStateMachine();
        var config = new RaftNodeConfig("n1", [], 10_000, 20_000, 5_000);
        var node = new RaftNode(config, log, transport, stateMachine, new InMemoryRaftStateStorage());
        transport.Register(node);

        await node.StartAsync();
        try {
            // Compact the follower's log ahead to index 10.
            await node.ReceiveInstallSnapshotAsync(new InstallSnapshotRequest(5, "leader", 10, 3, 0, ReadOnlyMemory<byte>.Empty, Done: true));
            log.LastIncludedIndex.Should().Be(10);

            // A later snapshot at index 5 is stale and must not roll back the log.
            var resp = await node.ReceiveInstallSnapshotAsync(new InstallSnapshotRequest(5, "leader", 5, 2, 0, ReadOnlyMemory<byte>.Empty, Done: true));
            resp.Term.Should().Be(5);
            log.LastIncludedIndex.Should().Be(10); // unchanged
        } finally {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task InstallSnapshot_HigherTerm_CausesStepDown() {
        var transport = new InMemoryRaftTransport();
        var node = CreateNode("n1", [], transport, electionTimeoutMinMs: 50, electionTimeoutMaxMs: 100);

        await node.StartAsync();
        try {
            await WaitForLeader([node], TimeSpan.FromSeconds(2));
            node.IsLeader.Should().BeTrue();

            var resp = await node.ReceiveInstallSnapshotAsync(new InstallSnapshotRequest(99, "n2", 5, 2, 0, ReadOnlyMemory<byte>.Empty, Done: true));
            resp.Term.Should().Be(99);
            node.IsLeader.Should().BeFalse();
        } finally {
            await node.StopAsync();
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
