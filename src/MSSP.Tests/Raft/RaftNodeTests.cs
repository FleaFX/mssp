using FluentAssertions;
using MSSP.Raft;

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
