using FluentAssertions;
using MSSP.Raft;
using MSSP.Storage;

namespace MSSP.Cluster;

public class RaftLogStateMachineTests {
    static RaftLogEntry CommandEntry(ulong index, string payload = "data") =>
        new(Term: 1, Index: index, Type: RaftLogEntryType.Command,
            Payload: System.Text.Encoding.UTF8.GetBytes(payload));

    static RaftLogEntry NoOpEntry(ulong index) =>
        new(Term: 1, Index: index, Type: RaftLogEntryType.NoOp, Payload: ReadOnlyMemory<byte>.Empty);

    [Fact]
    public void MarkApplied_SetsLastAppliedIndex() {
        var sm = new RaftLogStateMachine();

        sm.MarkApplied(7);

        sm.LastAppliedIndex.Should().Be(7);
    }

    [Fact]
    public void MarkApplied_OverwritesPreviousIndex() {
        var sm = new RaftLogStateMachine();
        sm.MarkApplied(3);

        sm.MarkApplied(9);

        sm.LastAppliedIndex.Should().Be(9);
    }

    [Fact]
    public async Task MarkApplied_DoesNotWriteToCommittedRecordsChannel() {
        // If MarkApplied wrote to the channel the apply loop would pick it up and,
        // in the worst case, prematurely resolve a pending write TCS.
        var sm = new RaftLogStateMachine();

        sm.MarkApplied(5);

        // A channel item would be immediately available; 100 ms is ample time to observe it.
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        var received = false;
        try {
            await foreach (var _ in sm.CommittedRecords.ReadAllAsync(cts.Token))
                received = true;
        } catch (OperationCanceledException) { /* expected — channel is empty */ }

        received.Should().BeFalse("MarkApplied must not write to the committed-records channel");
    }

    [Fact]
    public async Task ApplyAsync_CommandEntry_WritesToCommittedRecordsChannel() {
        // Sanity check: the normal ApplyAsync path still works correctly.
        var sm = new RaftLogStateMachine();
        var entry = CommandEntry(index: 1);

        await sm.ApplyAsync(entry);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        var received = false;
        try {
            await foreach (var _ in sm.CommittedRecords.ReadAllAsync(cts.Token))
                received = true;
        } catch (OperationCanceledException) { /* timeout after the one item was received */ }

        received.Should().BeTrue("ApplyAsync for a Command entry must write to the channel");
    }

    [Fact]
    public async Task ApplyAsync_NoOpEntry_DoesNotWriteToCommittedRecordsChannel() {
        var sm = new RaftLogStateMachine();

        await sm.ApplyAsync(NoOpEntry(index: 1));

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        var received = false;
        try {
            await foreach (var _ in sm.CommittedRecords.ReadAllAsync(cts.Token))
                received = true;
        } catch (OperationCanceledException) { }

        received.Should().BeFalse("ApplyAsync for a NoOp entry must not write to the channel");
    }

    [Fact]
    public async Task MarkApplied_UpdatesIndex_EquallyToApplyAsync() {
        // MarkApplied and ApplyAsync must both update LastAppliedIndex to the same value.
        var sm1 = new RaftLogStateMachine();
        var sm2 = new RaftLogStateMachine();
        var entry = NoOpEntry(index: 42);

        sm1.MarkApplied(42);
        await sm2.ApplyAsync(entry);

        sm1.LastAppliedIndex.Should().Be(sm2.LastAppliedIndex);
    }
}
