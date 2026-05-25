using FluentAssertions;
using MSSP.Raft;

namespace MSSP.Cluster;

public class SegmentedRaftLogTests {
    static RaftLogEntry Command(ulong term, ulong index, string payload = "x") =>
        new(term, index, RaftLogEntryType.Command, System.Text.Encoding.UTF8.GetBytes(payload));

    // ── OpenAsync ─────────────────────────────────────────────────────────────

    public class OpenAsync : IDisposable {
        readonly string _dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        public OpenAsync() => Directory.CreateDirectory(_dir);
        public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); }

        [Fact]
        public async Task NewDirectory_StartsEmpty() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir);

            log.LastIndex.Should().Be(0);
            log.LastTerm.Should().Be(0);
            log.LastIncludedIndex.Should().Be(0);
            log.LastIncludedTerm.Should().Be(0);
        }

        [Fact]
        public async Task ReopensExistingEntries() {
            {
                using var log = await SegmentedRaftLog.OpenAsync(_dir);
                await log.AppendAsync([Command(1, 1), Command(1, 2)]);
            }
            using var reopened = await SegmentedRaftLog.OpenAsync(_dir);

            reopened.LastIndex.Should().Be(2);
            var e1 = await reopened.GetEntryAsync(1);
            var e2 = await reopened.GetEntryAsync(2);
            e1.Term.Should().Be(1); e1.Index.Should().Be(1);
            e2.Term.Should().Be(1); e2.Index.Should().Be(2);
        }

        [Fact]
        public async Task TornWrite_IsDiscarded() {
            string segPath;
            {
                using var log = await SegmentedRaftLog.OpenAsync(_dir);
                await log.AppendAsync([Command(1, 1)]);
                segPath = Directory.GetFiles(_dir, "raft-*.seg").Single();
            }

            // corrupt the last 2 bytes of the segment (breaks the CRC footer)
            var len = new FileInfo(segPath).Length;
            using (var fs = new FileStream(segPath, FileMode.Open, FileAccess.Write)) fs.SetLength(len - 2);

            using var recovered = await SegmentedRaftLog.OpenAsync(_dir);
            recovered.LastIndex.Should().Be(0);
        }

        [Fact]
        public async Task RestoresSnapshotMetadata() {
            {
                using var log = await SegmentedRaftLog.OpenAsync(_dir);
                await log.AppendAsync([Command(1, 1), Command(1, 2)]);
                await log.CompactToAsync(2, 1);
            }
            using var reopened = await SegmentedRaftLog.OpenAsync(_dir);

            reopened.LastIncludedIndex.Should().Be(2);
            reopened.LastIncludedTerm.Should().Be(1);
        }
    }

    // ── AppendAsync ───────────────────────────────────────────────────────────

    public class AppendAsyncTests : IDisposable {
        readonly string _dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        public AppendAsyncTests() => Directory.CreateDirectory(_dir);
        public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); }

        [Fact]
        public async Task SingleEntry_IsReadBack() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir);
            await log.AppendAsync([Command(1, 1, "hello")]);

            var e = await log.GetEntryAsync(1);
            e.Term.Should().Be(1);
            e.Index.Should().Be(1);
            System.Text.Encoding.UTF8.GetString(e.Payload.Span).Should().Be("hello");
        }

        [Fact]
        public async Task MultipleEntries_IncrementLastIndex() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir);
            await log.AppendAsync([Command(1, 1), Command(1, 2), Command(2, 3)]);

            log.LastIndex.Should().Be(3);
            log.LastTerm.Should().Be(2);
        }

        [Fact]
        public async Task RotatesToNewSegment_WhenThresholdExceeded() {
            // each entry is HeaderSize(21) + payload(4) + CRC(4) = 29 bytes; threshold 50 forces rotation after 1 entry
            using var log = await SegmentedRaftLog.OpenAsync(_dir, maxSegmentBytes: 50);
            await log.AppendAsync([Command(1, 1, "aaaa"), Command(1, 2, "bbbb"), Command(1, 3, "cccc")]);

            Directory.GetFiles(_dir, "raft-*.seg").Should().HaveCountGreaterThan(1,
                "entries should be spread across multiple segment files");
            log.LastIndex.Should().Be(3);
        }

        [Fact]
        public async Task EntriesAreReadable_AcrossSegments() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir, maxSegmentBytes: 50);
            await log.AppendAsync([Command(1, 1, "first"), Command(1, 2, "second"), Command(2, 3, "third")]);

            for (ulong i = 1; i <= 3; i++) {
                var e = await log.GetEntryAsync(i);
                e.Index.Should().Be(i);
            }
        }
    }

    // ── GetTermAtAsync ────────────────────────────────────────────────────────

    public class GetTermAtAsyncTests : IDisposable {
        readonly string _dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        public GetTermAtAsyncTests() => Directory.CreateDirectory(_dir);
        public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); }

        [Fact]
        public async Task ReturnsTermFromEntry() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir);
            await log.AppendAsync([Command(3, 1), Command(5, 2)]);

            (await log.GetTermAtAsync(1)).Should().Be(3);
            (await log.GetTermAtAsync(2)).Should().Be(5);
        }

        [Fact]
        public async Task ReturnsLastIncludedTerm_WhenIndexEqualsLastIncludedIndex() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir);
            await log.AppendAsync([Command(2, 1), Command(2, 2)]);
            await log.CompactToAsync(2, 2);

            (await log.GetTermAtAsync(2)).Should().Be(2);
        }

        [Fact]
        public async Task ThrowsForCompactedIndex() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir);
            await log.AppendAsync([Command(1, 1), Command(1, 2), Command(1, 3)]);
            await log.CompactToAsync(2, 1);

            var act = async () => await log.GetTermAtAsync(1);
            await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
        }
    }

    // ── GetEntryAsync ─────────────────────────────────────────────────────────

    public class GetEntryAsyncTests : IDisposable {
        readonly string _dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        public GetEntryAsyncTests() => Directory.CreateDirectory(_dir);
        public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); }

        [Fact]
        public async Task ThrowsForIndexZero() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir);

            var act = async () => await log.GetEntryAsync(0);
            await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
        }

        [Fact]
        public async Task ThrowsForCompactedIndex() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir);
            await log.AppendAsync([Command(1, 1), Command(1, 2), Command(1, 3)]);
            await log.CompactToAsync(2, 1);

            var act = async () => await log.GetEntryAsync(1);
            await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
        }

        [Fact]
        public async Task ThrowsForIndexBeyondEnd() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir);
            await log.AppendAsync([Command(1, 1)]);

            var act = async () => await log.GetEntryAsync(2);
            await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
        }
    }

    // ── TruncateFromAsync ─────────────────────────────────────────────────────

    public class TruncateFromAsyncTests : IDisposable {
        readonly string _dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        public TruncateFromAsyncTests() => Directory.CreateDirectory(_dir);
        public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); }

        [Fact]
        public async Task RemovesEntriesFromIndex() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir);
            await log.AppendAsync([Command(1, 1), Command(1, 2), Command(2, 3)]);
            await log.TruncateFromAsync(2);

            log.LastIndex.Should().Be(1);
        }

        [Fact]
        public async Task AllowsAppendAfterTruncation() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir);
            await log.AppendAsync([Command(1, 1), Command(1, 2)]);
            await log.TruncateFromAsync(2);
            await log.AppendAsync([Command(2, 2)]);

            log.LastIndex.Should().Be(2);
            var e = await log.GetEntryAsync(2);
            e.Term.Should().Be(2);
        }

        [Fact]
        public async Task TruncatesAcrossSegments() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir, maxSegmentBytes: 50);
            await log.AppendAsync([Command(1, 1, "aaaa"), Command(1, 2, "bbbb"), Command(1, 3, "cccc")]);
            var segCountBefore = Directory.GetFiles(_dir, "raft-*.seg").Length;
            await log.TruncateFromAsync(2);

            log.LastIndex.Should().Be(1);
            Directory.GetFiles(_dir, "raft-*.seg").Length.Should().BeLessThan(segCountBefore,
                "segments after truncation point should be deleted");
        }
    }

    // ── CompactToAsync ────────────────────────────────────────────────────────

    public class CompactToAsyncTests : IDisposable {
        readonly string _dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        public CompactToAsyncTests() => Directory.CreateDirectory(_dir);
        public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); }

        [Fact]
        public async Task UpdatesLastIncludedIndexAndTerm() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir);
            await log.AppendAsync([Command(1, 1), Command(1, 2), Command(2, 3)]);
            await log.CompactToAsync(2, 1);

            log.LastIncludedIndex.Should().Be(2);
            log.LastIncludedTerm.Should().Be(1);
        }

        [Fact]
        public async Task DeletesSegmentsCoveredBySnapshot() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir, maxSegmentBytes: 50);
            await log.AppendAsync([Command(1, 1, "aaaa"), Command(1, 2, "bbbb"), Command(1, 3, "cccc")]);
            var segsBefore = Directory.GetFiles(_dir, "raft-*.seg").Length;
            segsBefore.Should().BeGreaterThan(1);

            await log.CompactToAsync(log.LastIndex - 1, 1);

            Directory.GetFiles(_dir, "raft-*.seg").Length.Should().BeLessThan(segsBefore,
                "segments fully covered by the snapshot must be deleted");
        }

        [Fact]
        public async Task EntriesAfterSnapshot_RemainsAccessible() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir);
            await log.AppendAsync([Command(1, 1), Command(1, 2), Command(2, 3)]);
            await log.CompactToAsync(2, 1);

            log.LastIndex.Should().Be(3);
            var e = await log.GetEntryAsync(3);
            e.Term.Should().Be(2);
        }

        [Fact]
        public async Task GetTermAtLastIncludedIndex_ReturnsLastIncludedTerm() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir);
            await log.AppendAsync([Command(3, 1), Command(3, 2)]);
            await log.CompactToAsync(2, 3);

            (await log.GetTermAtAsync(2)).Should().Be(3);
        }

        [Fact]
        public async Task IsIdempotent_WhenCalledWithSameIndex() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir);
            await log.AppendAsync([Command(1, 1), Command(1, 2)]);
            await log.CompactToAsync(1, 1);
            await log.CompactToAsync(1, 1); // no-op

            log.LastIncludedIndex.Should().Be(1);
        }

        [Fact]
        public async Task WritesSnapshotFile() {
            using var log = await SegmentedRaftLog.OpenAsync(_dir);
            await log.AppendAsync([Command(1, 1)]);
            await log.CompactToAsync(1, 1);

            File.Exists(Path.Combine(_dir, "raft-snapshot.json")).Should().BeTrue();
        }
    }

    // ── restart after compaction ──────────────────────────────────────────────

    public class RestartAfterCompaction : IDisposable {
        readonly string _dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        public RestartAfterCompaction() => Directory.CreateDirectory(_dir);
        public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); }

        [Fact]
        public async Task EntriesAfterSnapshot_SurviveRestart() {
            {
                using var log = await SegmentedRaftLog.OpenAsync(_dir);
                await log.AppendAsync([Command(1, 1), Command(1, 2), Command(2, 3)]);
                await log.CompactToAsync(2, 1);
            }
            using var reopened = await SegmentedRaftLog.OpenAsync(_dir);

            reopened.LastIncludedIndex.Should().Be(2);
            reopened.LastIncludedTerm.Should().Be(1);
            reopened.LastIndex.Should().Be(3);
            var e = await reopened.GetEntryAsync(3);
            e.Term.Should().Be(2);
            e.Index.Should().Be(3);
        }

        [Fact]
        public async Task FullyCompacted_LogIsEmpty_AfterRestart() {
            {
                using var log = await SegmentedRaftLog.OpenAsync(_dir);
                await log.AppendAsync([Command(1, 1), Command(1, 2)]);
                await log.CompactToAsync(2, 1);
            }
            using var reopened = await SegmentedRaftLog.OpenAsync(_dir);

            reopened.LastIncludedIndex.Should().Be(2);
            reopened.LastIndex.Should().Be(2);
            reopened.LastTerm.Should().Be(1);
        }

        [Fact]
        public async Task GetTermAtLastIncludedIndex_WorksAfterRestart() {
            {
                using var log = await SegmentedRaftLog.OpenAsync(_dir);
                await log.AppendAsync([Command(4, 1), Command(4, 2)]);
                await log.CompactToAsync(2, 4);
            }
            using var reopened = await SegmentedRaftLog.OpenAsync(_dir);

            (await reopened.GetTermAtAsync(2)).Should().Be(4);
        }
    }

    // ── AppendEntries — snapshot boundary behaviour ───────────────────────────

    public class AppendEntriesHandling {
        static (RaftNode node, InMemoryRaftTransport transport) CreateFollower(IRaftLog log) {
            var transport = new InMemoryRaftTransport();
            // use long election timeouts to avoid spurious elections during the test
            var config    = new RaftNodeConfig("follower", ["leader"], 10_000, 20_000, 5_000);
            var node      = new RaftNode(config, log, transport, new NullStateMachine(), new InMemoryRaftStateStorage());
            transport.Register(node);
            return (node, transport);
        }

        static RaftLogEntry Command(ulong term, ulong index) =>
            new(term, index, RaftLogEntryType.Command, System.Text.Encoding.UTF8.GetBytes("x"));

        [Fact]
        public async Task PrevLogIndex_EqualsLastIncludedIndex_Accepted() {
            var log = new InMemoryRaftLog();
            await log.AppendAsync([Command(1, 1), Command(1, 2), Command(1, 3)]);
            await log.CompactToAsync(2, 1);

            var (node, transport) = CreateFollower(log);
            await node.StartAsync();
            try {
                var request = new AppendEntriesRequest(
                    Term:         5,                // well above any auto-incremented term
                    LeaderId:     "leader",
                    PrevLogIndex: 2,                // == lastIncludedIndex
                    PrevLogTerm:  1,                // == lastIncludedTerm
                    Entries:      [Command(5, 4)],
                    LeaderCommit: 0);               // no commit — avoids ApplyCommittedEntries on compacted log

                var response = await transport.AppendEntriesAsync("follower", request);
                response.Success.Should().BeTrue("prevLogIndex == lastIncludedIndex should be accepted");
            } finally {
                await node.StopAsync();
            }
        }

        [Fact]
        public async Task PrevLogIndex_BelowLastIncludedIndex_Rejected() {
            var log = new InMemoryRaftLog();
            await log.AppendAsync([Command(1, 1), Command(1, 2), Command(1, 3)]);
            await log.CompactToAsync(3, 1);

            var (node, transport) = CreateFollower(log);
            await node.StartAsync();
            try {
                var request = new AppendEntriesRequest(
                    Term:         5,
                    LeaderId:     "leader",
                    PrevLogIndex: 2,        // < lastIncludedIndex (3)
                    PrevLogTerm:  1,
                    Entries:      [],
                    LeaderCommit: 1);

                var response = await transport.AppendEntriesAsync("follower", request);
                response.Success.Should().BeFalse("prevLogIndex below snapshot boundary must be rejected");
                response.ConflictIndex.Should().Be(4, "ConflictIndex must equal lastIncludedIndex + 1");
            } finally {
                await node.StopAsync();
            }
        }
    }
}
