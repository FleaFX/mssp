using FluentAssertions;

namespace MSSP.LsmTree;

public class SkipListTests : IDisposable {
    readonly SkipList<string, string> _skipList = new();

    public void Dispose() =>
        _skipList.Dispose();

    public class Write : SkipListTests {
        [Fact]
        public void InsertedValue_IsRetrievable() {
            _skipList.Write("key", "value");

            _skipList.TryGet("key", out var value).Should().BeTrue();
            value.Should().Be("value");
        }

        [Fact]
        public void SameKey_UpdatesValue() {
            _skipList.Write("key", "first");
            _skipList.Write("key", "second");

            _skipList.TryGet("key", out var value).Should().BeTrue();
            value.Should().Be("second");
        }

        [Fact]
        public void MultipleKeys_AllRetrievable() {
            for (var i = 0; i < 100; i++)
                _skipList.Write($"key-{i:D3}", $"value-{i}");

            for (var i = 0; i < 100; i++) {
                _skipList.TryGet($"key-{i:D3}", out var value).Should().BeTrue();
                value.Should().Be($"value-{i}");
            }
        }
    }

    public class TryGet : SkipListTests {
        [Fact]
        public void EmptyList_ReturnsFalse() {
            _skipList.TryGet("key", out var value).Should().BeFalse();
            value.Should().BeNull();
        }

        [Fact]
        public void AbsentKey_ReturnsFalse() {
            _skipList.Write("other", "value");

            _skipList.TryGet("key", out _).Should().BeFalse();
        }
    }

    public class Delete : SkipListTests {
        [Fact]
        public void AbsentKey_ReturnsFalse() =>
            _skipList.Delete("key").Should().BeFalse();

        [Fact]
        public void PresentKey_ReturnsTrueAndRemovesEntry() {
            _skipList.Write("key", "value");

            _skipList.Delete("key").Should().BeTrue();
            _skipList.TryGet("key", out _).Should().BeFalse();
        }

        [Fact]
        public void AlreadyDeletedKey_ReturnsFalse() {
            _skipList.Write("key", "value");
            _skipList.Delete("key");

            _skipList.Delete("key").Should().BeFalse();
        }

        [Fact]
        public void DeleteMiddleKey_NeighboursRemainAccessible() {
            _skipList.Write("a", "1");
            _skipList.Write("b", "2");
            _skipList.Write("c", "3");

            _skipList.Delete("b");

            _skipList.TryGet("a", out var a).Should().BeTrue();
            a.Should().Be("1");
            _skipList.TryGet("c", out var c).Should().BeTrue();
            c.Should().Be("3");
        }
    }

    public class Enumerate : SkipListTests {
        [Fact]
        public void EmptyList_YieldsNothing() =>
            _skipList.Should().BeEmpty();

        [Fact]
        public void EntriesYieldedInAscendingKeyOrder() {
            _skipList.Write("c", "3");
            _skipList.Write("a", "1");
            _skipList.Write("b", "2");

            _skipList.Should().Equal(
                new KeyValuePair<string, string>("a", "1"),
                new KeyValuePair<string, string>("b", "2"),
                new KeyValuePair<string, string>("c", "3")
            );
        }

        [Fact]
        public void DeletedEntriesNotYielded() {
            _skipList.Write("a", "1");
            _skipList.Write("b", "2");
            _skipList.Write("c", "3");
            _skipList.Delete("b");

            _skipList.Select(kv => kv.Key).Should().Equal("a", "c");
        }
    }

    public class Scan : SkipListTests {
        [Fact]
        public void EmptyList_YieldsNothing() =>
            _skipList.Scan("b").Should().BeEmpty();

        [Fact]
        public void ScanFromExistingKey_IncludesThatKeyAndAfter() {
            _skipList.Write("a", "1");
            _skipList.Write("b", "2");
            _skipList.Write("c", "3");

            _skipList.Scan("b").Should().Equal(
                new KeyValuePair<string, string>("b", "2"),
                new KeyValuePair<string, string>("c", "3")
            );
        }

        [Fact]
        public void ScanFromBetweenKeys_StartsAtNextKey() {
            _skipList.Write("a", "1");
            _skipList.Write("c", "3");

            _skipList.Scan("b").Should().ContainSingle(kv => kv.Key == "c");
        }

        [Fact]
        public void ScanFromBeforeFirst_YieldsAll() {
            _skipList.Write("b", "2");
            _skipList.Write("c", "3");

            _skipList.Scan("a").Should().HaveCount(2);
        }

        [Fact]
        public void ScanFromAfterLast_YieldsNothing() {
            _skipList.Write("a", "1");
            _skipList.Write("b", "2");

            _skipList.Scan("c").Should().BeEmpty();
        }
    }

    public class LargeScale : SkipListTests {
        [Fact]
        public void AllEntriesRetrievable_At65536Entries() {
            for (var i = 0; i < 0x1_0000; i++)
                _skipList.Write($"{i:D6}", $"value-{i}");

            for (var i = 0; i < 0x1_0000; i++) {
                _skipList.TryGet($"{i:D6}", out var value).Should().BeTrue();
                value.Should().Be($"value-{i}");
            }
        }
    }

    public class CountTests : SkipListTests {
        [Fact]
        public void EmptyList_IsZero() =>
            _skipList.Count.Should().Be(0);

        [Fact]
        public void IncreasesOnInsert() {
            _skipList.Write("a", "1");
            _skipList.Write("b", "2");

            _skipList.Count.Should().Be(2);
        }

        [Fact]
        public void UnchangedOnUpdate() {
            _skipList.Write("a", "1");
            _skipList.Write("a", "2");

            _skipList.Count.Should().Be(1);
        }

        [Fact]
        public void DecreasesOnDelete() {
            _skipList.Write("a", "1");
            _skipList.Write("b", "2");
            _skipList.Delete("a");

            _skipList.Count.Should().Be(1);
        }
    }

    public class Concurrency : SkipListTests {
        [Fact]
        public async Task ConcurrentWrites_AllEntriesRetrievable() {
            const int threadCount = 8;
            const int writesPerThread = 1000;

            await Task.WhenAll(Enumerable.Range(0, threadCount).Select(thread => Task.Run(() => {
                for (var i = 0; i < writesPerThread; i++)
                    _skipList.Write($"{thread:D2}-{i:D4}", $"value-{thread}-{i}");
            })));

            for (var thread = 0; thread < threadCount; thread++) {
                for (var i = 0; i < writesPerThread; i++) {
                    _skipList.TryGet($"{thread:D2}-{i:D4}", out var value).Should().BeTrue();
                    value.Should().Be($"value-{thread}-{i}");
                }
            }

            _skipList.Count.Should().Be(threadCount * writesPerThread);
        }

        [Fact]
        public async Task ConcurrentReadsAndWrites_NoExceptions() {
            const int entryCount = 500;
            const int operationsPerTask = 1000;

            for (var i = 0; i < entryCount; i++)
                _skipList.Write($"{i:D4}", $"value-{i}");

            await Task.WhenAll(
                Task.Run(() => {
                    for (var i = entryCount; i < entryCount + operationsPerTask; i++)
                        _skipList.Write($"{i:D4}", $"value-{i}");
                }),
                Task.Run(() => {
                    for (var i = 0; i < operationsPerTask; i++)
                        _skipList.TryGet($"{i:D4}", out _);
                })
            );
        }
    }
}
