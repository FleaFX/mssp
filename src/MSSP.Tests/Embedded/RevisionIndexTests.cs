using FluentAssertions;

namespace MSSP.Engine;

public class RevisionIndexTests {
    readonly RevisionIndex _index = new();

    public class Contains : RevisionIndexTests {
        [Fact]
        public void UnknownStream_ReturnsFalse() =>
            _index.Contains("stream-a").Should().BeFalse();

        [Fact]
        public void AfterSet_ReturnsTrue() {
            _index.Set("stream-a", 0UL);

            _index.Contains("stream-a").Should().BeTrue();
        }
    }

    public class TryGet : RevisionIndexTests {
        [Fact]
        public void UnknownStream_ReturnsFalse() {
            var found = _index.TryGet("stream-a", out var revision);

            found.Should().BeFalse();
            revision.Should().Be(0UL);
        }

        [Fact]
        public void AfterSet_ReturnsTrueWithRevision() {
            _index.Set("stream-a", 42UL);

            var found = _index.TryGet("stream-a", out var revision);

            found.Should().BeTrue();
            revision.Should().Be(42UL);
        }
    }

    public class Set : RevisionIndexTests {
        [Fact]
        public void OverwritesPreviousRevision() {
            _index.Set("stream-a", 5UL);
            _index.Set("stream-a", 10UL);

            _index.TryGet("stream-a", out var revision);
            revision.Should().Be(10UL);
        }
    }

    public class CheckConcurrency : RevisionIndexTests {
        [Fact]
        public void Any_ReturnsTrueWhenAbsent() =>
            _index.CheckConcurrency("stream-a", StreamRevision.Any).Should().BeTrue();

        [Fact]
        public void Any_ReturnsTrueWhenPresent() {
            _index.Set("stream-a", 5UL);

            _index.CheckConcurrency("stream-a", StreamRevision.Any).Should().BeTrue();
        }

        [Fact]
        public void NoStream_ReturnsTrueWhenAbsent() =>
            _index.CheckConcurrency("stream-a", StreamRevision.NoStream).Should().BeTrue();

        [Fact]
        public void NoStream_ReturnsFalseWhenPresent() {
            _index.Set("stream-a", 0UL);

            _index.CheckConcurrency("stream-a", StreamRevision.NoStream).Should().BeFalse();
        }

        [Fact]
        public void StreamExists_ReturnsFalseWhenAbsent() =>
            _index.CheckConcurrency("stream-a", StreamRevision.StreamExists).Should().BeFalse();

        [Fact]
        public void StreamExists_ReturnsTrueWhenPresent() {
            _index.Set("stream-a", 0UL);

            _index.CheckConcurrency("stream-a", StreamRevision.StreamExists).Should().BeTrue();
        }

        [Fact]
        public void SpecificRevision_ReturnsTrueWhenMatches() {
            _index.Set("stream-a", 5UL);

            _index.CheckConcurrency("stream-a", 5UL).Should().BeTrue();
        }

        [Fact]
        public void SpecificRevision_ReturnsFalseWhenMismatch() {
            _index.Set("stream-a", 5UL);

            _index.CheckConcurrency("stream-a", 3UL).Should().BeFalse();
        }

        [Fact]
        public void SpecificRevision_ReturnsFalseWhenAbsent() =>
            _index.CheckConcurrency("stream-a", 0UL).Should().BeFalse();
    }
}
