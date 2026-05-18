namespace MSSP.Cluster;

/// <summary>
/// In-memory cache of the latest committed revision per stream, used for optimistic
/// concurrency checks at state-machine apply time.
/// </summary>
sealed class RevisionIndex {
    readonly Dictionary<string, ulong> _revisions = new();

    /// <summary>
    /// Returns <c>true</c> if at least one event has been committed to <paramref name="streamId"/>.
    /// </summary>
    internal bool Contains(string streamId) => _revisions.ContainsKey(streamId);

    /// <summary>
    /// Attempts to retrieve the latest committed revision for <paramref name="streamId"/>.
    /// </summary>
    /// <param name="streamId">The stream to look up.</param>
    /// <param name="revision">When found, the zero-based revision of the last committed event.</param>
    /// <returns><c>true</c> if the stream exists in the index; otherwise <c>false</c>.</returns>
    internal bool TryGet(string streamId, out ulong revision) => _revisions.TryGetValue(streamId, out revision);

    /// <summary>
    /// Records that the latest committed revision for <paramref name="streamId"/> is <paramref name="revision"/>.
    /// </summary>
    internal void Set(string streamId, ulong revision) => _revisions[streamId] = revision;

    /// <summary>
    /// Checks whether the <paramref name="expected"/> revision is compatible with the current state of
    /// <paramref name="streamId"/>.
    /// </summary>
    /// <param name="streamId">The stream whose concurrency to check.</param>
    /// <param name="expected">The revision constraint supplied by the writer.</param>
    /// <returns><c>true</c> if the write may proceed; <c>false</c> if it would conflict.</returns>
    internal bool CheckConcurrency(string streamId, StreamRevision expected) {
        var exists = _revisions.TryGetValue(streamId, out var current);
        if (expected == StreamRevision.Any) return true;
        if (expected == StreamRevision.NoStream) return !exists;
        if (expected == StreamRevision.StreamExists) return exists;
        return exists && current == expected;
    }
}
