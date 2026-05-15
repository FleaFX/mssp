namespace MSSP.Embedded;

sealed class RevisionIndex {
    readonly Dictionary<string, ulong> _revisions = new();

    /// <summary>
    /// Returns <see langword="true"/> if the revision for <paramref name="streamId"/> is cached.
    /// </summary>
    internal bool Contains(string streamId) =>
        _revisions.ContainsKey(streamId);

    /// <summary>
    /// Tries to get the cached revision for <paramref name="streamId"/>.
    /// </summary>
    internal bool TryGet(string streamId, out ulong revision) =>
        _revisions.TryGetValue(streamId, out revision);

    /// <summary>
    /// Sets the cached revision for <paramref name="streamId"/>.
    /// </summary>
    internal void Set(string streamId, ulong revision) =>
        _revisions[streamId] = revision;

    /// <summary>
    /// Returns <see langword="true"/> if the current state satisfies the <paramref name="expected"/> revision constraint.
    /// </summary>
    internal bool CheckConcurrency(string streamId, StreamRevision expected) {
        var exists = _revisions.TryGetValue(streamId, out var current);
        if (expected == StreamRevision.Any) return true;
        if (expected == StreamRevision.NoStream) return !exists;
        if (expected == StreamRevision.StreamExists) return exists;
        return exists && current == expected;
    }
}
