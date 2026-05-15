namespace MSSP.Embedded;

sealed class RevisionIndex {
    readonly Dictionary<string, ulong> _revisions = new();

    internal bool Contains(string streamId) =>
        _revisions.ContainsKey(streamId);

    internal bool TryGet(string streamId, out ulong revision) =>
        _revisions.TryGetValue(streamId, out revision);

    internal void Set(string streamId, ulong revision) =>
        _revisions[streamId] = revision;

    internal bool CheckConcurrency(string streamId, StreamRevision expected) {
        var exists = _revisions.TryGetValue(streamId, out var current);
        if (expected == StreamRevision.Any) return true;
        if (expected == StreamRevision.NoStream) return !exists;
        if (expected == StreamRevision.StreamExists) return exists;
        return exists && current == expected;
    }
}
