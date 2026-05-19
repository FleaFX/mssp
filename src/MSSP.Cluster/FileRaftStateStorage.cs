using System.Text.Json;
using MSSP.Raft;

namespace MSSP.Cluster;

/// <summary>
/// Persists <see cref="RaftPersistentState"/> to a JSON file using an atomic tmp→rename write.
/// </summary>
sealed class FileRaftStateStorage(string dataDirectory) : IRaftStateStorage {
    static string StatePath(string dir) => Path.Combine(dir, "raft-state.json");

    /// <inheritdoc/>
    public async ValueTask<RaftPersistentState> LoadAsync(CancellationToken ct = default) {
        var path = StatePath(dataDirectory);
        if (!File.Exists(path)) return new RaftPersistentState(0, null);
        var json = await File.ReadAllTextAsync(path, ct);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        var term = root.GetProperty("currentTerm").GetUInt64();
        var votedFor = root.TryGetProperty("votedFor", out var vf) && vf.ValueKind != JsonValueKind.Null
            ? vf.GetString()
            : null;
        return new RaftPersistentState(term, votedFor);
    }

    /// <inheritdoc/>
    public async ValueTask SaveAsync(RaftPersistentState state, CancellationToken ct = default) {
        Directory.CreateDirectory(dataDirectory);
        var path = StatePath(dataDirectory);
        var tmp = path + ".tmp";
        var votedFor = state.VotedFor is null ? "null" : $"\"{state.VotedFor}\"";
        await File.WriteAllTextAsync(tmp, $"{{\"currentTerm\":{state.CurrentTerm},\"votedFor\":{votedFor}}}", ct);
        File.Move(tmp, path, overwrite: true);
    }
}
