using System.Buffers.Binary;

namespace MSSP.Storage;

public sealed partial class LsmStore<TKey> {
    /// <summary>
    /// Replays WAL records into the MemTable, skipping any keys already present in SST files.
    /// Called once during <see cref="OpenAsync"/> before the store is handed to callers.
    /// </summary>
    internal async ValueTask RecoverAsync(IAsyncEnumerable<ReadOnlyMemory<byte>> walRecords, CancellationToken cancellationToken) {
        var sstKeys = new HashSet<TKey>();
        foreach (var level in _sstLevels) {
            foreach (var file in level) {
                using var reader = _sst.OpenReader(file.FilePath);
                foreach (var (key, _) in reader.Scan())
                    sstKeys.Add(key);
            }
        }

        await foreach (var bytes in walRecords.WithCancellation(cancellationToken)) {
            var span = bytes.Span;
            if (span.Length < 5) continue;
            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(span[1..]);
            if (keyLen < 0 || 5 + keyLen > span.Length) continue;

            if (span[0] == WalRecord.TombstoneMarker) {
                _memTable.ApplyRecord(bytes);
                continue;
            }

            if (span[0] != WalRecord.WriteMarker) continue;
            TKey key = bytes.Slice(5, keyLen);
            if (!sstKeys.Contains(key))
                _memTable.ApplyRecord(bytes);
        }
    }

    /// <summary>
    /// Replaces the current SST files with those from <paramref name="sourceDirectory"/> and
    /// resets the MemTable to empty. Called by the cluster layer after an
    /// <c>InstallSnapshot</c> RPC has delivered a snapshot archive to
    /// <paramref name="sourceDirectory"/>.
    /// </summary>
    /// <remarks>
    /// <see cref="LsmStore{TKey}"/> is not thread-safe. The caller is responsible for ensuring
    /// no concurrent writes, flushes, or compactions are in progress.
    /// </remarks>
    internal async ValueTask ReloadAsync(string sourceDirectory, CancellationToken cancellationToken) {
        // Remove all current SST files (ISstAccess.Delete also removes .bf sidecars).
        foreach (var level in _sstLevels) {
            foreach (var file in level)
                _sst.Delete(file.FilePath);
        }
        _sstLevels.Clear();

        // Copy new .sst files into the data directory and register them per level.
        foreach (var srcPath in Directory.EnumerateFiles(sourceDirectory, "*.sst").OrderBy(f => f)) {
            var sstInfo = SstFileInfo.Parse(srcPath, 0); // size irrelevant; only level is needed from source path
            var destPath = Path.Combine(_dataDirectory, Path.GetFileName(srcPath));
            await CopyFileAsync(srcPath, destPath, cancellationToken);
            AddFileToLevel(_sstLevels, new SstFileInfo(destPath, sstInfo.Level, new FileInfo(destPath).Length));
        }

        // Copy .bf sidecar files (managed by ISstAccess, not tracked in _sstLevels).
        foreach (var srcPath in Directory.EnumerateFiles(sourceDirectory, "*.bf")) {
            var destPath = Path.Combine(_dataDirectory, Path.GetFileName(srcPath));
            await CopyFileAsync(srcPath, destPath, cancellationToken);
        }

        // Reset MemTable; entries before the snapshot are now covered by the new SST files.
        _memTable.Dispose();
        _memTable = new MemTable<TKey>(_capacityBytes);

        // Ensure at least L1 exists when the snapshot source was empty,
        // so FlushAsync can always index into _sstLevels[0].
        if (_sstLevels.Count == 0)
            _sstLevels.Add(new List<SstFileInfo>());

        // No CompactAsync here: snapshot data is already compacted. Running it again
        // after InstallSnapshot would slow down cluster recovery unnecessarily.
    }

    static async ValueTask CopyFileAsync(string source, string destination, CancellationToken cancellationToken) {
        await using var src = new FileStream(
            source,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 81920,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        await using var dest = new FileStream(
            destination,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None,
            bufferSize: 81920,
            FileOptions.Asynchronous);
        await src.CopyToAsync(dest, cancellationToken);
    }
}
