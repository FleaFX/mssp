using System.IO.Compression;

namespace MSSP.Engine;

public sealed partial class EmbeddedMsspClient {
    /// <summary>
    /// Creates a compressed backup of the store as a ZIP archive at <paramref name="backupPath"/>.
    /// Archives all active SST files, their bloom filter sidecars, the subscription log, and the WAL.
    /// Writes that started before this call are guaranteed to be included;
    /// writes that start after may or may not be included (fuzzy backup).
    /// </summary>
    /// <remarks>
    /// Only available on instances created via <see cref="OpenAsync"/>.
    /// The store remains fully operational during the backup.
    /// </remarks>
    public async ValueTask CreateBackupAsync(string backupPath, CancellationToken cancellationToken = default) {
        if (_dataDirectory is null || _lsmStore is null)
            throw new InvalidOperationException($"{nameof(CreateBackupAsync)} is only available on instances created via {nameof(OpenAsync)}.");

        var parentDir = Path.GetDirectoryName(backupPath);
        if (!string.IsNullOrEmpty(parentDir))
            Directory.CreateDirectory(parentDir);

        // Open SST file handles on the actor thread (engine path) or under _writeLock (legacy path).
        // Either way, FileShare.Delete allows compaction to unlink files while the handle is open.
        IReadOnlyList<FileStream> sstStreams;
        if (_engine is { } engine) {
            sstStreams = await engine.OpenBackupStreamsAsync(cancellationToken);
        } else {
            var streams = new List<FileStream>();
            await _writeLock.WaitAsync(cancellationToken);
            try {
                foreach (var filePath in _lsmStore!.GetActiveFilePaths())
                    streams.Add(new FileStream(filePath, FileMode.Open, FileAccess.Read,
                        FileShare.ReadWrite | FileShare.Delete,
                        bufferSize: 81920, FileOptions.Asynchronous | FileOptions.SequentialScan));
            } catch {
                foreach (var s in streams) s.Dispose();
                _writeLock.Release();
                throw;
            }
            _writeLock.Release();
            sstStreams = streams;
        }

        // Write compressed archive. On any failure the partial archive is deleted
        // so callers are never left with a file that looks complete but is corrupt.
        var backupCreated = false;
        try {
            await using var zipStream = new FileStream(backupPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 81920, FileOptions.Asynchronous);
            await using var archive = new ZipArchive(zipStream, ZipArchiveMode.Create, leaveOpen: false);

            foreach (var stream in sstStreams)
                await AddStreamToArchiveAsync(archive, Path.GetFileName(stream.Name), stream, cancellationToken);

            // Subscription log must be included so GlobalPosition continuity is preserved on restore.
            // SubscriptionPipeline initialises _globalSequence from the log's last position; without it
            // the sequence restarts at 0 and new events collide with pre-backup positions.
            foreach (var logFile in Directory.EnumerateFiles(_dataDirectory, "subscriptions-*.log").OrderBy(f => f))
                await AddToArchiveAsync(archive, logFile, cancellationToken);

            var walPrevPath = Path.Combine(_dataDirectory, "wal_prev.log");
            if (File.Exists(walPrevPath))
                await AddToArchiveAsync(archive, walPrevPath, cancellationToken);

            var walPath = Path.Combine(_dataDirectory, "wal.log");
            if (File.Exists(walPath))
                await AddToArchiveAsync(archive, walPath, cancellationToken);

            backupCreated = true;
        } finally {
            foreach (var s in sstStreams) s.Dispose();
            if (!backupCreated)
                try { File.Delete(backupPath); } catch { /* best-effort cleanup of partial archive */ }
        }
    }

    /// <summary>
    /// Extracts a backup archive created by <see cref="CreateBackupAsync"/> into
    /// <paramref name="targetDirectory"/>, replacing any existing SST files and WAL.
    /// After this call, open the store at <paramref name="targetDirectory"/> with
    /// <see cref="OpenAsync"/> to resume from the backup state.
    /// </summary>
    /// <remarks>
    /// This is an offline operation. The store at <paramref name="targetDirectory"/> must not be
    /// open while this method runs.
    /// </remarks>
    public static async ValueTask RestoreBackupAsync(
        string backupPath,
        string targetDirectory,
        CancellationToken cancellationToken = default) {

        Directory.CreateDirectory(targetDirectory);

        // Remove existing SST, .bf, subscription log, and WAL files from targetDirectory.
        foreach (var file in Directory.EnumerateFiles(targetDirectory, "*.sst")
                     .Concat(Directory.EnumerateFiles(targetDirectory, "*.bf"))
                     .Concat(Directory.EnumerateFiles(targetDirectory, "subscriptions-*.log")))
            File.Delete(file);

        var existingWalPrev = Path.Combine(targetDirectory, "wal_prev.log");
        if (File.Exists(existingWalPrev))
            File.Delete(existingWalPrev);

        var existingWal = Path.Combine(targetDirectory, "wal.log");
        if (File.Exists(existingWal))
            File.Delete(existingWal);

        // Extract archive entries to targetDirectory.
        // Validate each entry's resolved path to prevent ZipSlip: an entry with a
        // name like "../evil" or an absolute path must not escape targetDirectory.
        var resolvedTarget = Path.GetFullPath(targetDirectory).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;

        await using var zipStream = new FileStream(backupPath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 81920, FileOptions.Asynchronous);
        await using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read, leaveOpen: false);

        foreach (var entry in archive.Entries) {
            var destPath = Path.GetFullPath(Path.Combine(targetDirectory, entry.FullName));
            if (!destPath.StartsWith(resolvedTarget, StringComparison.OrdinalIgnoreCase))
                throw new InvalidDataException($"Backup entry '{entry.Name}' resolves outside the target directory.");
            await using var entryStream = entry.Open();
            await using var destStream = new FileStream(destPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 81920, FileOptions.Asynchronous);
            await entryStream.CopyToAsync(destStream, cancellationToken);
        }
    }

    static async Task AddToArchiveAsync(ZipArchive archive, string filePath, CancellationToken cancellationToken) {
        await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);
        await AddStreamToArchiveAsync(archive, Path.GetFileName(filePath), fileStream, cancellationToken);
    }

    static async Task AddStreamToArchiveAsync(ZipArchive archive, string entryName, Stream source, CancellationToken cancellationToken) {
        var entry = archive.CreateEntry(entryName, CompressionLevel.Optimal);
        await using var entryStream = entry.Open();
        await source.CopyToAsync(entryStream, cancellationToken);
    }
}
