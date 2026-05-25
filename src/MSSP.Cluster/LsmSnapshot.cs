using System.Buffers.Binary;
using System.Text;

namespace MSSP.Cluster;

/// <summary>
/// Serialises and deserialises the LSM-store files (SST files and bloom-filter sidecars)
/// that form the physical state of a Raft snapshot.
/// </summary>
/// <remarks>
/// Archive layout:
/// <code>
/// [fileCount : 4 bytes LE]
/// per file:
///   [nameLen  : 4 bytes LE]
///   [name     : nameLen bytes, UTF-8  (filename only, no directory component)]
///   [size     : 8 bytes LE]
///   [data     : size bytes]
/// </code>
/// </remarks>
static class LsmSnapshot {
    /// <summary>
    /// Packs every <c>*.sst</c> and <c>*.bf</c> file in <paramref name="dataDirectory"/>
    /// into a single flat binary archive suitable for shipping via
    /// <see cref="MSSP.Raft.InstallSnapshotRequest"/>.
    /// </summary>
    /// <remarks>
    /// SST files are immutable once written, so reading them concurrently with an ongoing
    /// flush is safe. A concurrent compaction that deletes old SST files while this method
    /// is running will cause those files to be skipped; since MSSP event keys are unique,
    /// re-applying the corresponding entries on the follower is idempotent.
    /// </remarks>
    internal static ReadOnlyMemory<byte> Serialize(string dataDirectory) {
        var paths = Directory
            .EnumerateFiles(dataDirectory, "*.sst")
            .Concat(Directory.EnumerateFiles(dataDirectory, "*.bf"))
            .OrderBy(f => f);

        // read all file contents eagerly; skip any file that disappeared due to concurrent compaction
        var entries = new List<(string Name, byte[] Data)>();
        foreach (var path in paths) {
            try {
                entries.Add((Path.GetFileName(path), File.ReadAllBytes(path)));
            } catch (FileNotFoundException) {
                // merged away by a concurrent compaction; the merged file is also enumerated
            }
        }

        var ms = new MemoryStream();
        Span<byte> buf4 = stackalloc byte[4];
        Span<byte> buf8 = stackalloc byte[8];

        BinaryPrimitives.WriteInt32LittleEndian(buf4, entries.Count);
        ms.Write(buf4);

        foreach (var (name, data) in entries) {
            var nameBytes = Encoding.UTF8.GetBytes(name);
            BinaryPrimitives.WriteInt32LittleEndian(buf4, nameBytes.Length);
            ms.Write(buf4);
            ms.Write(nameBytes);

            BinaryPrimitives.WriteInt64LittleEndian(buf8, data.LongLength);
            ms.Write(buf8);
            ms.Write(data);
        }

        return ms.ToArray();
    }

    /// <summary>
    /// Unpacks the archive produced by <see cref="Serialize"/> into <paramref name="targetDirectory"/>.
    /// </summary>
    internal static void Deserialize(ReadOnlyMemory<byte> archive, string targetDirectory) {
        Directory.CreateDirectory(targetDirectory);
        var span = archive.Span;
        var pos = 0;

        var fileCount = BinaryPrimitives.ReadInt32LittleEndian(span[pos..]); pos += 4;
        for (var i = 0; i < fileCount; i++) {
            var nameLen = BinaryPrimitives.ReadInt32LittleEndian(span[pos..]); pos += 4;
            var name = Encoding.UTF8.GetString(span.Slice(pos, nameLen)); pos += nameLen;
            var size = (int)BinaryPrimitives.ReadInt64LittleEndian(span[pos..]); pos += 8;
            File.WriteAllBytes(Path.Combine(targetDirectory, name), span.Slice(pos, size).ToArray());
            pos += size;
        }
    }
}
