using System.Text.RegularExpressions;

namespace MSSP.Engine.Storage;

/// <summary>Represents an SST file with its level and size.</summary>
/// <param name="FilePath">The full path to the SST file.</param>
/// <param name="Level">The level name (1 = L1, 2 = L2, etc.).</param>
/// <param name="SizeBytes">The file size in bytes.</param>
public readonly record struct SstFileInfo(string FilePath, int Level, long SizeBytes) {
    static readonly Regex LevelPattern = new(@"_L(\d+)\.sst$", RegexOptions.Compiled);

    /// <summary>Parses a file path to extract the level. Defaults to level 1 if no suffix is found.</summary>
    /// <param name="path">The SST file path (e.g., "1234567890_L2.sst").</param>
    /// <param name="sizeBytes">The file size in bytes.</param>
    public static SstFileInfo Parse(string path, long sizeBytes) {
        var match = LevelPattern.Match(path);
        return match.Success
            ? new SstFileInfo(path, int.Parse(match.Groups[1].Value), sizeBytes)
            : new SstFileInfo(path, 1, sizeBytes); // Default to L1 for existing files
    }

    /// <summary>Gets the bloom filter sidecar path for this SST file (e.g. "foo_L2.sst.bf").</summary>
    public string BloomFilterPath => FilePath + ".bf";
}
