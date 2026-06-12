namespace MSSP.Engine.BloomFilters;

/// <summary>
/// The path to the bloom filter sidecar file (<c>.bf</c>) that corresponds to a given SST file.
/// </summary>
/// <param name="path">The path to the SST file.</param>
public readonly struct BloomFilterPath(string path) {
    readonly string _path = Path.ChangeExtension(path, ".bf");

    /// <summary>
    /// Implicitly casts the given <see cref="BloomFilterPath"/> to a <see cref="string"/>.
    /// </summary>
    /// <param name="instance">The <see cref="BloomFilterPath"/> to cast.</param>
    public static implicit operator string(BloomFilterPath instance) => instance._path;
}
