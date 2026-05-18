namespace MSSP;

/// <summary>
/// Contract for keys used in the LSM tree.
/// Keys must be orderable and serializable to bytes for WAL persistence.
/// </summary>
/// <typeparam name="TSelf">The implementing type.</typeparam>
public interface IKey<TSelf> : IComparable<TSelf>, IEquatable<TSelf> where TSelf : IKey<TSelf> {
    /// <summary>
    /// Implicitly converts <paramref name="key"/> to its byte representation.
    /// </summary>
    /// <param name="key">The key to convert.</param>
    static abstract implicit operator ReadOnlyMemory<byte>(TSelf key);

    /// <summary>
    /// Implicitly converts <paramref name="memory"/> to a <typeparamref name="TSelf"/> key.
    /// </summary>
    /// <param name="memory">The bytes to convert.</param>
    static abstract implicit operator TSelf(ReadOnlyMemory<byte> memory);
}
