using System.Text;

namespace MSSP.LsmTree;

sealed record StringKey(string Value) : IKey<StringKey> {
    public int CompareTo(StringKey? other) =>
        string.Compare(Value, other?.Value, StringComparison.Ordinal);
    public static implicit operator ReadOnlyMemory<byte>(StringKey key) =>
        Encoding.UTF8.GetBytes(key.Value);
    public static implicit operator StringKey(ReadOnlyMemory<byte> memory) =>
        new(Encoding.UTF8.GetString(memory.Span));
}
