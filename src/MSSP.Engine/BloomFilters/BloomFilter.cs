using System.Buffers.Binary;

namespace MSSP.Engine.BloomFilters;

/// <summary>
/// A space-efficient probabilistic data structure that tests set membership.
/// May return false positives; never returns false negatives.
/// </summary>
public sealed class BloomFilter {
    readonly byte[] _bits;
    readonly int _m;
    readonly int _k;

    BloomFilter(int m, int k, byte[] bits) {
        _m = m;
        _k = k;
        _bits = bits;
    }

    /// <summary>
    /// Creates a new empty bloom filter sized for <paramref name="expectedItems"/> items
    /// with a target false positive rate of <paramref name="falsePositiveRate"/>.
    /// </summary>
    public static BloomFilter Create(int expectedItems, double falsePositiveRate = 0.01) {
        if (expectedItems <= 0) throw new ArgumentOutOfRangeException(nameof(expectedItems));
        var m = OptimalBitCount(expectedItems, falsePositiveRate);
        var k = OptimalHashCount(expectedItems, m);
        return new BloomFilter(m, k, new byte[(m + 7) / 8]);
    }

    /// <summary>
    /// Adds <paramref name="item"/> to the filter.
    /// </summary>
    public void Add(ReadOnlySpan<byte> item) {
        for (var i = 0; i < _k; i++) {
            var bit = (int)(DoubleHash(item, (uint)i) % (uint)_m);
            _bits[bit >> 3] |= (byte)(1 << (bit & 7));
        }
    }

    /// <summary>
    /// Returns <c>true</c> if <paramref name="item"/> may be in the set (possible false positive),
    /// or <c>false</c> if it is definitely not in the set.
    /// </summary>
    public bool MayContain(ReadOnlySpan<byte> item) {
        for (var i = 0; i < _k; i++) {
            var bit = (int)(DoubleHash(item, (uint)i) % (uint)_m);
            if ((_bits[bit >> 3] & (byte)(1 << (bit & 7))) == 0)
                return false;
        }
        return true;
    }

    /// <summary>
    /// Writes the filter to <paramref name="output"/> in binary format: m(4) + k(4) + bits(ceil(m/8)).
    /// </summary>
    public void WriteTo(Stream output) {
        Span<byte> header = stackalloc byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(header[0..], _m);
        BinaryPrimitives.WriteInt32LittleEndian(header[4..], _k);
        output.Write(header);
        output.Write(_bits);
    }

    /// <summary>
    /// Reads a filter from <paramref name="input"/>.
    /// </summary>
    /// <exception cref="InvalidDataException">The stream does not contain a valid bloom filter.</exception>
    public static BloomFilter ReadFrom(Stream input) {
        Span<byte> header = stackalloc byte[8];
        input.ReadExactly(header);
        var m = BinaryPrimitives.ReadInt32LittleEndian(header[0..]);
        var k = BinaryPrimitives.ReadInt32LittleEndian(header[4..]);
        if (m <= 0 || k <= 0)
            throw new InvalidDataException($"Invalid bloom filter header: m={m}, k={k}.");
        var bits = new byte[(m + 7) / 8];
        input.ReadExactly(bits);
        return new BloomFilter(m, k, bits);
    }

    // Double hashing: g(i, x) = (h1(x) + i * h2(x)) mod m
    // h1 and h2 are derived from the upper and lower 32 bits of a single 64-bit FNV-1a hash,
    // which yields two statistically independent values from one pass over the data.
    // See Kirsch & Mitzenmacher (2006): https://doi.org/10.1007/11841036_42
    uint DoubleHash(ReadOnlySpan<byte> data, uint i) {
        var h = Fnv1a64(data);
        var h1 = (uint)(h >> 32);
        var h2 = (uint)h | 1; // odd h2 ensures full coverage when m is a power of two
        return h1 + i * h2;
    }

    // ReSharper disable once InconsistentNaming
    static ulong Fnv1a64(ReadOnlySpan<byte> data) {
        const ulong prime = 1099511628211UL;
        var hash = 14695981039346656037UL;
        foreach (var b in data) {
            hash ^= b;
            hash *= prime;
        }
        return hash;
    }

    static int OptimalBitCount(int n, double p) =>
        Math.Max(8, (int)Math.Ceiling(-(n * Math.Log(p)) / (Math.Log(2) * Math.Log(2))));

    static int OptimalHashCount(int n, int m) =>
        Math.Max(1, (int)Math.Round((double)m / n * Math.Log(2)));
}
