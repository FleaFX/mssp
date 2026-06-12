namespace MSSP.Engine.Storage;

// SST binary format:
//   Data section   — sequential entries, each: marker(1) + keyLen(4) + keyBytes + [valueLen(4) + valueBytes]
//   Index section  — sparse index entries, each: keyLen(4) + keyBytes + dataOffset(8)
//   Footer         — magic(8) + indexOffset(8) + entryCount(4) + indexEntryCount(4) + sparseInterval(4) = 28 bytes
static class Sst {
    internal const int FooterSize = 28;
    internal const byte WriteMarker = 0x01;
    internal const byte TombstoneMarker = 0x02;
    internal const int DefaultSparseInterval = 128;
    internal static ReadOnlySpan<byte> Magic => "MSSPSST\0"u8;
}
