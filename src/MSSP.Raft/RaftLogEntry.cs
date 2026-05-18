namespace MSSP.Raft;

public enum RaftLogEntryType : byte {
    NoOp = 0,
    Command = 1,
}

public readonly record struct RaftLogEntry(ulong Term, ulong Index, RaftLogEntryType Type, ReadOnlyMemory<byte> Payload);
