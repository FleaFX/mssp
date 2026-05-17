using System.Numerics;

namespace MSSP.Log;

/// <summary>
/// Represents a log record's offset within the log.
/// </summary>
readonly struct LogSequenceNumber(long value) :
      IAdditionOperators<LogSequenceNumber, long, LogSequenceNumber>
    , ISubtractionOperators<LogSequenceNumber, long, LogSequenceNumber>
    , IIncrementOperators<LogSequenceNumber>
    , IComparable<LogSequenceNumber> {
    readonly long _value = value;

    /// <summary>
    /// The <see cref="LogSequenceNumber"/> to use when starting from an empty log.
    /// </summary>
    public static LogSequenceNumber Initial => new();

    /// <inheritdoc/>
    public int CompareTo(LogSequenceNumber other) => _value.CompareTo(other._value);

    /// <inheritdoc/>
    public static LogSequenceNumber operator +(LogSequenceNumber lsn, long val) => new(lsn._value + val);

    /// <inheritdoc/>
    public static LogSequenceNumber operator -(LogSequenceNumber lsn, long val) => new(lsn._value - val);

    /// <inheritdoc/>
    public static LogSequenceNumber operator ++(LogSequenceNumber lsn) => new(lsn._value + 1);

    /// <inheritdoc/>
    public override string ToString() => $"seq. {_value}";
}
