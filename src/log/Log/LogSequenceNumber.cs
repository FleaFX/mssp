using System.Numerics;

namespace Log;

/// <summary>
/// Represents a log record's offset within the log.
/// </summary>
readonly struct LogSequenceNumber :
      IAdditionOperators<LogSequenceNumber, long, LogSequenceNumber>
    , ISubtractionOperators<LogSequenceNumber, long, LogSequenceNumber>
    , IIncrementOperators<LogSequenceNumber>
    , IComparable<LogSequenceNumber> {
    readonly long _value;

    /// <summary>
    /// The <see cref="LogSequenceNumber"/> to use when starting from an empty log.
    /// </summary>
    public static LogSequenceNumber Initial => new();

    /// <summary>
    /// Initializes a new <see cref="LogSequenceNumber"/>.
    /// </summary>
    /// <param name="value">The ordinal value of the log sequence number.</param>
    public LogSequenceNumber(long value) {
        _value = value;
    }

    /// <summary>
    /// Compares the current instance with another object of the same type and returns an integer that indicates whether the current instance precedes, follows, or occurs in the same position in the sort order as the other object.
    /// </summary>
    /// <param name="other">An object to compare with this instance.</param>
    /// <returns>A value that indicates the relative order of the objects being compared. The return value has these meanings:
    ///     <list type="table">
    ///      <listheader>
    ///             <term> Value</term>
    ///             <description> Meaning</description>
    ///         </listheader>
    ///      <item>
    ///             <term> Less than zero</term>
    ///             <description> This instance precedes <paramref name="other" /> in the sort order.</description>
    ///         </item>
    ///      <item>
    ///             <term> Zero</term>
    ///             <description> This instance occurs in the same position in the sort order as <paramref name="other" />.</description>
    ///      </item>
    ///         <item>
    ///             <term> Greater than zero</term>
    ///             <description> This instance follows <paramref name="other" /> in the sort order.</description>
    ///         </item>
    ///     </list>
    /// </returns>
    public int CompareTo(LogSequenceNumber other) => _value.CompareTo(other._value);

    /// <summary>Adds two values together to compute their sum.</summary>
    /// <param name="lsn">The value to which <paramref name="val" /> is added.</param>
    /// <param name="val">The value which is added to <paramref name="lsn" />.</param>
    /// <returns>The sum of <paramref name="lsn" /> and <paramref name="val" />.</returns>
    public static LogSequenceNumber operator +(LogSequenceNumber lsn, long val) => new(lsn._value + val);

    /// <summary>Subtracts two values to compute their difference.</summary>
    /// <param name="lsn">The value from which <paramref name="val" /> is subtracted.</param>
    /// <param name="val">The value which is subtracted from <paramref name="lsn" />.</param>
    /// <returns>The value of <paramref name="val" /> subtracted from <paramref name="lsn" />.</returns>
    public static LogSequenceNumber operator -(LogSequenceNumber lsn, long val) => new(lsn._value - val);

    /// <summary>Increments a value.</summary>
    /// <param name="lsn">The log sequence number to increment.</param>
    /// <returns>The result of incrementing <paramref name="lsn" />.</returns>
    public static LogSequenceNumber operator ++(LogSequenceNumber lsn) => new(lsn._value + 1);

    /// <summary>Returns the fully qualified type name of this instance.</summary>
    /// <returns>The fully qualified type name.</returns>
    public override string ToString() => $"seq. {_value}";
}