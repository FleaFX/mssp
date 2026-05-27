using System.Diagnostics;

namespace MSSP.Embedded;

/// <summary>
/// Lightweight allocation-free timer. Captures the start timestamp on construction
/// and exposes the elapsed time in milliseconds via <see cref="ElapsedMs"/>. 
/// </summary>
/// <remarks>
/// Use <see cref="Start"/> to create an instance and read <see cref="ElapsedMs"/>
/// after the operation completes. Safe to store across await points (plain struct,
/// not a ref struct).
/// </remarks>
internal readonly struct OperationTimer() {
    readonly long _start = Stopwatch.GetTimestamp();

    /// <summary>Starts a new timer.</summary>
    internal static OperationTimer Start() => new();

    /// <summary>Elapsed milliseconds since <see cref="Start"/> was called.</summary>
    internal long ElapsedMs => (long)Stopwatch.GetElapsedTime(_start).TotalMilliseconds;
}
