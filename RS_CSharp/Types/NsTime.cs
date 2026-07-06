/**************************************************************************
 * NsTime.cs
 *
 * Nanosecond precision timestamp, equivalent to libmseed nstime_t.
 *
 * This file is part of the ringserver C# port.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 **************************************************************************/

using System;

namespace RingServer.Types;

/// <summary>
/// Nanosecond-precision timestamp.
/// Represents the number of nanoseconds since the Unix epoch (1970-01-01).
/// Equivalent to nstime_t in the C code (int64_t).
/// </summary>
public readonly struct NsTime : IEquatable<NsTime>, IComparable<NsTime>
{
    /// <summary>Number of nanoseconds since Unix epoch</summary>
    public long Value { get; }

    public NsTime(long nanoseconds)
    {
        Value = nanoseconds;
    }

    public static NsTime Unset => new(long.MinValue);
    public static NsTime Error => new(long.MinValue + 1);
    public static NsTime Zero => new(0);

    public bool IsUnset => Value == long.MinValue;
    public bool IsError => Value == long.MinValue + 1;

    /// <summary>
    /// Get current time as nanoseconds since epoch
    /// </summary>
    public static NsTime Now
    {
        get
        {
            var utcNow = DateTime.UtcNow;
            var unixEpoch = DateTime.UnixEpoch;
            var elapsed = utcNow - unixEpoch;
            long ns = elapsed.Ticks * 100; // 1 tick = 100 ns
            return new NsTime(ns);
        }
    }

    /// <summary>
    /// Convert from seconds (double) to NsTime
    /// </summary>
    public static NsTime FromSeconds(double seconds)
    {
        return new NsTime((long)(seconds * Constants.NsModulus));
    }

    /// <summary>
    /// Convert to seconds as a double
    /// </summary>
    public double ToSeconds()
    {
        return (double)Value / Constants.NsModulus;
    }

    /// <summary>
    /// Convert to DateTime (with loss of sub-tick precision)
    /// </summary>
    public DateTime ToDateTime()
    {
        var ticks = Value / 100; // nanoseconds to ticks
        return DateTime.UnixEpoch.AddTicks(ticks);
    }

    /// <summary>
    /// Create from DateTime
    /// </summary>
    public static NsTime FromDateTime(DateTime dt)
    {
        var elapsed = dt.ToUniversalTime() - DateTime.UnixEpoch;
        return new NsTime(elapsed.Ticks * 100);
    }

    public static NsTime operator +(NsTime a, NsTime b) => new(a.Value + b.Value);
    public static NsTime operator -(NsTime a, NsTime b) => new(a.Value - b.Value);
    public static bool operator >(NsTime a, NsTime b) => a.Value > b.Value;
    public static bool operator <(NsTime a, NsTime b) => a.Value < b.Value;
    public static bool operator >=(NsTime a, NsTime b) => a.Value >= b.Value;
    public static bool operator <=(NsTime a, NsTime b) => a.Value <= b.Value;

    public bool Equals(NsTime other) => Value == other.Value;
    public override bool Equals(object? obj) => obj is NsTime other && Equals(other);
    public override int GetHashCode() => Value.GetHashCode();
    public int CompareTo(NsTime other) => Value.CompareTo(other.Value);

    public static bool operator ==(NsTime a, NsTime b) => a.Value == b.Value;
    public static bool operator !=(NsTime a, NsTime b) => a.Value != b.Value;

    public override string ToString()
    {
        if (IsUnset) return "UNSET";
        if (IsError) return "ERROR";
        return ToDateTime().ToString("yyyy-MM-ddTHH:mm:ss.fffffff");
    }

    /// <summary>
    /// Format as ISO time string (equivalent to ms_nstime2timestr)
    /// </summary>
    public string ToIsoString()
    {
        if (IsUnset) return "UNSET";
        if (IsError) return "ERROR";
        return ToDateTime().ToString("yyyy-MM-ddTHH:mm:ss.ffffff");
    }
}
