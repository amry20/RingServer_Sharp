/**************************************************************************
 * Generic.cs
 *
 * Generic utility routines ported from generic.c.
 *
 * This file is part of the ringserver C# port.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Copyright (C) 2024-2026:
 * Ported to C# from original C code by Chad Trabant, EarthScope Data Services
 **************************************************************************/

using System;
using System.Runtime.CompilerServices;
using System.Text;
using RingServer.Types;

namespace RingServer;

/// <summary>
/// Generic utility routines.
/// </summary>
public static class Generic
{
    /// <summary>
    /// Split stream ID into separate components according to pattern:
    /// "id1_id2_id3_id4_id5_id6/TYPE"
    /// Returns count of identifiers returned (including type) on success and -1 on error.
    /// Equivalent to SplitStreamID() in generic.c.
    /// </summary>
    public static int SplitStreamID(string streamid, char delim, int maxlength,
                                     out string? id1, out string? id2, out string? id3,
                                     out string? id4, out string? id5, out string? id6,
                                     out string? type)
    {
        id1 = id2 = id3 = id4 = id5 = id6 = type = null;
        if (string.IsNullOrEmpty(streamid))
            return -1;

        if (delim == '\0')
            delim = '_';

        var ids = new string?[6];
        int count = 0;

        // Split by '/' for type
        int typeSep = streamid.LastIndexOf('/');
        string idPart;
        if (typeSep >= 0)
        {
            type = streamid[(typeSep + 1)..];
            if (type.Length > maxlength)
                type = type[..maxlength];
            count++;
            idPart = streamid[..typeSep];
        }
        else
        {
            type = "";
            idPart = streamid;
        }

        // Split by delimiter
        var parts = idPart.Split(delim, 6);
        for (int i = 0; i < parts.Length && i < 6; i++)
        {
            ids[i] = parts[i];
            if (ids[i]!.Length > maxlength)
                ids[i] = ids[i]![..maxlength];
        }

        if (ids[0] != null) { id1 = ids[0]; count++; }
        if (ids[1] != null) { id2 = ids[1]; count++; }
        if (ids[2] != null) { id3 = ids[2]; count++; }
        if (ids[3] != null) { id4 = ids[3]; count++; }
        if (ids[4] != null) { id5 = ids[4]; count++; }
        if (ids[5] != null) { id6 = ids[5]; count++; }

        return count;
    }

    /// <summary>
    /// Return the current time as a high precision nanosecond epoch.
    /// Equivalent to NSnow() in generic.c.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static NsTime NSnow()
    {
        return NsTime.Now;
    }

    /// <summary>
    /// Perform a 64-bit Fowler/Noll/Vo hash (FNV-1a) on a string.
    /// Equivalent to FNVhash64() in generic.c.
    /// </summary>
    public static ulong FNVhash64(string? str)
    {
        if (str == null)
            return 0;

        ulong hval = 0xCBF29CE484222325;

        foreach (char ch in str)
        {
            byte b = (byte)(ch & 0xFF);
            hval = 0x00000100000001B3 * (hval ^ b);
        }

        return hval;
    }

    /// <summary>
    /// Compare two Keys. For use with RBTree when keys are ulong.
    /// Equivalent to KeyCompare() in generic.c.
    /// </summary>
    public static int KeyCompare(object? a, object? b)
    {
        ulong va = a is ulong ulA ? ulA : Convert.ToUInt64(a);
        ulong vb = b is ulong ulB ? ulB : Convert.ToUInt64(b);

        if (va > vb) return 1;
        if (va < vb) return -1;
        return 0;
    }

    /// <summary>
    /// Compare two string keys for RBTree.
    /// </summary>
    public static int StringKeyCompare(object? a, object? b)
    {
        return string.Compare(a?.ToString(), b?.ToString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// Return 1 if the specified string is all digits and 0 otherwise.
    /// Equivalent to IsAllDigits() in generic.c.
    /// </summary>
    public static bool IsAllDigits(string? str)
    {
        if (string.IsNullOrEmpty(str))
            return false;

        foreach (char c in str)
        {
            if (!char.IsDigit(c))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Convert a size in bytes to a human readable string (KiB, MiB, GiB, etc.).
    /// Equivalent to HumanSizeString() in generic.c.
    /// </summary>
    public static string HumanSizeString(ulong bytes)
    {
        string[] units = { "B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB" };
        double size = bytes;
        int idx = 0;

        while (size >= 1024.0 && idx < 6)
        {
            size /= 1024.0;
            idx++;
        }

        if (idx == 0)
            return $"{bytes} {units[idx]}";
        else
            return $"{size:F1} {units[idx]}";
    }
}
