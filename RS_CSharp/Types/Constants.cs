/**************************************************************************
 * Constants.cs
 *
 * Global constants for ringserver.
 *
 * This file is part of the ringserver C# port.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright (C) 2024-2026:
 * Ported to C# from original C code by Chad Trabant, EarthScope Data Services
 **************************************************************************/

namespace RingServer.Types;

/// <summary>
/// Global constants for the ringserver
/// </summary>
public static class Constants
{
    public const string Package = "ringserver";
    public const string Version = "4.0.1-csharp";

    // Ring buffer constants
    public const string RingSignature = "RING";
    public const ushort RingVersion = 2;

    // Special ring packet ID values (highest 10 values are reserved)
    public const ulong RingIdError = ulong.MaxValue;
    public const ulong RingIdNone = ulong.MaxValue - 1;
    public const ulong RingIdEarliest = ulong.MaxValue - 2;
    public const ulong RingIdLatest = ulong.MaxValue - 3;
    public const ulong RingIdNext = ulong.MaxValue - 4;
    public const ulong RingIdMaximum = ulong.MaxValue - 10;

    // Maximum stream ID string length
    public const int MaxStreamId = 60;

    // Regex pattern to match legacy stream IDs for miniSEED using SEED codes
    public const string LegacyMseedStreamIdPattern = @"^[0-9A-Z]{1,2}_[0-9A-Z]{1,5}_[0-9A-Z]{0,2}_[0-9A-Z]{3}/MSEED$";

    // SeedLink constants
    public const string SlServerVer = "RingServer/" + Version;
    public const string SlCapabilitiesId = "SLPROTO:4.0 SLPROTO:3.1 CAP WS:13";
    public const string SlServerId = "SeedLink v4.0 (" + SlServerVer + ") :: " + SlCapabilitiesId;
    public const string SlCapabilitiesV4 = "SLPROTO:4.0 SLPROTO:3.1 TIME WS:13 SEQWILDCARD";
    public const int SlHeadSizeV3 = 8;
    public const int SlHeadSizeV4 = 17;
    public const int SlInfoRecSize = 512;
    public const int SlMaxRegexLen = 2097152;
    public const int SlMaxSelectLen = 2048;

    // DataLink constants
    public const string DlServerVer = "RingServer/" + Version;
    public const string DlCapabilitiesId = "DLPROTO:1.1";
    public const string DlServerId = "DataLink v1.1 (" + DlServerVer + ") :: " + DlCapabilitiesId;
    public const int DlMaxRegexLen = 1048576;

    // Archive layout definitions
    public const string BudLayout = "%n/%s/%s.%n.%l.%c.%Y.%j";
    public const string CssLayout = "%Y/%j/%s.%c.%Y:%j:#H:#M:#S";
    public const string ChanLayout = "%n.%s.%l.%c";
    public const string QChanLayout = "%n.%s.%l.%c.%q";
    public const string CDayLayout = "%n.%s.%l.%c.%Y:%j:#H:#M:#S";
    public const string SDayLayout = "%n.%s.%Y:%j";
    public const string HSDayLayout = "%h/%n.%s.%Y:%j";

    // Maximum filename length for output files
    public const int MaxFilenameLen = 400;

    // Maximum filename length for scanned files
    public const int MsScanMaxFilename = 512;

    // Time conversion
    public const long NsModulus = 1_000_000_000L;
    public const long NsUnset = long.MinValue;
    public const long NsError = long.MinValue + 1;

    // Default configuration values
    public const ulong DefaultRingSize = 1024UL * 1024 * 1024; // 1 GiB
    public const uint DefaultMaxClients = 600;
    public const uint DefaultClientTimeout = 3600;
    public const float DefaultTimeWinLimit = 1.0f;
    public const int DefaultMseedIdleTo = 300;

    // Reserve connections for write-permitted addresses
    public const int ReserveConnections = 10;
}
