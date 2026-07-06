/**************************************************************************
 * Enums.cs
 *
 * Enumerations for ringserver.
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
 **************************************************************************/

namespace RingServer.Types;

/// <summary>
/// Thread states
/// </summary>
public enum ThreadState
{
    Spawning,   // Thread is now spawning
    Active,     // Thread is active
    Close,      // Thread close triggered
    Closing,    // Thread in close process
    Closed      // Thread is closed
}

/// <summary>
/// Server thread types
/// </summary>
public enum ServerThreadType
{
    Listen,         // Listen for incoming network connections
    MSeedScan       // Scan for miniSEED files
}

/// <summary>
/// Listen thread protocols (bit flags)
/// </summary>
[Flags]
public enum ListenProtocols : uint
{
    DataLink = 1u << 1,
    SeedLink = 1u << 2,
    Http     = 1u << 3,
    All      = DataLink | SeedLink | Http
}

/// <summary>
/// Listen thread options (bit flags)
/// </summary>
[Flags]
public enum ListenOptions : uint
{
    None          = 0,
    Tls           = 1u << 1,
    IPv4          = 1u << 2,
    IPv6          = 1u << 3
}

/// <summary>
/// Client types
/// </summary>
public enum ClientType
{
    Undetermined,
    DataLink,
    SeedLink,
    Http
}

/// <summary>
/// Client states
/// </summary>
public enum ClientState
{
    Command,     // Initial command state
    Station,     // SeedLink STATION negotiation
    RingConfig,  // SeedLink ring configuration
    Stream       // Data streaming
}

/// <summary>
/// SeedLink error codes (bit flags)
/// </summary>
[Flags]
public enum ErrorCode : uint
{
    None         = 0,
    Internal     = 1u << 1,
    Unsupported  = 1u << 2,
    Unexpected   = 1u << 3,
    Unauthorized = 1u << 4,
    Limit        = 1u << 5,
    Arguments    = 1u << 6,
    Auth         = 1u << 7
}

/// <summary>
/// SeedLink INFO request types
/// </summary>
public enum SlInfoType
{
    Id            = 1,
    Capabilities  = 2,
    Stations      = 3,
    Streams       = 4,
    Connections   = 5
}

/// <summary>
/// Info elements (bit flags), most are defined in SeedLink v4
/// </summary>
[Flags]
public enum InfoElements : uint
{
    Id             = 1u << 1,
    Capabilities   = 1u << 2,
    Formats        = 1u << 3,
    Filters        = 1u << 4,
    Stations       = 1u << 5,
    StationStreams = 1u << 6,
    Streams        = 1u << 7,
    Connections    = 1u << 8,
    Status         = 1u << 9
}

/// <summary>
/// Storage mode for ring buffer persistence.
/// </summary>
public enum StorageMode
{
    /// <summary>File-based ring buffer (default legacy mode)</summary>
    File,
    /// <summary>SQL database (PostgreSQL) as primary storage</summary>
    Sql,
    /// <summary>Auto-detect: use SQL if PostgresConnStr is configured, otherwise File</summary>
    Auto
}
