/**************************************************************************
 * ServerConfig.cs
 *
 * Configuration parameters and command line processing.
 * Equivalent to config.c in the C version.
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

using System.Net;
using System.Net.Sockets;
using RingServer.Ring;
using RingServer.Ring;
using RingServer.Types;

// Disambiguate ThreadState from System.Threading.ThreadState
using ThreadState = RingServer.Types.ThreadState;

namespace RingServer.Config;

/// <summary>
/// Global server parameters.
/// Equivalent to param_s in ringserver.h.
/// </summary>
public class ServerParams
{
    public NsTime ServerStartTime = NsTime.Unset;
    public int ClientCount;
    public int ShutdownSig;
    public DateTime ConfigFileMtime;
    public readonly object SthreadsLock = new();
    public SThread? Sthreads;
    public readonly object CthreadsLock = new();
    public CThread? Cthreads;
}

/// <summary>
/// Server thread structure (doubly-linked list).
/// Equivalent to sthread in ringserver.h.
/// </summary>
public class SThread
{
    public ThreadData? Td;
    public ServerThreadType Type;
    public object? Params;
    public SThread? Prev;
    public SThread? Next;
}

/// <summary>
/// Client thread structure (doubly-linked list).
/// Equivalent to cthread in ringserver.h.
/// </summary>
public class CThread
{
    public ThreadData? Td;
    public CThread? Prev;
    public CThread? Next;
}

/// <summary>
/// Thread data associated with most threads.
/// Equivalent to thread_data in ringserver.h.
/// </summary>
public class ThreadData
{
    public readonly object Lock = new();
    public ThreadState State = ThreadState.Spawning;
    public int Done;
    public object? PrivatePtr;
}

/// <summary>
/// Singly-linked list of string values.
/// Equivalent to strnode in ringserver.h.
/// </summary>
public class StrNode
{
    public string? String;
    public StrNode? Next;
}

/// <summary>
/// Server-wide configuration parameters.
/// Equivalent to config_s in ringserver.h.
/// </summary>
public class ServerConfig
{
    // Static singleton
    public static ServerConfig Instance { get; } = new();
    public static ServerParams Params { get; } = new();
    public static RingParams? RingParams;            // Global ring parameters
    public static Mseed.MseedArchive? Archive;       // Global PostgreSQL archive instance

    public string? ConfigFile;
    public string? ServerId;
    public string? RingDir;
    public ulong RingSize = Constants.DefaultRingSize;
    public uint PktSize;
    public uint MaxClients = Constants.DefaultMaxClients;
    public uint MaxClientsPerIP;
    public uint ClientTimeout = Constants.DefaultClientTimeout;
    public float TimeWinLimit = Constants.DefaultTimeWinLimit;
    public bool ResolveHosts = true;
    public bool MemoryMapRing = true;
    public bool VolatileRing;
    public byte AutoRecovery = 1;
    public string? WebRoot;
    public string? HttpHeaders;
    public string? MSeedArchive;
    public int MSeedIdleTo = Constants.DefaultMseedIdleTo;
    public string? PostgresConnStr;              // PostgreSQL connection string for archive
    public int PostgresRetentionDays;            // Auto-delete partitions older than N days (0 = keep forever)
    public StorageMode StorageMode = StorageMode.Auto;  // Storage mode: File, Sql, or Auto
    public IPNet? LimitIps;
    public IPNet? MatchIps;
    public IPNet? RejectIps;
    public IPNet? WriteIps;
    public IPNet? TrustedIps;
    public string? TlsCertFile;
    public string? TlsKeyFile;
    public bool TlsVerifyClientCert;

    /// <summary>
    /// Initialize default packet size: sizeof(RingPacket) + 512 bytes data.
    /// </summary>
    public void InitDefaultPktSize()
    {
        PktSize = (uint)(RingPacket.SerializedSize + 512);
    }
}

/// <summary>
/// Configuration options processing.
/// </summary>
public static class ConfigProcessor
{
    /// <summary>
    /// Process command line parameters.
    /// Equivalent to ProcessParam() in config.c.
    /// Returns 0 on success, -1 on failure.
    /// </summary>
    public static int ProcessParam(string[] args)
    {
        var config = ServerConfig.Instance;

        if (config.PktSize == 0)
            config.InitDefaultPktSize();

        for (int i = 0; i < args.Length; i++)
        {
            string arg = args[i];

            switch (arg)
            {
                case "-V":
                    Console.WriteLine($"{Constants.Package} version: {Constants.Version}");
                    Environment.Exit(0);
                    break;

                case "-h":
                    Usage(0);
                    break;

                case "-H":
                    Usage(1);
                    break;

                case "-C":
                    PrintConfigReference();
                    Environment.Exit(0);
                    break;

                case var v when v.StartsWith("-v"):
                    int vcount = 1;
                    for (int j = 1; j < v.Length && v[j] == 'v'; j++)
                        vcount++;
                    Logging.Verbose += vcount;
                    Logging.lprintf(2, "Verbosity set to {0}", Logging.Verbose);
                    break;

                case "-I":
                    config.ServerId = GetOptVal(args, ref i);
                    break;

                case "-m":
                    if (uint.TryParse(GetOptVal(args, ref i), out uint maxcl))
                        config.MaxClients = maxcl;
                    break;

                case "-M":
                    if (uint.TryParse(GetOptVal(args, ref i), out uint maxperip))
                        config.MaxClientsPerIP = maxperip;
                    break;

                case "-Rd":
                    config.RingDir = GetOptVal(args, ref i);
                    break;

                case "-Rs":
                    config.RingSize = CalcSize(GetOptVal(args, ref i));
                    break;

                case "-Rp":
                    uint datasize = uint.Parse(GetOptVal(args, ref i));
                    config.PktSize = (uint)(RingPacket.SerializedSize + datasize);
                    break;

                case "-NOMM":
                    config.MemoryMapRing = false;
                    break;

                case "-L":
                    ParseListenPort(GetOptVal(args, ref i), ListenProtocols.All);
                    break;

                case "-DL":
                    ParseListenPort(GetOptVal(args, ref i), ListenProtocols.DataLink);
                    break;

                case "-SL":
                    ParseListenPort(GetOptVal(args, ref i), ListenProtocols.SeedLink);
                    break;

                case "-HL":
                    ParseListenPort(GetOptVal(args, ref i), ListenProtocols.Http);
                    break;

                case "-T":
                    Logging.TLogParams.TLogBaseDir = GetOptVal(args, ref i);
                    break;

                case "-Ti":
                    if (int.TryParse(GetOptVal(args, ref i), out int interval))
                        Logging.TLogParams.TLogInterval = interval * 3600;
                    break;

                case "-Tp":
                    Logging.TLogParams.TLogPrefix = GetOptVal(args, ref i);
                    break;

                case "-STDERR":
                    // In .NET, Console.SetError already handles this
                    Console.SetOut(Console.Error);
                    break;

                case "-MSWRITE":
                    config.MSeedArchive = GetOptVal(args, ref i);
                    break;

                case "-MSSCAN":
                    AddMSeedScanThread(GetOptVal(args, ref i));
                    break;

                case "-VOLATILE":
                    config.VolatileRing = true;
                    break;

                case "-NOAUTOREC":
                    config.AutoRecovery = 0;
                    break;

                case "-StorageMode":
                    {
                        string mode = GetOptVal(args, ref i).ToLowerInvariant();
                        config.StorageMode = mode switch
                        {
                            "file" => StorageMode.File,
                            "sql" => StorageMode.Sql,
                            "auto" => StorageMode.Auto,
                            _ => StorageMode.Auto
                        };
                        Logging.lprintf(1, "Storage mode set to: {0}", config.StorageMode);
                    }
                    break;

                case var _ when arg.StartsWith('-'):
                    Logging.lprintf(0, "Unknown option: {0}", arg);
                    Environment.Exit(1);
                    break;

                default:
                    // Config file
                    config.ConfigFile = arg;
                    break;
            }
        }

        // If a config file was specified, read it
        if (config.ConfigFile != null)
        {
            if (ReadConfigFile(config.ConfigFile, false) < 0)
                return -1;
        }

        // Add default localhost loopback if WriteIps is empty (null)
        if (config.WriteIps == null)
        {
            AddIPNet("WriteIP", "127.0.0.1/8", null);
            AddIPNet("WriteIP", "::1/128", null);
        }

        // Add default localhost loopback if TrustedIps is empty (null)
        if (config.TrustedIps == null)
        {
            AddIPNet("TrustedIP", "127.0.0.1/8", null);
            AddIPNet("TrustedIP", "::1/128", null);
        }

        return 0;
    }

    /// <summary>
    /// Read and process configuration file.
    /// Equivalent to ReadConfigFile() in config.c.
    /// </summary>
    public static int ReadConfigFile(string configfile, bool dynamiconly)
    {
        var config = ServerConfig.Instance;

        try
        {
            string[] lines = File.ReadAllLines(configfile);
            int linenum = 0;

            foreach (string rawline in lines)
            {
                linenum++;
                string line = rawline.Trim();

                // Skip empty lines and comments
                if (string.IsNullOrEmpty(line) || line.StartsWith('#'))
                    continue;

                if (SetParameter(line, dynamiconly) < 0)
                {
                    Logging.lprintf(0, "Error processing config line {0}: {1}", linenum, line);
                    return -1;
                }
            }

            var fi = new FileInfo(configfile);
            ServerConfig.Params.ConfigFileMtime = fi.LastWriteTime;
        }
        catch (Exception ex)
        {
            Logging.lprintf(0, "Error reading config file {0}: {1}", configfile, ex.Message);
            return -1;
        }

        return 0;
    }

    /// <summary>
    /// Set a single configuration parameter from a parameter string.
    /// Equivalent to SetParameter() in config.c.
    /// Returns 1 on success setting parameter, 0 if parameter ignored (not dynamic), -1 on error.
    /// </summary>
    public static int SetParameter(string paramstring, bool dynamiconly)
    {
        var config = ServerConfig.Instance;
        var parts = paramstring.Split(' ', 2, StringSplitOptions.TrimEntries);
        if (parts.Length < 1) return 0;

        string keyword = parts[0];
        string? value = parts.Length > 1 ? parts[1].Trim('"') : null;

        // Parse configuration keywords
        switch (keyword)
        {
            case "Verbosity":
                if (value != null && int.TryParse(value, out int v))
                    Logging.Verbose = v;
                return 1;

            case "ServerID":
                config.ServerId = value;
                return 1;

            case "MaxClients":
                if (value != null && uint.TryParse(value, out uint mc))
                    config.MaxClients = mc;
                return 1;

            case "MaxClientsPerIP":
                if (value != null && uint.TryParse(value, out uint mcip))
                    config.MaxClientsPerIP = mcip;
                return 1;

            case "ClientTimeout":
                if (value != null && uint.TryParse(value, out uint ct))
                    config.ClientTimeout = ct;
                return 1;

            case "RingDirectory":
                config.RingDir = value;
                return 1;

            case "RingSize":
                if (value != null)
                    config.RingSize = CalcSize(value);
                return 1;

            case "MaxPacketSize":
                if (value != null && uint.TryParse(value, out uint mps))
                    config.PktSize = (uint)(RingPacket.SerializedSize + mps);
                return 1;

            case "MemoryMapRing":
                if (value != null)
                    config.MemoryMapRing = YesNo(value);
                return 1;

            case "VolatileRing":
                if (value != null)
                    config.VolatileRing = YesNo(value);
                return 1;

            case "AutoRecovery":
                if (value != null)
                {
                    if (int.TryParse(value, out int ar))
                        config.AutoRecovery = (byte)ar;
                }
                return 1;

            case "ResolveHostnames":
            case "ResolveHosts":
                if (value != null)
                    config.ResolveHosts = YesNo(value);
                return 1;

            case "TimeWindowLimit":
                if (value != null && float.TryParse(value, out float twl))
                    config.TimeWinLimit = twl;
                return 1;

            case "ListenPort":
                if (value != null)
                    ParseListenPort(value, ListenProtocols.All);
                return 1;

            case "DataLinkPort":
                if (value != null)
                    ParseListenPort(value, ListenProtocols.DataLink);
                return 1;

            case "SeedLinkPort":
                if (value != null)
                    ParseListenPort(value, ListenProtocols.SeedLink);
                return 1;

            case "HTTPPort":
                if (value != null)
                    ParseListenPort(value, ListenProtocols.Http);
                return 1;

            case "WebRoot":
                config.WebRoot = value;
                return 1;

            case "HTTPHeader":
            case "HttpHeaders":
                config.HttpHeaders = value;
                return 1;

            case "TransferLogDirectory":
                Logging.TLogParams.TLogBaseDir = value;
                return 1;

            case "TransferLogInterval":
                if (value != null && int.TryParse(value, out int tli))
                    Logging.TLogParams.TLogInterval = tli * 3600;
                return 1;

            case "TransferLogPrefix":
                Logging.TLogParams.TLogPrefix = value;
                return 1;

            case "TransferLogTX":
                if (value != null && int.TryParse(value, out int tltx))
                    Logging.TLogParams.TxLog = tltx;
                return 1;

            case "TransferLogRX":
                if (value != null && int.TryParse(value, out int tlrx))
                    Logging.TLogParams.RxLog = tlrx;
                return 1;

            case "MSeedWrite":
                config.MSeedArchive = value;
                return 1;

            case "PostgresConnStr":
                config.PostgresConnStr = value;
                return 1;

            case "PostgresRetentionDays":
                if (value != null && int.TryParse(value, out int prd))
                    config.PostgresRetentionDays = prd;
                return 1;

            case "StorageMode":
                if (value != null)
                {
                    config.StorageMode = value.ToLowerInvariant() switch
                    {
                        "file" => StorageMode.File,
                        "sql" => StorageMode.Sql,
                        "auto" => StorageMode.Auto,
                        _ => StorageMode.Auto
                    };
                    Logging.lprintf(1, "Storage mode set to: {0}", config.StorageMode);
                }
                return 1;

            case "MSeedScan":
                if (value != null)
                    AddMSeedScanThread(value);
                return 1;

            case "MSeedIdleTimeout":
                if (value != null && int.TryParse(value, out int msto))
                    config.MSeedIdleTo = msto;
                return 1;

            case "LimitIP":
            case "MatchIP":
            case "RejectIP":
            case "WriteIP":
            case "TrustedIP":
                if (value != null)
                {
                    var ipParts = value.Split(' ', 2, StringSplitOptions.TrimEntries);
                    string network = ipParts[0];
                    string? limitstr = ipParts.Length > 1 ? ipParts[1] : null;
                    AddIPNet(keyword, network, limitstr);
                }
                return 1;

            case "TLSCertFile":
            case "TlsCertificate":
                config.TlsCertFile = value;
                return 1;

            case "TLSKeyFile":
            case "TlsKey":
                config.TlsKeyFile = value;
                return 1;

            case "TLSVerifyClientCert":
            case "TlsVerifyClient":
                if (value != null)
                    config.TlsVerifyClientCert = YesNo(value);
                return 1;

            default:
                if (dynamiconly)
                {
                    Logging.lprintf(0, "Ignoring non-dynamic parameter: {0}", keyword);
                    return 0;
                }
                Logging.lprintf(0, "Unknown configuration parameter: {0}", keyword);
                return -1;
        }
    }

    /// <summary>
    /// Parse a listen port specification.
    /// Format: [protocol:]port[:options]
    /// Examples: 18000, datalink:18000, seedlink:18000, tls:18000, dual:18000
    /// </summary>
    private static void ParseListenPort(string spec, ListenProtocols defaultProtocol)
    {
        var config = ServerConfig.Instance;
        var protocols = defaultProtocol;
        var options = ListenOptions.None;
        string portStr;

        var colonIdx = spec.IndexOf(':');
        if (colonIdx >= 0)
        {
            string prefix = spec[..colonIdx].ToLowerInvariant();
            portStr = spec[(colonIdx + 1)..];

            switch (prefix)
            {
                case "datalink":
                    protocols = ListenProtocols.DataLink;
                    break;
                case "seedlink":
                    protocols = ListenProtocols.SeedLink;
                    break;
                case "http":
                    protocols = ListenProtocols.Http;
                    break;
                case "tls":
                    options |= ListenOptions.Tls;
                    break;
                case "dual":
                    options |= ListenOptions.IPv4 | ListenOptions.IPv6;
                    break;
                case "ipv4":
                    options |= ListenOptions.IPv4;
                    break;
                case "ipv6":
                    options |= ListenOptions.IPv6;
                    break;
                default:
                    portStr = spec;
                    break;
            }
        }
        else
        {
            portStr = spec;
        }

        // Parse trailing options after port
        var portParts = portStr.Split(':');
        portStr = portParts[0];
        for (int i = 1; i < portParts.Length; i++)
        {
            switch (portParts[i].ToLowerInvariant())
            {
                case "tls": options |= ListenOptions.Tls; break;
                case "dual": options |= ListenOptions.IPv4 | ListenOptions.IPv6; break;
                case "ipv4": options |= ListenOptions.IPv4; break;
                case "ipv6": options |= ListenOptions.IPv6; break;
            }
        }

        var lpp = new ListenPortParams
        {
            PortStr = portStr,
            Protocols = protocols,
            Options = options
        };

        AddListenThreads(lpp);
    }

    /// <summary>
    /// Initialize a server socket and add listen threads.
    /// Equivalent to InitServerSocket() + AddListenThreads() in config.c.
    /// </summary>
    private static void AddListenThreads(ListenPortParams lpp)
    {
        var config = ServerConfig.Instance;

        // Determine address families
        var families = new List<AddressFamily>();
        bool reqIPv4 = (lpp.Options & ListenOptions.IPv4) != 0;
        bool reqIPv6 = (lpp.Options & ListenOptions.IPv6) != 0;

        if (reqIPv6 || (!reqIPv4 && !reqIPv6))
            families.Add(AddressFamily.InterNetworkV6);
        if (reqIPv4 || (!reqIPv4 && !reqIPv6))
            families.Add(AddressFamily.InterNetwork);

        int port = int.Parse(lpp.PortStr);

        foreach (var family in families)
        {
            var socket = new Socket(family, SocketType.Stream, ProtocolType.Tcp);
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);

            if (family == AddressFamily.InterNetworkV6)
            {
                socket.DualMode = false;
            }

            IPAddress ipAddr = family == AddressFamily.InterNetworkV6
                ? IPAddress.IPv6Any
                : IPAddress.Any;

            try
            {
                socket.Bind(new IPEndPoint(ipAddr, port));
                socket.Listen(128);

                var listenLpp = new ListenPortParams
                {
                    PortStr = lpp.PortStr,
                    Protocols = lpp.Protocols,
                    Options = lpp.Options,
                    ListenSocket = socket
                };

                // Add to server threads
                var st = new SThread
                {
                    Type = ServerThreadType.Listen,
                    Params = listenLpp
                };

                lock (ServerConfig.Params.SthreadsLock)
                {
                    st.Next = ServerConfig.Params.Sthreads;
                    if (ServerConfig.Params.Sthreads != null)
                        ServerConfig.Params.Sthreads.Prev = st;
                    ServerConfig.Params.Sthreads = st;
                }

                Logging.lprintf(1, "Listening on port {0} ({1})", lpp.PortStr, family);
            }
            catch (Exception ex)
            {
                Logging.lprintf(0, "Error binding to port {0}: {1}", lpp.PortStr, ex.Message);
                socket.Close();
            }
        }
    }

    /// <summary>
    /// Add an IP network to the appropriate filter list.
    /// Equivalent to AddIPNet() in config.c.
    /// </summary>
    private static int AddIPNet(string keyword, string network, string? limitstr)
    {
        var config = ServerConfig.Instance;

        var ipnet = new IPNet();

        // Parse network address/mask
        var parts = network.Split('/');
        string addrStr = parts[0];
        int prefixLen = parts.Length > 1 && int.TryParse(parts[1], out int p) ? p : -1;

        if (IPAddress.TryParse(addrStr, out var addr))
        {
            ipnet.Family = addr.AddressFamily;
            ipnet.Network = addr.GetAddressBytes();
            ipnet.LimitStr = limitstr;

            // Calculate netmask from prefix length
            int addrLen = ipnet.Network.Length;
            ipnet.Netmask = new byte[addrLen];
            if (prefixLen >= 0)
            {
                for (int i = 0; i < addrLen; i++)
                {
                    int bits = Math.Min(8, prefixLen - i * 8);
                    if (bits > 0)
                        ipnet.Netmask[i] = (byte)(0xFF << (8 - bits) & 0xFF);
                    else
                        ipnet.Netmask[i] = 0;
                }
            }
            else
            {
                // Default: same length netmask (single host)
                Array.Fill(ipnet.Netmask, (byte)0xFF);
            }
        }
        else
        {
            Logging.lprintf(0, "Cannot parse IP address: {0}", addrStr);
            return -1;
        }

        // Add to the appropriate list
        ref IPNet? listRef = ref GetIPListRef(config, keyword);

        ipnet.Next = listRef;
        listRef = ipnet;

        return 0;
    }

    private static ref IPNet? GetIPListRef(ServerConfig config, string keyword)
    {
        switch (keyword)
        {
            case "LimitIP": return ref config.LimitIps!;
            case "MatchIP": return ref config.MatchIps!;
            case "RejectIP": return ref config.RejectIps!;
            case "WriteIP": return ref config.WriteIps!;
            case "TrustedIP": return ref config.TrustedIps!;
            default: throw new ArgumentException("Unknown IP list type: " + keyword);
        }
    }

    private static int AddMSeedScanThread(string configstr)
    {
        var st = new SThread
        {
            Type = ServerThreadType.MSeedScan,
            Params = configstr
        };

        lock (ServerConfig.Params.SthreadsLock)
        {
            st.Next = ServerConfig.Params.Sthreads;
            if (ServerConfig.Params.Sthreads != null)
                ServerConfig.Params.Sthreads.Prev = st;
            ServerConfig.Params.Sthreads = st;
        }

        return 0;
    }

    /// <summary>
    /// Convert yes/no string to boolean.
    /// </summary>
    private static bool YesNo(string value)
    {
        return value == "1" || value.Equals("yes", StringComparison.OrdinalIgnoreCase) ||
               value.Equals("true", StringComparison.OrdinalIgnoreCase) ||
               value.Equals("on", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Calculate size from string with optional suffixes (K, M, G, T).
    /// Equivalent to CalcSize() in config.c.
    /// </summary>
    private static ulong CalcSize(string sizestr)
    {
        sizestr = sizestr.Trim();
        if (string.IsNullOrEmpty(sizestr)) return 0;

        char suffix = sizestr[^1];
        string numPart = sizestr;

        ulong multiplier = 1;
        switch (char.ToUpperInvariant(suffix))
        {
            case 'K': multiplier = 1024; numPart = sizestr[..^1]; break;
            case 'M': multiplier = 1024 * 1024; numPart = sizestr[..^1]; break;
            case 'G': multiplier = 1024 * 1024 * 1024; numPart = sizestr[..^1]; break;
            case 'T': multiplier = 1024UL * 1024 * 1024 * 1024; numPart = sizestr[..^1]; break;
        }

        if (ulong.TryParse(numPart, out ulong value))
            return value * multiplier;

        if (double.TryParse(numPart, out double dval))
            return (ulong)(dval * multiplier);

        return 0;
    }

    /// <summary>
    /// Get option value from argument list.
    /// </summary>
    private static string GetOptVal(string[] args, ref int idx)
    {
        idx++;
        if (idx < args.Length)
            return args[idx];
        return "";
    }

    /// <summary>
    /// Print usage message.
    /// </summary>
    internal static void Usage(int level)
    {
        var config = ServerConfig.Instance;
        uint datasize = config.PktSize > (uint)RingPacket.SerializedSize
            ? config.PktSize - (uint)RingPacket.SerializedSize : 0;

        Console.Error.WriteLine($"{Constants.Package} version {Constants.Version}");
        Console.Error.WriteLine();
        Console.Error.WriteLine("Usage: {0} [options] [configfile]", Constants.Package);
        Console.Error.WriteLine();
        Console.Error.WriteLine(" ## Options ##");
        Console.Error.WriteLine(" -V             Print program version and exit");
        Console.Error.WriteLine(" -h             Print this usage message");
        Console.Error.WriteLine(" -H             Print an extended usage message");
        Console.Error.WriteLine(" -C             Print configuration file options and descriptions");
        Console.Error.WriteLine(" -v             Be more verbose, multiple flags can be used");
        Console.Error.WriteLine(" -I serverID    Server ID (default 'Ring Server')");
        Console.Error.WriteLine(" -m maxclnt     Maximum number of concurrent clients (currently {0})", config.MaxClients);
        Console.Error.WriteLine(" -M maxperIP    Maximum number of concurrent clients per address (currently {0})", config.MaxClientsPerIP);
        Console.Error.WriteLine(" -Rd ringdir    Directory for ring buffer files, required");
        Console.Error.WriteLine(" -Rs bytes      Ring packet buffer file size in bytes (default 1 Gibibyte)");
        Console.Error.WriteLine(" -Rp pktsize    Maximum ring packet data size in bytes (currently {0})", datasize);
        Console.Error.WriteLine(" -NOMM          Do not memory map the packet buffer, use memory instead");
        Console.Error.WriteLine(" -L port        Listen for connections on port, all protocols (default off)");
        Console.Error.WriteLine(" -T logdir      Directory to write transfer logs (default is no logs)");
        Console.Error.WriteLine(" -Ti hours      Transfer log writing interval (default 24 hours)");
        Console.Error.WriteLine(" -Tp prefix     Prefix to add to transfer log files (default is none)");
        Console.Error.WriteLine(" -STDERR        Send all console output to stderr instead of stdout");
        Console.Error.WriteLine();

        if (level >= 1)
        {
            Console.Error.WriteLine(" -MSWRITE format  Write all received miniSEED to an archive");
            Console.Error.WriteLine(" -MSSCAN dir      Scan directory for files containing miniSEED");
            Console.Error.WriteLine(" -VOLATILE        Create volatile ring, contents not saved to files");
            Console.Error.WriteLine();
        }

        Environment.Exit(1);
    }

    /// <summary>
    /// Print configuration file reference.
    /// </summary>
    internal static void PrintConfigReference()
    {
        Console.WriteLine("# ringserver configuration file reference");
        Console.WriteLine("# See the ringserver documentation for details.");
    }
}
