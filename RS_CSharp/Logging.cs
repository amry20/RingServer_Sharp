/**************************************************************************
 * Logging.cs
 *
 * Generic logging routines.
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

using RingServer.Types;

namespace RingServer;

/// <summary>
/// Logging parameters for transfer logs (TLog).
/// Equivalent to TLogParams_s in logging.h.
/// </summary>
public class TLogParams
{
    public string? TLogBaseDir;
    public int TxLog = 1;
    public int RxLog = 1;
    public int TLogInterval = 86400;
    public long TLogStart;
    public long TLogStartInt;
    public long TLogEndInt;
    public string? TLogPrefix;
}

/// <summary>
/// Generic logging routines with verbosity control.
/// Equivalent to lprintf/lprint in logging.c.
/// </summary>
public static class Logging
{
    /// <summary>
    /// Global verbosity level. Messages with level &lt;= Verbose are printed.
    /// </summary>
    public static int Verbose { get; set; } = 1;

    /// <summary>
    /// Global transfer log parameters.
    /// </summary>
    public static TLogParams TLogParams { get; set; } = new();

    /// <summary>
    /// Lock for transfer log file writing.
    /// </summary>
    private static readonly object TLogFileLock = new();

    private static readonly string[] Days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    private static readonly string[] Months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

    /// <summary>
    /// A generic log message handler, prepends a current date/time string
    /// to each message. Adds a newline to the output.
    /// Returns the number of characters in the formatted message.
    /// Equivalent to lprintf() in logging.c.
    /// </summary>
    public static int lprintf(int level, string fmt, params object?[] args)
    {
        if (level > Verbose)
            return 0;

        var now = DateTime.Now;
        string message;

        if (args.Length > 0)
            message = string.Format(fmt, args);
        else
            message = fmt;

        int rv = message.Length;

        Console.WriteLine("{0} {1} {2,2} {3,2}:{4,2}:{5,2} {6,4} - {7}{8}",
            Days[(int)now.DayOfWeek],
            Months[now.Month - 1],
            now.Day,
            now.Hour, now.Minute, now.Second,
            now.Year,
            message,
            (rv > 200) ? " ..." : "");

        return rv;
    }

    /// <summary>
    /// A simple gateway to lprintf(), with no level prefix check.
    /// Equivalent to lprint() in logging.c.
    /// </summary>
    public static void lprint(string message)
    {
        message = message.TrimEnd('\n');
        lprintf(0, "{0}", message);
    }

    /// <summary>
    /// Write transfer packet and byte counts to files.
    /// Equivalent to WriteTLog() in logging.c.
    /// </summary>
    public static int WriteTLog(ClientInfo cinfo, bool reset)
    {
        var tlp = TLogParams;
        if (string.IsNullOrEmpty(tlp.TLogBaseDir))
            return 0;
        if (tlp.TxLog == 0 && tlp.RxLog == 0)
            return 0;

        ulong txtotalbytes = 0;
        ulong rxtotalbytes = 0;

        string modestr = cinfo.Type switch
        {
            ClientType.DataLink => "DataLink",
            ClientType.SeedLink => "SeedLink",
            _ => "Unknown"
        };

        var startDt = DateTimeOffset.FromUnixTimeSeconds(tlp.TLogStartInt).LocalDateTime;
        var endDt = DateTimeOffset.FromUnixTimeSeconds(tlp.TLogEndInt).LocalDateTime;
        var now = DateTime.Now;
        var connDt = new DateTime(1970, 1, 1).AddTicks(cinfo.ConnTime.Value * 100);

        string prefix = tlp.TLogPrefix ?? "";
        string prefixSep = !string.IsNullOrEmpty(prefix) ? "-" : "";

        string txfilename = Path.Combine(tlp.TLogBaseDir,
            $"{prefix}{prefixSep}txlog-{startDt:yyyyMMddTHHmm}-{endDt:yyyyMMddTHHmm}");
        string rxfilename = Path.Combine(tlp.TLogBaseDir,
            $"{prefix}{prefixSep}rxlog-{startDt:yyyyMMddTHHmm}-{endDt:yyyyMMddTHHmm}");

        int rv = 0;

        lock (TLogFileLock)
        {
            try
            {
                if (tlp.TxLog != 0)
                {
                    using var txfp = new StreamWriter(txfilename, append: true);
                    txfp.WriteLine("START CLIENT {0} [{1}] ({2}|{3}) @ {4:O} (connected {5:O}) TX",
                        cinfo.Hostname, cinfo.IpStr, modestr, cinfo.ClientId, now, connDt);

                    lock (cinfo.StreamsLock)
                    {
                        if (cinfo.Streams != null)
                        {
                            var stack = new Types.Stack<object>();
                            cinfo.Streams.BuildStack(stack);

                            while (stack.NotEmpty)
                            {
                                var node = (RBNode)stack.Pop()!;
                                var sn = (StreamNode)node.Data!;
                                if (sn.TxBytes > 0)
                                {
                                    txfp.WriteLine("{0} {1} {2}", sn.StreamId, sn.TxBytes, sn.TxPackets);
                                    txtotalbytes += sn.TxBytes;
                                    if (reset)
                                    {
                                        sn.TxBytes = 0;
                                        sn.TxPackets = 0;
                                    }
                                }
                            }
                        }
                    }

                    txfp.WriteLine("TOTAL {0}", txtotalbytes);
                    txfp.WriteLine("END CLIENT");
                }
            }
            catch (Exception ex)
            {
                lprintf(0, "Error writing TX transfer log: {0}", ex.Message);
                rv = -1;
            }

            try
            {
                if (tlp.RxLog != 0)
                {
                    using var rxfp = new StreamWriter(rxfilename, append: true);
                    rxfp.WriteLine("START CLIENT {0} [{1}] ({2}|{3}) @ {4:O} (connected {5:O}) RX",
                        cinfo.Hostname, cinfo.IpStr, modestr, cinfo.ClientId, now, connDt);

                    lock (cinfo.StreamsLock)
                    {
                        if (cinfo.Streams != null)
                        {
                            var stack = new Types.Stack<object>();
                            cinfo.Streams.BuildStack(stack);

                            while (stack.NotEmpty)
                            {
                                var node = (RBNode)stack.Pop()!;
                                var sn = (StreamNode)node.Data!;
                                if (sn.RxBytes > 0)
                                {
                                    rxfp.WriteLine("{0} {1} {2}", sn.StreamId, sn.RxBytes, sn.RxPackets);
                                    rxtotalbytes += sn.RxBytes;
                                    if (reset)
                                    {
                                        sn.RxBytes = 0;
                                        sn.RxPackets = 0;
                                    }
                                }
                            }
                        }
                    }

                    rxfp.WriteLine("TOTAL {0}", rxtotalbytes);
                    rxfp.WriteLine("END CLIENT");
                }
            }
            catch (Exception ex)
            {
                lprintf(0, "Error writing RX transfer log: {0}", ex.Message);
                rv = -1;
            }
        }

        return rv;
    }
}
