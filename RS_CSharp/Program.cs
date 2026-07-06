/**************************************************************************
 * Program.cs
 *
 * Main entry point and watchdog (supervisory) loop for ringserver.
 * Equivalent to main() in ringserver.c.
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

using System.Runtime.InteropServices;
using RingServer.Config;
using RingServer.Net;
using RingServer.Ring;
using RingServer.Types;

using ThreadState = RingServer.Types.ThreadState;

namespace RingServer;

public static class Program
{
    /// <summary>
    /// Main entry point.
    /// Equivalent to main() in ringserver.c.
    /// </summary>
    public static int Main(string[] args)
    {
        var config = ServerConfig.Instance;
        var @params = ServerConfig.Params;

        string ringfilename = "";
        string streamfilename = "";
        string ringfileBackup = "";
        string streamfileBackup = "";

        // Ring descriptor (file descriptor equivalent)
        int ringfd = -1;
        RingParams? ringparams = null;

        // Process command line parameters
        if (config.PktSize == 0)
            config.InitDefaultPktSize();

        if (ConfigProcessor.ProcessParam(args) < 0)
            return 1;

        // Signal handling using .NET's Console.CancelKeyPress
        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            if (@params.ShutdownSig == 0)
            {
                Logging.lprintf(1, "Received termination signal (Ctrl+C)");
                @params.ShutdownSig = 1;
            }
        };

        // Handle SIGTERM via AppDomain.CurrentDomain.ProcessExit
        AppDomain.CurrentDomain.ProcessExit += (sender, e) =>
        {
            if (@params.ShutdownSig == 0)
            {
                Logging.lprintf(1, "Received termination signal (ProcessExit)");
                @params.ShutdownSig = 1;
            }
        };

        // Console.CancelKeyPress doesn't cover SIGTERM on Linux,
        // so also set up a handler via PosixSignal
        try
        {
            // PosixSignalRegistration is available on .NET 6+
            var sigtermReg = PosixSignalRegistration.Create(PosixSignal.SIGTERM, ctx =>
            {
                Logging.lprintf(1, "Received SIGTERM signal");
                @params.ShutdownSig = 1;
                ctx.Cancel = true;
            });

            var sigintReg = PosixSignalRegistration.Create(PosixSignal.SIGINT, ctx =>
            {
                Logging.lprintf(1, "Received SIGINT signal");
                if (@params.ShutdownSig == 0)
                    @params.ShutdownSig = 1;
                ctx.Cancel = true;
            });

            var sigusr1Reg = PosixSignalRegistration.Create((PosixSignal)10, ctx =>  // SIGUSR1 = 10
            {
                Logging.lprintf(1, "Received SIGUSR1 signal, printing server details");
                LogRingParameters(ringparams);
                LogServerParameters(config, @params);
                ctx.Cancel = true;
            });
        }
        catch (PlatformNotSupportedException)
        {
            // PosixSignalRegistration is only supported on Unix-like platforms
            Logging.lprintf(2, "Posix signal registration not supported on this platform");
        }
        catch (Exception ex)
        {
            Logging.lprintf(2, "Could not register Posix signals: {0}", ex.Message);
        }

        // Initialize ring system
        if (!string.IsNullOrEmpty(config.RingDir) || config.VolatileRing)
        {
            if (!config.VolatileRing)
            {
                // Create ring file path: "<ringdir>/packetbuf"
                ringfilename = Path.Combine(config.RingDir!, "packetbuf");
                // Create stream index file path: "<ringdir>/streamidx"
                streamfilename = Path.Combine(config.RingDir!, "streamidx");
            }

            // Initialize ring system
            int ringinit = RingBuffer.Initialize(ringfilename, streamfilename,
                config.RingSize, config.PktSize,
                config.MemoryMapRing, config.VolatileRing,
                out ringfd, out ringparams);

            if (ringinit != 0)
            {
                // Exit on unrecoverable errors or if no auto recovery
                if (ringinit == -2 || config.AutoRecovery == 0)
                {
                    Logging.lprintf(0, "Error initializing ring buffer ({0})", ringinit);
                    return 1;
                }

                // Auto-recovery: move corrupt files and re-initialize
                if (config.AutoRecovery == 1 && (ringinit == -1 || ringinit > 0))
                {
                    string suffix;

                    if (ringinit == -1)
                    {
                        suffix = ".corrupt";
                        Logging.lprintf(0, "Auto recovery, moving packet buffer and stream index files to .corrupt");
                    }
                    else
                    {
                        suffix = $".version{ringinit}";
                        Logging.lprintf(0, "Auto recovery, moving packet buffer and stream index files to .version{ringinit}");
                    }

                    ringfileBackup = ringfilename + suffix;
                    streamfileBackup = streamfilename + suffix;

                    // Rename original ring and stream files to backup names
                    try
                    {
                        if (File.Exists(ringfilename))
                        {
                            if (File.Exists(ringfileBackup))
                                File.Delete(ringfileBackup);
                            File.Move(ringfilename, ringfileBackup);
                        }
                    }
                    catch (Exception ex)
                    {
                        Logging.lprintf(0, "Error renaming {0} to {1}: {2}", ringfilename, ringfileBackup, ex.Message);
                        return 1;
                    }

                    try
                    {
                        if (File.Exists(streamfilename))
                        {
                            if (File.Exists(streamfileBackup))
                                File.Delete(streamfileBackup);
                            File.Move(streamfilename, streamfileBackup);
                        }
                    }
                    catch (Exception ex)
                    {
                        Logging.lprintf(0, "Error renaming {0} to {1}: {2}", streamfilename, streamfileBackup, ex.Message);
                        return 1;
                    }
                }
                // Remove existing packet buffer and index
                else if (config.AutoRecovery == 2)
                {
                    Logging.lprintf(0, "Auto recovery, removing existing packet buffer and stream index files");

                    try
                    {
                        if (File.Exists(ringfilename))
                            File.Delete(ringfilename);
                        if (File.Exists(streamfilename))
                            File.Delete(streamfilename);
                    }
                    catch (Exception ex)
                    {
                        Logging.lprintf(0, "Error removing ring files: {0}", ex.Message);
                        return 1;
                    }
                }
                else
                {
                    Logging.lprintf(0, "Unrecognized combination of auto recovery: {0}, and ringinit return {1}",
                        config.AutoRecovery, ringinit);
                    return 1;
                }

                // Re-initialize ring system
                ringinit = RingBuffer.Initialize(ringfilename, streamfilename,
                    config.RingSize, config.PktSize,
                    config.MemoryMapRing, config.VolatileRing,
                    out ringfd, out ringparams);

                if (ringinit != 0)
                {
                    Logging.lprintf(0, "Error re-initializing ring buffer on auto-recovery ({0})", ringinit);
                    return 1;
                }

                // Try to load packets from backup if this was a version conversion
                if (config.AutoRecovery == 1 && File.Exists(ringfileBackup) && ringinit == 0)
                {
                    Logging.lprintf(0, "Backup files remain after re-initialization: {0}", ringfileBackup);
                    // Note: LoadBufferV1 conversion is not yet implemented in C# port
                }
            }
        }
        else
        {
            Logging.lprintf(0, "Error: ring directory is not set and ring is not volatile");
            return 1;
        }

        if (ringparams == null)
        {
            Logging.lprintf(0, "Error: ring parameters not initialized");
            return 1;
        }

        // Store global ring parameters
        ServerConfig.RingParams = ringparams;

        // Set server start time
        @params.ServerStartTime = Generic.NSnow();

        // Initialize watchdog loop interval times
        var curtime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var chktime = curtime;

        // Initialize transfer log window timers
        if (Logging.TLogParams.TLogBaseDir != null)
        {
            Logging.TLogParams.TLogStart = curtime;
            TransferLog.CalcIntWin(curtime, Logging.TLogParams.TLogInterval,
                out Logging.TLogParams.TLogStartInt, out Logging.TLogParams.TLogEndInt);
        }

        // Log parameters
        RingBuffer.LogParameters(ringparams);
        LogServerParameters(config, @params);

        // Watchdog loop: monitors server and client threads
        // with restarts and cleanup when necessary
        var timereqMs = 250; // 1/4 second loop interval

        while (true)
        {
            // If shutdown is requested, signal all threads
            if (@params.ShutdownSig == 1)
            {
                @params.ShutdownSig = 2; // Move to shutting down state
                timereqMs = 100; // Faster loop during shutdown

                // Request shutdown of server threads (close listen sockets)
                lock (@params.SthreadsLock)
                {
                    var loopstp = @params.Sthreads;
                    while (loopstp != null)
                    {
                        if (loopstp.Type == ServerThreadType.Listen)
                        {
                            var lpp = (ListenPortParams)loopstp.Params!;
                            if (lpp.ListenSocket != null)
                            {
                                Logging.lprintf(3, "Closing port {0} server socket", lpp.PortStr);
                                try
                                {
                                    lpp.ListenSocket.Close();
                                }
                                catch (Exception ex)
                                {
                                    Logging.lprintf(2, "Error closing socket: {0}", ex.Message);
                                }
                                lpp.ListenSocket = null;
                            }
                        }

                        loopstp = loopstp.Next;
                    }
                }

                // Request shutdown of client threads
                lock (@params.CthreadsLock)
                {
                    var loopctp = @params.Cthreads;
                    while (loopctp != null)
                    {
                        if (loopctp.Td != null)
                        {
                            lock (loopctp.Td.Lock)
                            {
                                if (loopctp.Td.State != ThreadState.Closing &&
                                    loopctp.Td.State != ThreadState.Closed)
                                {
                                    loopctp.Td.State = ThreadState.Close;
                                }
                            }
                        }
                        loopctp = loopctp.Next;
                    }
                }
            }

            // Safety valve for deadlock during shutdown
            if (@params.ShutdownSig >= 100)
            {
                Logging.lprintf(0, "Shutdown did not complete cleanly after ~10 seconds");
                break;
            }

            if (@params.ShutdownSig > 1)
            {
                @params.ShutdownSig++;
            }

            // Transmission log writing time window check
            int tlogwrite = 0;
            if (Logging.TLogParams.TLogBaseDir != null && @params.ShutdownSig == 0)
            {
                if (curtime >= Logging.TLogParams.TLogEndInt)
                    tlogwrite = 1;
                else
                    tlogwrite = 0;
            }

            // Loop through server thread list to monitor, restart, and cleanup
            int servercount = 0;
            lock (@params.SthreadsLock)
            {
                var loopstp = @params.Sthreads;
                while (loopstp != null)
                {
                    string threadType;
                    System.Threading.ThreadStart? threadFunc = null;

                    if (loopstp.Type == ServerThreadType.Listen)
                    {
                        threadType = "Listen";
                        threadFunc = () =>
                        {
                            var lpp = (ListenPortParams)loopstp.Params!;
                            var tdp = loopstp.Td ?? new ThreadData { PrivatePtr = lpp };
                            loopstp.Td = tdp;
                            ListenServer.Run(lpp, tdp);
                            // Set thread completed
                            lock (tdp.Lock)
                            {
                                tdp.State = ThreadState.Closed;
                            }
                        };
                    }
                    else if (loopstp.Type == ServerThreadType.MSeedScan)
                    {
                        threadType = "MSeedScan";
                        // MSeedScan thread not yet implemented as a standalone thread
                        threadType = "MSeedScan (stub)";
                        threadFunc = null;
                    }
                    else
                    {
                        threadType = "Unknown";
                        threadFunc = null;
                    }

                    // Report status of server thread
                    if (loopstp.Td != null)
                    {
                        string state = loopstp.Td.State switch
                        {
                            ThreadState.Spawning => "SPAWNING",
                            ThreadState.Active => "ACTIVE",
                            ThreadState.Close => "CLOSE",
                            ThreadState.Closing => "CLOSING",
                            ThreadState.Closed => "CLOSED",
                            _ => "UNKNOWN"
                        };
                        Logging.lprintf(3, "Server thread ({0}) state: {1}", threadType, state);
                        servercount++;
                    }
                    else
                    {
                        Logging.lprintf(2, "Server thread ({0}) not running", threadType);
                    }

                    // Listen thread handling
                    if (loopstp.Type == ServerThreadType.Listen)
                    {
                        // Cleanup CLOSED listen thread
                        if (loopstp.Td != null && loopstp.Td.State == ThreadState.Closed)
                        {
                            Logging.lprintf(1, "Cleaning up CLOSED {0} thread", threadType);
                            loopstp.Td = null;
                        }

                        // Start new listening thread if needed
                        if (loopstp.Td == null && @params.ShutdownSig == 0)
                        {
                            var lpp = (ListenPortParams)loopstp.Params!;

                            if (lpp.ListenSocket != null)
                            {
                                var tdp = new ThreadData
                                {
                                    State = ThreadState.Spawning,
                                    PrivatePtr = lpp
                                };
                                loopstp.Td = tdp;

                                var targetSt = loopstp; // Capture local copy for closure
                                var targetLpp = lpp;
                                var targetTdp = tdp;
                                var listenThread = new System.Threading.Thread(() =>
                                {
                                    var listenTdp = new ThreadData
                                    {
                                        State = ThreadState.Spawning,
                                        PrivatePtr = targetLpp
                                    };
                                    targetSt.Td = listenTdp;
                                    ListenServer.Run(targetLpp, listenTdp);
                                    lock (listenTdp.Lock)
                                    {
                                        listenTdp.State = ThreadState.Closed;
                                    }
                                })
                                {
                                    IsBackground = true,
                                    Name = $"Listen-{lpp.PortStr}"
                                };

                                Logging.lprintf(2, "Starting Listen thread for port {0}", lpp.PortStr);
                                listenThread.Start();
                            }
                        }
                    }
                    // MSeedScan thread handling
                    else if (loopstp.Type == ServerThreadType.MSeedScan)
                    {
                        // Cleanup CLOSED scanning thread
                        if (loopstp.Td != null && loopstp.Td.State == ThreadState.Closed)
                        {
                            Logging.lprintf(1, "Cleaning up CLOSED {0} thread", threadType);
                            loopstp.Td = null;
                        }
                    }

                    loopstp = loopstp.Next;
                }
            }

            // Reset total count and byte rates
            double txpacketrate = 0, txbyterate = 0, rxpacketrate = 0, rxbyterate = 0;

            // Loop through client thread list printing status and doing cleanup
            lock (@params.CthreadsLock)
            {
                var loopctp = @params.Cthreads;
                while (loopctp != null)
                {
                    var ctp = loopctp;
                    loopctp = loopctp.Next;

                    if (ctp.Td == null)
                        continue;

                    string state = ctp.Td.State switch
                    {
                        ThreadState.Spawning => "SPAWNING",
                        ThreadState.Active => "ACTIVE",
                        ThreadState.Close => "CLOSE",
                        ThreadState.Closing => "CLOSING",
                        ThreadState.Closed => "CLOSED",
                        _ => "UNKNOWN"
                    };
                    Logging.lprintf(3, "Client thread state: {0}", state);

                    // Free associated resources and remove CLOSED client threads
                    if (ctp.Td.State == ThreadState.Closed)
                    {
                        Logging.lprintf(3, "Removing client thread from the cthreads list");

                        // Unlink from the cthreads list
                        if (ctp.Prev == null && ctp.Next == null)
                            @params.Cthreads = null;
                        else if (ctp.Prev == null && ctp.Next != null)
                            @params.Cthreads = ctp.Next;
                        if (ctp.Prev != null)
                            ctp.Prev.Next = ctp.Next;
                        if (ctp.Next != null)
                            ctp.Next.Prev = ctp.Prev;

                        // Decrement client count
                        if (@params.ClientCount > 0)
                            @params.ClientCount--;
                    }
                    else
                    {
                        // Update transmission and reception rates
                        if (ctp.Td.PrivatePtr is ClientInfo cinfo)
                        {
                            CalcStats(cinfo);
                            txpacketrate += cinfo.TxPacketRate;
                            txbyterate += cinfo.TxByteRate;
                            rxpacketrate += cinfo.RxPacketRate;
                            rxbyterate += cinfo.RxByteRate;

                            // Write transfer logs and reset byte counts
                            if (tlogwrite != 0)
                            {
                                Logging.WriteTLog(cinfo, true);
                            }

                            // Close idle clients if limit is set and exceeded
                            if (config.ClientTimeout > 0)
                            {
                                var now = Generic.NSnow();
                                var diff = now - cinfo.LastXchange;
                                if (diff.Value > config.ClientTimeout * Constants.NsModulus)
                                {
                                    lock (ctp.Td.Lock)
                                    {
                                        if (ctp.Td.State != ThreadState.Close &&
                                            ctp.Td.State != ThreadState.Closing &&
                                            ctp.Td.State != ThreadState.Closed)
                                        {
                                            Logging.lprintf(1, "Closing idle client connection: {0}", cinfo.Hostname);
                                            ctp.Td.State = ThreadState.Close;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Logging.lprintf(3, "Client connections: {0}", @params.ClientCount);

            // Update count and byte rate ring parameters
            if (ringparams != null)
            {
                ringparams.TxPacketRate = txpacketrate;
                ringparams.TxByteRate = txbyterate;
                ringparams.RxPacketRate = rxpacketrate;
                ringparams.RxByteRate = rxbyterate;
            }

            // Check for config file updates
            if (config.ConfigFile != null)
            {
                try
                {
                    var fi = new FileInfo(config.ConfigFile);
                    if (fi.Exists && fi.LastWriteTime > @params.ConfigFileMtime)
                    {
                        Logging.lprintf(1, "Re-reading configuration parameters from {0}", config.ConfigFile);
                        ConfigProcessor.ReadConfigFile(config.ConfigFile, true);
                        @params.ConfigFileMtime = fi.LastWriteTime;
                    }
                }
                catch (Exception ex)
                {
                    Logging.lprintf(0, "Error checking config file: {0}", ex.Message);
                }
            }

            // Reset transfer log writing time windows
            if (Logging.TLogParams.TLogBaseDir != null && @params.ShutdownSig == 0 && (tlogwrite != 0))
            {
                TransferLog.CalcIntWin(DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                    Logging.TLogParams.TLogInterval,
                    out Logging.TLogParams.TLogStartInt, out Logging.TLogParams.TLogEndInt);
            }

            // All done if shutting down and no threads left
            if (@params.ShutdownSig >= 2 && @params.ClientCount == 0 && servercount == 0)
                break;

            // Throttle the loop
            System.Threading.Thread.Sleep(timereqMs);

            curtime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            chktime = curtime;
        }

        // Shutdown ring buffer
        if ((!string.IsNullOrEmpty(config.RingDir) || config.VolatileRing) && ringparams != null)
        {
            if (RingBuffer.Shutdown(ringfd, streamfilename, ringparams) != 0)
            {
                Logging.lprintf(0, "Error shutting down ring buffer");
                return 1;
            }
        }

        Logging.lprintf(1, "RingServer shutdown complete");

        return 0;
    }

    /// <summary>
    /// Calculate statistics for the specified client connection.
    /// Equivalent to CalcStats() in ringserver.c.
    /// </summary>
    private static void CalcStats(ClientInfo cinfo)
    {
        if (cinfo == null) return;

        var nsnow = Generic.NSnow();
        double deltasec;

        // Determine time difference since the previous history values were set in seconds
        if (cinfo.RateTime.Value == 0)
            deltasec = 1.0;
        else
            deltasec = (double)(nsnow.Value - cinfo.RateTime.Value) / Constants.NsModulus;

        // Calculate percent lag if reader exists
        if (cinfo.Reader != null && cinfo.Reader.PktId <= Constants.RingIdMaximum)
        {
            // Note: Percent lag calculation requires ringparams access
            cinfo.PercentLag = 0;
        }
        else
        {
            cinfo.PercentLag = 0;
        }

        // Transmission
        if (cinfo.TxPackets[0] > 0)
        {
            cinfo.TxPacketRate = (cinfo.TxPackets[0] - cinfo.TxPackets[1]) / deltasec;
            cinfo.TxByteRate = (double)(cinfo.TxBytes[0] - cinfo.TxBytes[1]) / deltasec;
            cinfo.TxPackets[1] = cinfo.TxPackets[0];
            cinfo.TxBytes[1] = cinfo.TxBytes[0];
        }

        // Reception
        if (cinfo.RxPackets[0] > 0)
        {
            cinfo.RxPacketRate = (cinfo.RxPackets[0] - cinfo.RxPackets[1]) / deltasec;
            cinfo.RxByteRate = (double)(cinfo.RxBytes[0] - cinfo.RxBytes[1]) / deltasec;
            cinfo.RxPackets[1] = cinfo.RxPackets[0];
            cinfo.RxBytes[1] = cinfo.RxBytes[0];
        }

        // Update time stamp of history values
        cinfo.RateTime = new NsTime(nsnow.Value);
    }

    /// <summary>
    /// Log high-level server parameters.
    /// Equivalent to LogServerParameters() in ringserver.c.
    /// </summary>
    private static void LogServerParameters(ServerConfig config, ServerParams @params)
    {
        Logging.lprintf(1, "Server parameters:");
        Logging.lprintf(1, "   server ID: {0}", config.ServerId ?? "(none)");
        Logging.lprintf(1, "   ring directory: {0}", config.RingDir ?? "NONE");
        Logging.lprintf(1, "   max clients: {0}", config.MaxClients);
        Logging.lprintf(1, "   max clients per IP: {0}", config.MaxClientsPerIP);

        Logging.lprintf(2, "   configuration file: {0}", config.ConfigFile ?? "NONE");
        Logging.lprintf(2, "   client timeout: {0} seconds", config.ClientTimeout);
        Logging.lprintf(2, "   time window limit: {0}%", config.TimeWinLimit * 100);
        Logging.lprintf(2, "   resolve hostnames: {0}", config.ResolveHosts ? "yes" : "no");
        Logging.lprintf(2, "   auto recovery: {0}", config.AutoRecovery);
        Logging.lprintf(2, "   TLS certificate file: {0}", config.TlsCertFile ?? "NONE");
        Logging.lprintf(2, "   TLS key file: {0}", config.TlsKeyFile ?? "NONE");
        Logging.lprintf(2, "   TLS verify client certificate: {0}", config.TlsVerifyClientCert ? "yes" : "no");

        Logging.lprintf(3, "   web root: {0}", config.WebRoot ?? "NONE");
        Logging.lprintf(3, "   HTTP headers: {0}", config.HttpHeaders ?? "NONE");
        Logging.lprintf(3, "   miniSEED archive: {0}", config.MSeedArchive ?? "NONE");
        Logging.lprintf(3, "   miniSEED idle file timeout: {0} seconds", config.MSeedIdleTo);

        Logging.lprintf(3, "   transfer log: {0}", Logging.TLogParams.TLogBaseDir ?? "NONE");
        if (Logging.TLogParams.TLogBaseDir != null && Logging.Verbose >= 3)
        {
            Logging.lprintf(3, "     log prefix: {0}", Logging.TLogParams.TLogPrefix ?? "NONE");
            Logging.lprintf(3, "     log interval: {0} seconds", Logging.TLogParams.TLogInterval);
        }

        // Log IP lists
        LogIPList("limit IP range", config.LimitIps);
        LogIPList("match IP range", config.MatchIps);
        LogIPList("reject IP range", config.RejectIps);
        LogIPList("write IP range", config.WriteIps);
        LogIPList("trusted IP range", config.TrustedIps);
    }

    /// <summary>
    /// Log an IPNet list at verbosity level 3.
    /// </summary>
    private static void LogIPList(string label, IPNet? list)
    {
        if (list != null && Logging.Verbose >= 3)
        {
            var current = list;
            while (current != null)
            {
                string network = current.Network != null
                    ? string.Join(".", current.Network)
                    : "(null)";
                string netmask = current.Netmask != null
                    ? string.Join(".", current.Netmask)
                    : "(null)";
                Logging.lprintf(3, "   {0}: {1}/{2}", label, network, netmask);
                if (current.LimitStr != null)
                    Logging.lprintf(3, "     limit pattern: {0}", current.LimitStr);
                current = current.Next;
            }
        }
        else
        {
            Logging.lprintf(3, "   {0}: NONE", label);
        }
    }

    /// <summary>
    /// Log ring parameters - delegates to RingBuffer.LogParameters().
    /// </summary>
    private static void LogRingParameters(RingParams? ringparams)
    {
        if (ringparams != null)
            RingBuffer.LogParameters(ringparams);
    }
}

/// <summary>
/// Transfer log calculation utilities.
/// </summary>
internal static class TransferLog
{
    /// <summary>
    /// Calculate interval time window based on current time and interval length.
    /// Equivalent to CalcIntWin() in logging.c.
    /// </summary>
    public static int CalcIntWin(long curtime, int interval, out long startint, out long endint)
    {
        startint = 0;
        endint = 0;

        if (interval <= 0)
            return -1;

        // Calculate start of current interval
        startint = (curtime / interval) * interval;
        endint = startint + interval;

        return 0;
    }
}
