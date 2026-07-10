/**************************************************************************
 * ListenServer.cs
 *
 * Listen thread and client dispatching.
 * Equivalent to ListenThread() in ringserver.c.
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
using RingServer.Config;
using RingServer.Types;

using ThreadState = RingServer.Types.ThreadState;

namespace RingServer.Net;

/// <summary>
/// Server listen thread for accepting connections and dispatching client threads.
/// Equivalent to ListenThread() in ringserver.c.
/// </summary>
public static class ListenServer
{
    /// <summary>
    /// Run the listen loop for a specific port configuration.
    /// Equivalent to ListenThread() in ringserver.c.
    /// </summary>
    public static void Run(ListenPortParams lpp, ThreadData tdp)
    {
        var config = ServerConfig.Instance;
        var @params = ServerConfig.Params;

        // Set thread active
        lock (tdp.Lock)
        {
            tdp.State = ThreadState.Active;
        }

        string protocolStr = SendData.GenProtocolString(lpp.Protocols, lpp.Options);
        Logging.lprintf(1, "Listening for connections on port {0} ({1})",
            lpp.PortStr, protocolStr);

        // Connection dispatch loop
        while (@params.ShutdownSig == 0)
        {
            Socket? clientSocket = null;

            try
            {
                clientSocket = lpp.ListenSocket!.Accept();
            }
            catch (SocketException ex)
            {
                if (ex.SocketErrorCode == SocketError.ConnectionAborted ||
                    ex.SocketErrorCode == SocketError.Interrupted)
                {
                    continue;
                }

                if (@params.ShutdownSig == 0)
                    Logging.lprintf(0, "Could not accept incoming connection: {0}", ex.Message);

                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }

            if (clientSocket == null)
                continue;

            try
            {
                // Disable Nagle algorithm
                try
                {
                    clientSocket.NoDelay = true;
                }
                catch (Exception ex)
                {
                    Logging.lprintf(0, "Could not disable TCP delay algorithm: {0}", ex.Message);
                }

                // Get remote endpoint info
                var remoteEp = (IPEndPoint?)clientSocket.RemoteEndPoint;
                if (remoteEp == null)
                {
                    clientSocket.Close();
                    continue;
                }

                string ipStr = remoteEp.Address.ToString();
                string portStr = remoteEp.Port.ToString();

                Logging.lprintf(2, "Incoming connection on port {0} from {1}:{2}",
                    lpp.PortStr, ipStr, portStr);

                // Check match list
                if (config.MatchIps != null && !MatchIP(config.MatchIps, remoteEp.Address))
                {
                    Logging.lprintf(1, "Rejecting non-matching connection from: {0}:{1}", ipStr, portStr);
                    clientSocket.Close();
                    continue;
                }

                // Check reject list
                if (config.RejectIps != null && MatchIP(config.RejectIps, remoteEp.Address))
                {
                    Logging.lprintf(1, "Rejecting connection from: {0}:{1}", ipStr, portStr);
                    clientSocket.Close();
                    continue;
                }

                // Enforce per-address connection limit
                if (config.MaxClientsPerIP > 0)
                {
                    bool isWriteIP = config.WriteIps != null && MatchIP(config.WriteIps, remoteEp.Address);
                    if (!isWriteIP)
                    {
                        int ipCount = ClientIPCount(remoteEp.Address);
                        if (ipCount >= config.MaxClientsPerIP)
                        {
                            Logging.lprintf(1, "Too many connections from: {0}:{1}", ipStr, portStr);
                            clientSocket.Close();
                            continue;
                        }
                    }
                }

                // Enforce maximum number of clients
                if (config.MaxClients > 0 && @params.ClientCount >= config.MaxClients)
                {
                    bool isWriteIP = config.WriteIps != null && MatchIP(config.WriteIps, remoteEp.Address);
                    if (isWriteIP && @params.ClientCount <= (config.MaxClients + Constants.ReserveConnections))
                    {
                        Logging.lprintf(1, "Allowing connection in reserve space from {0}:{1}", ipStr, portStr);
                    }
                    else
                    {
                        Logging.lprintf(1, "Maximum number of clients exceeded: {0}", config.MaxClients);
                        Logging.lprintf(1, "  Rejecting connection from: {0}:{1}", ipStr, portStr);
                        clientSocket.Close();
                        continue;
                    }
                }

                // Allocate and initialize client info
                var cinfo = new ClientInfo
                {
                    Socket = clientSocket,
                    Protocols = lpp.Protocols,
                    Tls = (lpp.Options & ListenOptions.Tls) != 0,
                    Type = ClientType.Undetermined,
                    IpStr = ipStr,
                    PortStr = portStr,
                    ServerPort = lpp.PortStr,
                    ClientId = "Client",
                    TimeWinLimit = config.TimeWinLimit,
                    HttpHeaders = config.HttpHeaders,
                    ConnTime = Generic.NSnow(),
                    LastXchange = Generic.NSnow(),
                    RingParams = ServerConfig.RingParams!
                };

                // Set stream limit if specified for address
                if (config.LimitIps != null)
                {
                    var matched = FindMatchingIP(config.LimitIps, remoteEp.Address);
                    if (matched != null)
                        cinfo.LimitStr = matched.LimitStr;
                }

                // Grant write permission if address is in write list
                if (config.WriteIps != null && MatchIP(config.WriteIps, remoteEp.Address))
                    cinfo.WritePerm = true;

                // Set trusted flag if address is in trusted list
                if (config.TrustedIps != null && MatchIP(config.TrustedIps, remoteEp.Address))
                    cinfo.Trusted = true;

                // Initialize miniSEED write parameters
                if (!string.IsNullOrEmpty(config.MSeedArchive))
                {
                    cinfo.MsWrite = new DataStream
                    {
                        Path = config.MSeedArchive,
                        IdleTimeout = config.MSeedIdleTo,
                        MaxOpenFiles = 50,
                        OpenFileCount = 0,
                        GroupRoot = null
                    };
                }

                // Start a new client thread
                var clientTdp = new ThreadData
                {
                    State = ThreadState.Spawning,
                    PrivatePtr = cinfo
                };

                var ctThread = new System.Threading.Thread(() => ClientHandler.Run(clientTdp), 131072)
                {
                    IsBackground = true,
                    Name = $"Client-{ipStr}:{portStr}"
                };
                ctThread.Start();

                // Add to client threads list
                var ctp = new CThread { Td = clientTdp };

                lock (@params.CthreadsLock)
                {
                    ctp.Next = @params.Cthreads;
                    if (@params.Cthreads != null)
                        @params.Cthreads.Prev = ctp;
                    @params.Cthreads = ctp;
                }

                Interlocked.Increment(ref @params.ClientCount);
            }
            catch (Exception ex)
            {
                Logging.lprintf(0, "Error dispatching client socket connection: {0}", ex.Message);
                try { clientSocket.Close(); } catch { }
            }
        }

        // Set thread closing status
        lock (tdp.Lock)
        {
            tdp.State = ThreadState.Closing;
        }
    }

    /// <summary>
    /// Match an IP address against an IPNet list.
    /// </summary>
    public static bool MatchIP(IPNet? list, IPAddress addr)
    {
        return FindMatchingIP(list, addr) != null;
    }

    /// <summary>
    /// Find matching IPNet entry for an IP address.
    /// </summary>
    public static IPNet? FindMatchingIP(IPNet? list, IPAddress addr)
    {
        byte[] addrBytes = addr.GetAddressBytes();

        var current = list;
        while (current != null)
        {
            if (current.Family == addr.AddressFamily && current.Network != null && current.Netmask != null)
            {
                if (addrBytes.Length == current.Network.Length)
                {
                    bool match = true;
                    for (int i = 0; i < addrBytes.Length; i++)
                    {
                        if ((addrBytes[i] & current.Netmask[i]) != (current.Network[i] & current.Netmask[i]))
                        {
                            match = false;
                            break;
                        }
                    }
                    if (match)
                        return current;
                }
            }
            current = current.Next;
        }

        return null;
    }

    /// <summary>
    /// Count number of clients from a specific IP address.
    /// </summary>
    private static int ClientIPCount(IPAddress addr)
    {
        var @params = ServerConfig.Params;
        int count = 0;

        lock (@params.CthreadsLock)
        {
            var current = @params.Cthreads;
            while (current != null)
            {
                if (current.Td?.PrivatePtr is ClientInfo cinfo)
                {
                    if (cinfo.IpStr == addr.ToString())
                        count++;
                }
                current = current.Next;
            }
        }

        return count;
    }
}
