/**************************************************************************
 * ClientHandler.cs
 *
 * Client thread handling all communications with a client.
 * Equivalent to ClientThread() in clients.c.
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

using System.Net.Sockets;
using System.Text;
using RingServer.Config;
using RingServer.Protocols;
using RingServer.Ring;
using RingServer.Types;

using ThreadState = RingServer.Types.ThreadState;

namespace RingServer.Net;

/// <summary>
/// Client thread implementation.
/// Equivalent to ClientThread() in clients.c.
/// </summary>
public static class ClientHandler
{
    // Progressive throttle stepping and maximum in milliseconds
    private const int ThrottleStepping = 5;
    private const int ThrottleMaximum = 10;

    /// <summary>
    /// Main client thread routine.
    /// Equivalent to ClientThread() in clients.c.
    /// </summary>
    public static void Run(ThreadData tdp)
    {
        var config = ServerConfig.Instance;
        var cinfo = (ClientInfo)tdp.PrivatePtr!;

        // Top-level try/catch to prevent silent thread death
        try
        {
            RunInner(tdp, config, cinfo);
        }
        catch (Exception ex)
        {
            Logging.lprintf(0, "[{0}] FATAL: Client thread crashed: {1} ({2})",
                cinfo.IpStr, ex.Message, ex.GetType().Name);
            Logging.lprintf(0, "[{0}] Stack trace: {1}", cinfo.IpStr, ex.StackTrace);

            // Cleanup on crash
            try { CloseSocket(cinfo); } catch { }
            cinfo.SendBuffer = null;
            cinfo.RecvBuffer = null;
            cinfo.Reader = null;

            lock (tdp.Lock)
            {
                tdp.State = ThreadState.Closed;
            }
        }
    }

    private static void RunInner(ThreadData tdp, ServerConfig config, ClientInfo cinfo)
    {
        Logging.lprintf(1, "[{0}:{1}] ClientHandler.Run thread started (type={2}, proto={3})",
            cinfo.IpStr, cinfo.PortStr, cinfo.Type, cinfo.Protocols);

        var reader = new RingReader
        {
            PktOffset = -1,
            PktId = Constants.RingIdNone,
            PktTime = NsTime.Unset,
            DataStart = NsTime.Unset,
            DataEnd = NsTime.Unset
        };

        cinfo.Reader = reader;

        // Set initial state
        cinfo.State = ClientState.Command;

        // Resolve IP address to hostname
        if (config.ResolveHosts)
        {
            try
            {
                var entry = System.Net.Dns.GetHostEntry(cinfo.IpStr);
                cinfo.Hostname = entry.HostName;
            }
            catch
            {
                cinfo.Hostname = cinfo.IpStr;
            }
        }
        else
        {
            cinfo.Hostname = cinfo.IpStr;
        }

        Logging.lprintf(1, "Client connected [{0}]: {1} [{2}] port {3}",
            cinfo.ServerPort, cinfo.Hostname, cinfo.IpStr, cinfo.PortStr);

        // Initialize stream tracking binary tree
        lock (cinfo.StreamsLock)
        {
            cinfo.Streams = new RBTree(Generic.StringKeyCompare);
            cinfo.StreamsCount = 0;
        }

        // Allocate client send buffer
        cinfo.SendBufferSize = 2 * (int)cinfo.RingParams.PktSize;
        cinfo.SendBuffer = new byte[cinfo.SendBufferSize];

        // Allocate client receive buffer
        cinfo.RecvBufferSize = 10 * (int)cinfo.RingParams.PktSize;
        cinfo.RecvBuffer = new byte[cinfo.RecvBufferSize];

        // Set socket to non-blocking
        try
        {
            if (cinfo.Socket != null)
                cinfo.Socket.Blocking = false;
        }
        catch (Exception ex)
        {
            Logging.lprintf(0, "[{0}] Error setting non-blocking flag: {1}", cinfo.Hostname, ex.Message);
            CleanupAndExit(cinfo, tdp);
            return;
        }

        // Limit sources if specified
        if (!string.IsNullOrEmpty(cinfo.LimitStr))
        {
            if (!RingBuffer.UpdatePattern(ref reader.Limit, cinfo.LimitStr, "limit"))
            {
                CleanupAndExit(cinfo, tdp);
                return;
            }
        }

        // Set thread active
        lock (tdp.Lock)
        {
            if (tdp.State == ThreadState.Spawning)
                tdp.State = ThreadState.Active;
        }

        var ringBuffer = new RingBuffer(cinfo.RingParams);

        // Main client loop
        uint throttleMsec = 0;

        int loopCount = 0;

        Logging.lprintf(2, "[{0}] Starting main client loop", cinfo.Hostname);

        while (tdp.State != ThreadState.Close)
        {
            loopCount++;
            Logging.lprintf(4, "[{0}] Loop #{1} throttle={2}ms state={3} type={4} recvLen={5}",
                cinfo.Hostname, loopCount, throttleMsec, cinfo.State, cinfo.Type, cinfo.RecvLength);
            // Increment throttle if not at maximum
            if (throttleMsec < ThrottleMaximum)
                throttleMsec += ThrottleStepping;

            // Determine client type from first bytes if not TLS
            if (cinfo.Type == ClientType.Undetermined)
            {
                // Single-protocol port: set type directly without peeking
                bool protoDataLink = (cinfo.Protocols & ListenProtocols.DataLink) != 0;
                bool protoSeedLink = (cinfo.Protocols & ListenProtocols.SeedLink) != 0;
                bool protoHttp = (cinfo.Protocols & ListenProtocols.Http) != 0;

                if (protoDataLink && !protoSeedLink && !protoHttp)
                {
                    cinfo.Type = ClientType.DataLink;
                }
                else if (protoSeedLink && !protoDataLink && !protoHttp)
                {
                    cinfo.Type = ClientType.SeedLink;
                }
                else if (protoHttp && !protoDataLink && !protoSeedLink)
                {
                    cinfo.Type = ClientType.Http;
                }
                else
                {
                    // Multi-protocol port: peek first bytes to distinguish
                    int nrecv = PeekData(cinfo);
                    if (nrecv >= 2)
                    {
                        string peekStr = Encoding.ASCII.GetString(cinfo.RecvBuffer, 0, nrecv);

                        // Specific DataLink command prefixes
                        bool dlCmd = (peekStr[0] == 'D' && peekStr[1] == 'L') ||
                                     (peekStr[0] == 'I' && peekStr[1] == 'D') ||
                                     (peekStr[0] == 'C' && peekStr[1] == 'A') ||
                                     (peekStr[0] == 'S' && peekStr[1] == 'T') ||
                                     (peekStr[0] == 'E' && peekStr[1] == 'N') ||
                                     (peekStr[0] == 'B' && peekStr[1] == 'Y');

                        if (protoDataLink && dlCmd)
                        {
                            cinfo.Type = ClientType.DataLink;
                        }
                        // HTTP requests start with known method (need 3+ bytes)
                        else if (nrecv >= 3 && protoHttp &&
                                 IsHttpMethod(peekStr[..3]))
                        {
                            cinfo.Type = ClientType.Http;
                        }
                        // SeedLink starts with HELLO or other uppercase text
                        else if (protoSeedLink)
                        {
                            cinfo.Type = ClientType.SeedLink;
                        }
                        else if (protoDataLink)
                        {
                            cinfo.Type = ClientType.DataLink;
                        }
                        else
                        {
                            Logging.lprintf(0, "[{0}] Cannot determine allowed client protocol from '{1}'",
                                cinfo.Hostname, Sanitize(peekStr));
                            break;
                        }
                    }
                    else if (nrecv < 0 && nrecv != -99)
                    {
                        Logging.lprintf(0, "[{0}] BREAK: protocol detection nrecv={1}", cinfo.Hostname, nrecv);
                        break;
                    }
                }
            }

            // Receive data from client
            int nread = ClientRecv(cinfo);

            if (nread < 0)
            {
                Logging.lprintf(0, "[{0}] BREAK: ClientRecv returned {1} (socketErr={2})",
                    cinfo.Hostname, nread, cinfo.SocketErr);
                break;
            }

            // Data received from client — a complete line/command is available
            if (nread > 0)
            {
                throttleMsec = 0;
                cinfo.LastXchange = Generic.NSnow();

                // Handle data according to client type
                if (cinfo.Type == ClientType.DataLink)
                {
                    // For DataLink, RecvDLCommand already consumed the bytes from RecvBuffer
                    // and stored the command in cinfo.DlCommand.  No need to recalculate consumed.
                    int cmdResult = DataLinkProtocol.HandleCommand(cinfo, nread);
                    Logging.lprintf(4, "[{0}] DataLink HandleCommand returned {1}, nread={2}",
                        cinfo.Hostname, cmdResult, nread);
                    if (cmdResult != 0)
                    {
                        Logging.lprintf(0, "[{0}] BREAK: DataLink HandleCommand returned {1}", cinfo.Hostname, cmdResult);
                        break;
                    }
                }
                else if (cinfo.Type == ClientType.Http)
                {
                    // For text-based protocols (SeedLink, HTTP), calculate consumed bytes
                    int consumed = nread;
                    if (nread < cinfo.RecvLength)
                    {
                        if (cinfo.RecvBuffer![nread] == '\n')
                            consumed = nread + 1;
                        else if (cinfo.RecvBuffer![nread] == '\r')
                        {
                            consumed = nread + 1;
                            if (nread + 1 < cinfo.RecvLength && cinfo.RecvBuffer[nread + 1] == '\n')
                                consumed = nread + 2;
                        }
                    }

                    // NULL-terminate at the first terminator for command parsing
                    if (nread < cinfo.RecvLength)
                        cinfo.RecvBuffer![nread] = 0;

                    if (HttpProtocol.HandleRequest(cinfo) != 0)
                    {
                        Logging.lprintf(0, "[{0}] BREAK: HTTP HandleRequest failed", cinfo.Hostname);
                        break;
                    }

                    // Consume processed bytes from the receive buffer
                    if (consumed > 0)
                    {
                        if (consumed < cinfo.RecvLength)
                        {
                            int remaining = cinfo.RecvLength - consumed;
                            Buffer.BlockCopy(cinfo.RecvBuffer!, consumed, cinfo.RecvBuffer!, 0, remaining);
                            cinfo.RecvLength = remaining;
                        }
                        else
                        {
                            cinfo.RecvLength = 0;
                        }
                        cinfo.RecvConsumed = 0;
                    }
                }
                else
                {
                    // SeedLink: text-based, calculate consumed bytes (same as HTTP)
                    int consumed = nread;
                    if (nread < cinfo.RecvLength)
                    {
                        if (cinfo.RecvBuffer![nread] == '\n')
                            consumed = nread + 1;
                        else if (cinfo.RecvBuffer![nread] == '\r')
                        {
                            consumed = nread + 1;
                            if (nread + 1 < cinfo.RecvLength && cinfo.RecvBuffer[nread + 1] == '\n')
                                consumed = nread + 2;
                        }
                    }

                    // NULL-terminate at the first terminator for command parsing
                    if (nread < cinfo.RecvLength)
                        cinfo.RecvBuffer![nread] = 0;

                    int cmdResult = SeedLinkProtocol.HandleCommand(cinfo, nread);
                    Logging.lprintf(4, "[{0}] SeedLink HandleCommand returned {1}, nread={2}",
                        cinfo.Hostname, cmdResult, nread);
                    if (cmdResult != 0)
                    {
                        Logging.lprintf(0, "[{0}] BREAK: SeedLink HandleCommand returned {1}", cinfo.Hostname, cmdResult);
                        break;
                    }

                    // Consume processed bytes from the receive buffer
                    if (consumed > 0)
                    {
                        if (consumed < cinfo.RecvLength)
                        {
                            int remaining = cinfo.RecvLength - consumed;
                            Buffer.BlockCopy(cinfo.RecvBuffer!, consumed, cinfo.RecvBuffer!, 0, remaining);
                            cinfo.RecvLength = remaining;
                        }
                        else
                        {
                            cinfo.RecvLength = 0;
                        }
                        cinfo.RecvConsumed = 0;
                    }
                }
            }

            // Regular outbound data flow
            if (cinfo.State == ClientState.Stream)
            {
                int sentBytes;

                if (cinfo.Type == ClientType.DataLink)
                {
                    sentBytes = DataLinkProtocol.StreamPackets(cinfo, ringBuffer);
                }
                else if (cinfo.Type == ClientType.SeedLink)
                {
                    sentBytes = SeedLinkProtocol.StreamPackets(cinfo, ringBuffer);
                }
                else
                {
                    sentBytes = 0;
                }

                if (sentBytes < 0)
                {
                    Logging.lprintf(0, "[{0}] BREAK: StreamPackets error sentBytes={1}", cinfo.Hostname, sentBytes);
                    break;
                }
                if (sentBytes == 0)
                    throttleMsec = ThrottleMaximum;
                else
                    throttleMsec = 0;
            }

            // Throttle loop and idle check
            if (throttleMsec > 0)
            {
                // Check for non-communicating clients
                if (throttleMsec >= ThrottleMaximum &&
                    cinfo.LastXchange == cinfo.ConnTime &&
                    (Generic.NSnow() - cinfo.ConnTime) > new NsTime(Constants.NsModulus * 10))
                {
                    Logging.lprintf(0, "[{0}] Non-communicating client timeout", cinfo.Hostname);
                    break;
                }

                // Throttle (poll or sleep)
                if (cinfo.Type != ClientType.Undetermined && cinfo.Socket != null)
                {
                    SendData.PollSocket(cinfo.Socket, 1, 0, (int)throttleMsec);
                }
                else
                {
                    System.Threading.Thread.Sleep((int)throttleMsec);
                }
            }
        }

        // Cleanup
        ClientCleanup(cinfo, tdp);
    } /* End of RunInner() */

    /// <summary>
    /// Peek at data without consuming it.
    /// Returns number of bytes available, 0 if none, negative on error.
    /// </summary>
    private static int PeekData(ClientInfo cinfo)
    {
        try
        {
            if (cinfo.Socket?.Poll(100000, SelectMode.SelectRead) == true)
            {
                // Peek directly into RecvBuffer (like C's recv(socket, recvbuf, 3, MSG_PEEK)).
                // Do NOT set RecvLength — the peeked bytes stay on the socket and will be
                // re-read (consumed) by the subsequent RecvLine/ClientRecv call.
                int nrecv = cinfo.Socket.Receive(cinfo.RecvBuffer!, 0, 3, SocketFlags.Peek);
                return nrecv;
            }
            return 0;
        }
        catch (SocketException ex)
        {
            // WSAEWOULDBLOCK (10035) is normal for non-blocking sockets with no data
            if (ex.SocketErrorCode == SocketError.WouldBlock)
                return 0;
            return -1;
        }
        catch
        {
            return -1;
        }
    }

    /// <summary>
    /// Check if the first 3 bytes look like an HTTP method.
    /// </summary>
    private static bool IsHttpMethod(string s)
    {
        if (s.Length < 3) return false;
        return (s[0] == 'G' && s[1] == 'E' && s[2] == 'T') ||
               (s[0] == 'H' && s[1] == 'E' && s[2] == 'A') ||
               (s[0] == 'P' && s[1] == 'O' && s[2] == 'S') ||
               (s[0] == 'P' && s[1] == 'U' && s[2] == 'T') ||
               (s[0] == 'D' && s[1] == 'E' && s[2] == 'L') ||
               (s[0] == 'T' && s[1] == 'R' && s[2] == 'A') ||
               (s[0] == 'C' && s[1] == 'O' && s[2] == 'N');
    }

    /// <summary>
    /// Receive data from client according to type.
    /// Equivalent to ClientRecv() in clients.c.
    /// </summary>
    private static int ClientRecv(ClientInfo cinfo)
    {
        if (cinfo == null) return -1;

        if (cinfo.Type == ClientType.DataLink)
            return RecvDLCommand(cinfo);
        else if (cinfo.Type == ClientType.SeedLink)
            return RecvLine(cinfo);
        else if (cinfo.Type == ClientType.Http)
            return RecvLine(cinfo);

        return 0;
    }

    /// <summary>
    /// Receive a DataLink command (3-byte pre-header + body).
    /// Equivalent to RecvDLCommand() in clients.c.
    ///
    /// DataLink binary framing: 'D' 'L' &lt;uint8 length&gt; &lt;length bytes of body&gt;
    ///
    /// Returns >0 as total bytes consumed on success (3 + body length).
    /// Returns 0 when no data is available.
    /// Returns -1 on error or timeout.
    /// Returns -2 on orderly shutdown (FIN).
    /// </summary>
    private static int RecvDLCommand(ClientInfo cinfo)
    {
        if (cinfo.Socket == null) return -1;

        try
        {
            int totalRead = 0;

            // Receive 3-byte pre-header: 'D', 'L', <length>
            int space = cinfo.RecvBufferSize - cinfo.RecvLength;
            if (space <= 0) return 0;

            int n = cinfo.Socket.Receive(cinfo.RecvBuffer!, cinfo.RecvLength, space, SocketFlags.None);

            if (n == 0)
                return -2; // Orderly shutdown (FIN)
            if (n < 0)
                return 0; // WouldBlock or similar

            cinfo.RecvLength += n;
            totalRead += n;

            // Need at least 3 bytes for pre-header
            if (cinfo.RecvLength < 3)
                return 0; // Need more data

            // Verify DataLink sequence bytes 'D' 'L'
            if (cinfo.RecvBuffer![0] != 'D' || cinfo.RecvBuffer[1] != 'L')
            {
                Logging.lprintf(2, "[{0}] Error verifying DataLink sequence bytes ({1}{2})",
                    cinfo.Hostname,
                    (cinfo.RecvBuffer[0] < 32 || cinfo.RecvBuffer[0] > 126) ? '?' : (char)cinfo.RecvBuffer[0],
                    (cinfo.RecvBuffer[1] < 32 || cinfo.RecvBuffer[1] > 126) ? '?' : (char)cinfo.RecvBuffer[1]);
                cinfo.SocketErr = -1;
                return -1;
            }

            // Read header body length (1 byte unsigned)
            byte headerLen = cinfo.RecvBuffer[2];

            // Sanity check: header must be at least 2 bytes (minimum command like "ID")
            if (headerLen < 2)
            {
                Logging.lprintf(0, "[{0}] Pre-header indicates header size too small: {1}",
                    cinfo.Hostname, headerLen);
                cinfo.SocketErr = -1;
                return -1;
            }

            // Read command body into DlCommand buffer
            int bodyNeed = headerLen;
            int bodyHave = cinfo.RecvLength - 3;

            while (bodyHave < bodyNeed)
            {
                space = cinfo.RecvBufferSize - cinfo.RecvLength;
                if (space <= 0) break;

                n = cinfo.Socket.Receive(cinfo.RecvBuffer!, cinfo.RecvLength, space, SocketFlags.None);

                if (n == 0)
                {
                    cinfo.SocketErr = -2;
                    return -2; // Orderly shutdown
                }
                if (n < 0)
                    break; // WouldBlock

                cinfo.RecvLength += n;
                totalRead += n;
                bodyHave = cinfo.RecvLength - 3;
            }

            if (bodyHave < bodyNeed)
                return 0; // Incomplete body, wait for more data

            // Copy the command body to DlCommand and null-terminate
            Buffer.BlockCopy(cinfo.RecvBuffer!, 3, cinfo.DlCommand, 0, Math.Min(headerLen, cinfo.DlCommand.Length - 1));
            cinfo.DlCommand[headerLen] = 0; // Null-terminate

            // Shift remaining data in RecvBuffer past the consumed bytes
            int consumed = 3 + headerLen; // pre-header + body
            if (consumed < cinfo.RecvLength)
            {
                int remaining = cinfo.RecvLength - consumed;
                Buffer.BlockCopy(cinfo.RecvBuffer!, consumed, cinfo.RecvBuffer!, 0, remaining);
                cinfo.RecvLength = remaining;
            }
            else
            {
                cinfo.RecvLength = 0;
            }

            return totalRead;
        }
        catch (SocketException ex)
        {
            if (ex.SocketErrorCode == SocketError.WouldBlock)
                return 0;
            cinfo.SocketErr = -1;
            return -1;
        }
        catch
        {
            cinfo.SocketErr = -1;
            return -1;
        }
    }

    /// <summary>
    /// Receive a line of text from the client.
    /// Equivalent to RecvLine() in clients.c.
    ///
    /// Returns >0 as number of bytes in line (up to first \r or \n) on success.
    /// Returns  0 when no complete line is available yet (data stays in buffer).
    /// Returns -1 on error or orderly shutdown.
    /// </summary>
    private static int RecvLine(ClientInfo cinfo)
    {
        if (cinfo.Socket == null) { cinfo.SocketErr = -1; return -1; }

        try
        {
            // Try to receive more data if buffer doesn't have a complete line yet
            if (cinfo.RecvBuffer != null && (cinfo.RecvLength == 0 || !HasTerminator(cinfo.RecvBuffer, cinfo.RecvLength)))
            {
                int space = cinfo.RecvBufferSize - cinfo.RecvLength;
                if (space > 0)
                {
                    int n = cinfo.Socket.Receive(cinfo.RecvBuffer!, cinfo.RecvLength, space, SocketFlags.None);
                    if (n == 0)
                        { cinfo.SocketErr = -2; return -2; }
                    if (n > 0)
                        cinfo.RecvLength += n;
                }
            }

            // Skip leading terminators (SeedLink v4 requires ignoring empty commands)
            // Equivalent to the "skipped" loop in C RecvLine
            int skipped = 0;
            while (skipped < cinfo.RecvLength)
            {
                if (cinfo.RecvBuffer![skipped] == '\n' || cinfo.RecvBuffer[skipped] == '\r')
                    skipped++;
                else
                    break;

                if (skipped >= 10)
                {
                    Logging.lprintf(0, "[{0}] Received too many empty lines", cinfo.Hostname);
                    cinfo.SocketErr = -1;
                    return -1;
                }
            }

            // Shift past skipped terminators
            if (skipped > 0)
            {
                int remaining = cinfo.RecvLength - skipped;
                if (remaining > 0)
                    Buffer.BlockCopy(cinfo.RecvBuffer!, skipped, cinfo.RecvBuffer!, 0, remaining);
                cinfo.RecvLength = remaining;
            }

            // If no data left after skipping, try to receive more
            if (cinfo.RecvLength == 0)
            {
                int space = cinfo.RecvBufferSize;
                int n = cinfo.Socket.Receive(cinfo.RecvBuffer!, 0, space, SocketFlags.None);
                if (n == 0)
                    { cinfo.SocketErr = -2; return -2; }
                if (n > 0)
                    cinfo.RecvLength = n;
                else
                    return 0; // WouldBlock - no data available
            }

            // Search for line terminators (\r or \n) in received data
            for (int i = 0; i < cinfo.RecvLength; i++)
            {
                if (cinfo.RecvBuffer![i] == '\n' || cinfo.RecvBuffer![i] == '\r')
                {
                    // Return number of bytes before the first terminator
                    return i;
                }
            }

            // No complete line yet, data stays in buffer for next call
            return 0;
        }
        catch (SocketException ex)
        {
            // WSAEWOULDBLOCK (10035) - no data available on non-blocking socket
            if (ex.SocketErrorCode == SocketError.WouldBlock)
                return 0;
            cinfo.SocketErr = -1;
            return -1;
        }
        catch
        {
            cinfo.SocketErr = -1;
            return -1;
        }
    }

    /// <summary>
    /// Check if buffer contains a line terminator (\r or \n).
    /// </summary>
    private static bool HasTerminator(byte[] buffer, int length)
    {
        for (int i = 0; i < length; i++)
        {
            if (buffer[i] == '\n' || buffer[i] == '\r')
                return true;
        }
        return false;
    }

    /// <summary>
    /// Clean up and exit on setup error.
    /// </summary>
    private static void CleanupAndExit(ClientInfo cinfo, ThreadData tdp)
    {
        lock (tdp.Lock)
        {
            tdp.State = ThreadState.Closing;
        }

        CloseSocket(cinfo);
        cinfo.Reader = null;

        lock (cinfo.StreamsLock)
        {
            cinfo.Streams?.Destroy();
            cinfo.Streams = null;
            cinfo.StreamsCount = 0;
        }

        cinfo.SendBuffer = null;
        cinfo.RecvBuffer = null;

        Logging.lprintf(1, "Client setup error, disconnected: {0}", cinfo.Hostname);

        lock (tdp.Lock)
        {
            tdp.State = ThreadState.Closed;
        }
    }

    /// <summary>
    /// Clean up client resources.
    /// Equivalent to the cleanup block in ClientThread() in clients.c.
    /// </summary>
    private static void ClientCleanup(ClientInfo cinfo, ThreadData tdp)
    {
        lock (ServerConfig.Params.CthreadsLock)
        {
            tdp.State = ThreadState.Closing;
        }

        CloseSocket(cinfo);

        // Write transfer log
        if (!string.IsNullOrEmpty(Logging.TLogParams.TLogBaseDir))
        {
            Logging.lprintf(2, "[{0}] Writing transmission log", cinfo.Hostname);
            Logging.WriteTLog(cinfo, true);
        }

        // Release regex patterns
        cinfo.Reader = null;

        // Release stream tracking tree
        lock (cinfo.StreamsLock)
        {
            cinfo.Streams?.Destroy();
            cinfo.Streams = null;
            cinfo.StreamsCount = 0;
        }

        cinfo.SendBuffer = null;
        cinfo.RecvBuffer = null;

        // Shutdown miniSEED write data stream
        if (cinfo.MsWrite != null)
        {
            // ds_streamproc equivalent
            cinfo.MsWrite = null;
        }

        // Free protocol-specific extended info
        if (cinfo.Type == ClientType.SeedLink && cinfo.ExtInfo != null)
        {
            SeedLinkProtocol.Free(cinfo);
        }
        if (cinfo.Type == ClientType.DataLink && cinfo.ExtInfo != null)
        {
            DataLinkProtocol.Free(cinfo);
        }

        Logging.lprintf(1, "Client disconnected: {0}", cinfo.Hostname);

        lock (tdp.Lock)
        {
            tdp.State = ThreadState.Closed;
        }
    }

    /// <summary>
    /// Close the client socket.
    /// </summary>
    private static void CloseSocket(ClientInfo cinfo)
    {
        try
        {
            if (cinfo.Socket != null)
            {
                cinfo.Socket.Shutdown(SocketShutdown.Both);
                cinfo.Socket.Close();
                cinfo.Socket = null;
            }
        }
        catch
        {
            // Ignore socket close errors
        }
    }

    /// <summary>
    /// Sanitize a string for logging.
    /// </summary>
    private static string Sanitize(string s)
    {
        var sb = new StringBuilder();
        foreach (char c in s)
        {
            if (c < 32 || c > 126)
                sb.Append('?');
            else
                sb.Append(c);
        }
        return sb.ToString();
    }
}
