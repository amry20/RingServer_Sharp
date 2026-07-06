/**************************************************************************
 * SendData.cs
 *
 * Data sending routines with WebSocket support.
 * Equivalent to SendData/SendDataMB in clients.c.
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
using RingServer.Types;

namespace RingServer.Net;

/// <summary>
/// Data sending utilities for client connections.
/// </summary>
public static class SendData
{
    /// <summary>
    /// Send a single buffer to a client.
    /// Returns 0 on success, -1 on error, -2 on orderly shutdown.
    /// Equivalent to SendData() in clients.c.
    /// </summary>
    public static int SendToClient(ClientInfo cinfo, byte[] buffer, int buflen, bool noWsFrame)
    {
        return SendBuffersToClient(cinfo, [buffer], [buflen], 1, noWsFrame);
    }

    /// <summary>
    /// Send multiple buffers to a client in order.
    /// Returns 0 on success, -1 on error, -2 on orderly shutdown.
    /// Equivalent to SendDataMB() in clients.c.
    /// </summary>
    public static int SendBuffersToClient(ClientInfo cinfo, byte[][] buffers, int[] lengths, int bufcount, bool noWsFrame)
    {
        if (cinfo == null || cinfo.Socket == null)
            return -1;

        if (bufcount <= 0)
            return 0;

        // Calculate total buffer length
        int totalbuflen = 0;
        for (int i = 0; i < bufcount; i++)
            totalbuflen += lengths[i];

        // Build complete data block
        byte[] data = new byte[totalbuflen];
        int offset = 0;
        for (int i = 0; i < bufcount; i++)
        {
            Buffer.BlockCopy(buffers[i], 0, data, offset, lengths[i]);
            offset += lengths[i];
        }

        try
        {
            // Set socket to blocking for send
            cinfo.Socket.Blocking = true;

            // Send all data
            int totalSent = 0;
            while (totalSent < data.Length)
            {
                int sent = cinfo.Socket.Send(data, totalSent, data.Length - totalSent, SocketFlags.None);
                if (sent <= 0)
                {
                    cinfo.SocketErr = -1;
                    return -1;
                }
                totalSent += sent;
            }
        }
        catch (SocketException ex)
        {
            if (ex.SocketErrorCode == SocketError.ConnectionReset ||
                ex.SocketErrorCode == SocketError.ConnectionAborted)
            {
                cinfo.SocketErr = -2;
                return -2;
            }
            cinfo.SocketErr = -1;
            return -1;
        }
        catch (ObjectDisposedException)
        {
            cinfo.SocketErr = -2;
            return -2;
        }
        finally
        {
            // Restore non-blocking mode after send
            try { cinfo.Socket.Blocking = false; } catch { }
        }

        // Update TX tracking
        cinfo.TxPackets[0]++;
        cinfo.TxBytes[0] += (ulong)totalbuflen;

        // Update stream tracking if we have stream info
        if (cinfo.Streams != null && !string.IsNullOrEmpty(cinfo.StreamId))
        {
            lock (cinfo.StreamsLock)
            {
                var node = cinfo.Streams.Find(cinfo.StreamId);
                if (node != null)
                {
                    var sn = (StreamNode)node.Data!;
                    sn.TxPackets++;
                    sn.TxBytes += (ulong)totalbuflen;
                }
            }
        }

        return 0;
    }

    /// <summary>
    /// Poll a socket for data availability.
    /// Equivalent to PollSocket() in the C code.
    /// Returns 1 if data available, 0 if timeout, -1 on error.
    /// </summary>
    public static int PollSocket(Socket socket, int checkRead, int checkWrite, int timeoutMs)
    {
        try
        {
            var mode = SelectMode.SelectRead;

            if (checkWrite != 0)
                mode = SelectMode.SelectWrite;

            return socket.Poll(timeoutMs * 1000, mode) ? 1 : 0;
        }
        catch
        {
            return -1;
        }
    }

    /// <summary>
    /// Generate a string of protocols and options for a listener.
    /// Equivalent to GenProtocolString() in the C code.
    /// </summary>
    public static string GenProtocolString(ListenProtocols protocols, ListenOptions options)
    {
        var parts = new List<string>();

        if ((protocols & ListenProtocols.DataLink) != 0)
            parts.Add("DataLink");
        if ((protocols & ListenProtocols.SeedLink) != 0)
            parts.Add("SeedLink");
        if ((protocols & ListenProtocols.Http) != 0)
            parts.Add("HTTP");

        if ((options & ListenOptions.Tls) != 0)
            parts.Add("TLS");
        if ((options & ListenOptions.IPv4) != 0)
            parts.Add("IPv4");
        if ((options & ListenOptions.IPv6) != 0)
            parts.Add("IPv6");

        return string.Join(", ", parts);
    }
}
