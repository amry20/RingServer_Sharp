/**************************************************************************
 * SeedLinkProtocol.cs
 *
 * SeedLink protocol handler.
 * Equivalent to slclient.c / slclient.h in the C version.
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

using System.Text;
using System.Collections.Generic;
using RingServer.Net;
using RingServer.Ring;
using RingServer.Types;
using RingServer.Config;
using RingServer.Mseed;

namespace RingServer.Protocols;

/// <summary>
/// Extended client info for SeedLink protocol.
/// Equivalent to SLClient_s in slclient.h.
/// </summary>
public class SLExtInfo
{
    public int SlVersion;
    public int SlProtocol;
    public int ProtoMajor = 3;
    public int ProtoMinor = 0;
    public byte[] SeqNumbers = new byte[4];
    /// <summary>Current station ID (NET_STA format) for SELECT command context.</summary>
    public string? CurrentStation;
    /// <summary>Requested start time from TIME command (NsTime.Unset if not set).</summary>
    public NsTime StartTime = NsTime.Unset;
    /// <summary>Requested end time from TIME command (NsTime.Unset if not set).</summary>
    public NsTime EndTime = NsTime.Unset;
    /// <summary>True if DATA command was received in station mode (waiting for END).</summary>
    public bool InStationMode = false;
    /// <summary>Accumulated station IDs for this client (NET_STA format).</summary>
    public List<string> SelectedStations = new();
    /// <summary>Pending DB records for historical streaming (DB → ring fallback).</summary>
    public Queue<MseedQueryResult>? DbRecords;
    /// <summary>True if DB pre-fetch has been performed for this session.</summary>
    public bool DbPreFetchDone;
    /// <summary>End time of the last DB record sent. Used to reposition ring reader after DB exhaustion.</summary>
    public NsTime LastDbEndTime = NsTime.Unset;
    /// <summary>The requested end time from TIME command. When DB pagination reaches this time, stop refetching.</summary>
    public NsTime DbQueryEndTime = NsTime.Unset;
    /// <summary>The stream ID LIKE patterns used for DB queries (stored for pagination refetch).</summary>
    public List<string>? DbQueryStreamIds;
    /// <summary>Cached RingPacket for StreamPackets (avoid per-call GC allocations).</summary>
    internal RingPacket? CachedPacket;
    /// <summary>Cached packet data buffer for StreamPackets.</summary>
    internal byte[]? CachedPacketData;
    /// <summary>Cached size of the packet data buffer.</summary>
    internal int CachedPacketDataSize;
}

/// <summary>
/// SeedLink protocol handler.
/// Equivalent to slclient.c.
/// </summary>
public static class SeedLinkProtocol
{
    /// <summary>
    /// Handle a SeedLink command from the client.
    /// Returns 0 on success, non-zero to close connection.
    /// Equivalent to SLHandleCmd() in slclient.c.
    /// </summary>
    /// <param name="cinfo">Client info.</param>
    /// <param name="lineLen">Number of bytes in the command line (excluding terminator).</param>
    public static int HandleCommand(ClientInfo cinfo, int lineLen)
    {
        if (lineLen <= 0)
            return 0;

        // Ensure ExtInfo is initialized for SeedLink clients
        if (cinfo.ExtInfo == null)
        {
            cinfo.ExtInfo = new SLExtInfo();
        }

        string cmd = Encoding.ASCII.GetString(cinfo.RecvBuffer!, 0, lineLen);
        cmd = cmd.TrimEnd('\n', '\r');

        if (string.IsNullOrEmpty(cmd))
            return 0;

        Logging.lprintf(1, "[{0}] SeedLink command: {1}", cinfo.Hostname, cmd);

        // Split command
        var parts = cmd.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0)
            return 0;

        string command = parts[0].ToUpperInvariant();

        switch (command)
        {
            case "HELLO":
                SendHello(cinfo);
                break;

            case "ID":
                SendServerId(cinfo);
                break;

            case "INFO":
                HandleInfo(cinfo, cmd);
                break;

            case "CAT":
                // Catalog streams
                SendCatalog(cinfo);
                break;

            case "CAPABILITIES":
                // Accept capabilities, send OK
                SendResponse(cinfo, "OK");
                break;

            case "SLPROTO":
                // SeedLink protocol version negotiation
                HandleSlProto(cinfo, parts);
                break;

            case "USERAGENT":
                // Client user agent string (SeedLink v4)
                if (parts.Length > 1)
                    cinfo.ClientId = string.Join(" ", parts[1..]);
                SendResponse(cinfo, "OK");
                break;

            case "BATCH":
                // Batch mode: suppress per-command OK replies (not fully impl, just ack)
                SendResponse(cinfo, "OK");
                break;

            case "STATION":
                // Request station data — SeedLink v3: STATION <sta> <net>
                HandleStation(cinfo, parts);
                break;

            case "SELECT":
                // Select specific channels for current STATION
                HandleSelect(cinfo, cmd);
                break;

            case "DATA":
            case "FETCH":
                // Start streaming from specified packet (v3: DATA [seq_hex [time]])
                // Sets ring position and starts streaming
                HandleData(cinfo, parts, command == "FETCH");
                break;

            case "TIME":
                // TIME start_time [end_time] — window-based request (SeedLink v3)
                HandleTime(cinfo, parts);
                break;

            case "MATCH":
                if (parts.Length > 1)
                    HandleMatch(cinfo, cmd);
                break;

            case "REJECT":
                if (parts.Length > 1)
                    HandleReject(cinfo, cmd);
                break;

            case "LINT":
                if (parts.Length > 1)
                    HandleLimit(cinfo, cmd);
                break;

            case "STREAM":
                // Start streaming from earliest
                cinfo.State = ClientState.Stream;
                cinfo.Reader!.PktId = Constants.RingIdEarliest;
                SendResponse(cinfo, "OK");
                break;

            case "END":
            case "ENDFETCH":
                // End negotiation, start streaming (SeedLink v3/v4)
                HandleEnd(cinfo);
                break;

            case "BYE":
                // Disconnect
                return 1;

            default:
                Logging.lprintf(0, "[{0}] Unknown SeedLink command: {1}", cinfo.Hostname, command);
                break;
        }

        // Buffer consumption is now handled by the main loop in ClientHandler
        return 0;
    }

    /// <summary>
    /// Stream packets to the client over SeedLink protocol.
    /// Equivalent to SLStreamPackets() in slclient.c.
    /// Returns number of bytes sent, 0 if no data, negative on error.
    ///
    /// MATCHES C ORIGINAL: reads ONE packet per call, sends it, returns.
    /// The main loop re-calls this function.  C original's 10-attempt
    /// THROTTLE_TRIGGER is handled in ClientHandler, not here.
    /// </summary>
    public static int StreamPackets(ClientInfo cinfo, RingBuffer ringBuffer)
    {
        if (cinfo.State != ClientState.Stream)
            return 0;

        // DB fallback: send pre-fetched historical records before ring streaming
        if (cinfo.ExtInfo is SLExtInfo slExt && slExt.DbRecords != null && slExt.DbRecords.Count > 0)
        {
            var dbRecord = slExt.DbRecords.Dequeue();

            // Track end time of last DB record for ring repositioning
            slExt.LastDbEndTime = dbRecord.EndTime;

            // If this was the last DB record, try to fetch more from DB or reposition ring.
            if (slExt.DbRecords.Count == 0)
            {
                HandleDbQueueExhausted(cinfo, slExt);
            }

            return SendDbRecord(cinfo, dbRecord, slExt.ProtoMajor);
        }

        // Use cached buffers to avoid per-call GC pressure.
        // Allocating new byte[~4096] + new RingPacket() every iteration causes
        // ~42MB/sec garbage for high-throughput streaming, leading to GC pauses
        // that cause the reader to fall behind and miss packets (gaps).
        var slextInfo = cinfo.ExtInfo as SLExtInfo;
        int packetDataSize = (int)(cinfo.RingParams.PktSize - RingPacket.SerializedSize);

        RingPacket packet;
        byte[] packetData;

        if (slextInfo?.CachedPacket != null && slextInfo.CachedPacketData != null &&
            slextInfo.CachedPacketDataSize == packetDataSize)
        {
            packet = slextInfo.CachedPacket;
            packetData = slextInfo.CachedPacketData;
        }
        else
        {
            packet = new RingPacket();
            packetData = new byte[packetDataSize];
            if (slextInfo != null)
            {
                slextInfo.CachedPacket = packet;
                slextInfo.CachedPacketData = packetData;
                slextInfo.CachedPacketDataSize = packetDataSize;
            }
        }

        Array.Clear(packetData, 0, packetData.Length);

        ulong pktid = ringBuffer.ReadNext(cinfo.Reader!, packet, packetData);

        if (pktid == Constants.RingIdError)
            return -1;
        if (pktid == Constants.RingIdNone)
            return 0;

        // Only send miniSEED data (packet data portion, not the ring header)
        if (packet.DataSize == 0)
            return 0;

        int dataLen = (int)Math.Min(packet.DataSize, (ulong)packetData.Length);

        // Validate miniSEED header — skip non-miniSEED packets.
        // Equivalent to MS2_ISVALIDHEADER / MS3_ISVALIDHEADER check in C original.
        // IMPORTANT: Return dataLen (non-zero) on skip so the main loop does NOT throttle.
        // The C original returns (datasize) even for non-miniSEED packets so that
        // the client loop immediately tries the next packet without polling delay.
        if (!IsValidMiniSeedHeader(packetData, dataLen))
        {
            // Hex dump first 16 bytes for debugging
            var hex = new System.Text.StringBuilder(dataLen * 3);
            int dumpLen = Math.Min(dataLen, 32);
            for (int i = 0; i < dumpLen; i++)
                hex.AppendFormat("{0:X2} ", packetData[i]);
            Logging.lprintf(2, "[{0}] Skipping non-miniSEED packet for stream {1} (pktid={2}, datasize={3}) first bytes: {4}",
                cinfo.Hostname, packet.StreamId?.TrimEnd('\0'), packet.PktId, packet.DataSize, hex.ToString());
            return dataLen; // NON-ZERO → no throttle, next packet immediately
        }

        byte[] header;
        int protoMajor = 3;
        if (cinfo.ExtInfo is SLExtInfo slext)
        {
            protoMajor = slext.ProtoMajor;
        }

        if (protoMajor == 4)
        {
            // Construct v4 header: "SE" + format(1) + subformat(1) + payloadlen(4) + pktid(8) + staidlen(1) + staid
            // Get station ID from streamid
            string staid = "";
            string streamId = packet.StreamId.TrimEnd('\0', ' ');
            if (streamId.StartsWith("FDSN:"))
            {
                // Format: FDSN:NET_STA_LOC_BAND_SOURCE_POSITION
                // C version does: ms_sid2nslc (streamid, net, sta, NULL, NULL)
                // and formats as net_sta
                var parts = streamId.Split(':');
                if (parts.Length > 1)
                {
                    var subParts = parts[parts.Length - 1].Split('_');
                    if (subParts.Length >= 2)
                    {
                        staid = $"{subParts[0]}_{subParts[1]}";
                    }
                }
            }
            else
            {
                staid = streamId;
            }

            byte[] staidBytes = Encoding.ASCII.GetBytes(staid);
            byte staidLen = (byte)staidBytes.Length;

            // formats: '2' (miniSEED v2) or '3' (miniSEED v3)
            // Let's inspect packetData to see if it's MS3.
            // MS3_ISVALIDHEADER check: packetData[0]=='M' && packetData[1]=='S' && packetData[2]==3
            char format = ' ';
            if (dataLen >= 3 && packetData[0] == (byte)'M' && packetData[1] == (byte)'S' && packetData[2] == 3)
            {
                format = '3';
            }
            else if (dataLen >= 8 && (packetData[6] == (byte)'D' || packetData[6] == (byte)'R' || packetData[6] == (byte)'Q' || packetData[6] == (byte)'M'))
            {
                format = '2';
            }

            char subformat = 'D'; // Data

            header = new byte[17 + staidLen];
            header[0] = (byte)'S';
            header[1] = (byte)'E';
            header[2] = (byte)format;
            header[3] = (byte)subformat;

            // V4 header values are little-endian byte order
            uint payloadlen = (uint)dataLen;
            BitConverter.TryWriteBytes(header.AsSpan(4, 4), payloadlen);
            BitConverter.TryWriteBytes(header.AsSpan(8, 8), pktid);
            header[16] = staidLen;
            if (staidLen > 0)
            {
                Buffer.BlockCopy(staidBytes, 0, header, 17, staidLen);
            }
        }
        else
        {
            // SeedLink v3 header: "SL" + 6-hex-digit sequence number = 8 bytes
            // Use lowest 24-bits of pktid as sequence number (SeedLink v3 limit)
            string headerStr = $"SL{(pktid & 0xFFFFFF):X6}";
            header = Encoding.ASCII.GetBytes(headerStr); // 8 bytes
        }

        // Send header + miniSEED data as two buffers
        int result = SendData.SendBuffersToClient(
            cinfo,
            [header, packetData],
            [header.Length, dataLen],
            2,
            false);
        if (result < 0)
            return result;

        int sentBytes = header.Length + dataLen;
        return sentBytes;
    }

    /// <summary>
    /// Called when the DB pre-fetch queue is empty.
    /// Tries to fetch more data from DB (pagination) before falling back to ring.
    /// This is critical for queries spanning large time ranges where the DB
    /// has more data than the initial LIMIT allows.
    /// </summary>
    private static void HandleDbQueueExhausted(ClientInfo cinfo, SLExtInfo slExt)
    {
        Logging.lprintf(1, "[{0}] DB queue exhausted (last end_time={1}, query_end_time={2})",
            cinfo.Hostname,
            slExt.LastDbEndTime.ToDateTime().ToString("yyyy-MM-ddTHH:mm:ss"),
            slExt.DbQueryEndTime.IsUnset ? "unset" : slExt.DbQueryEndTime.ToDateTime().ToString("yyyy-MM-ddTHH:mm:ss"));

        // Check if there is more data in DB between last record end and the requested end time
        if (!slExt.DbQueryEndTime.IsUnset && slExt.LastDbEndTime < slExt.DbQueryEndTime)
        {
            var archive = ServerConfig.Archive;
            if (archive != null && slExt.DbQueryStreamIds != null && slExt.DbQueryStreamIds.Count > 0)
            {
                try
                {
                    // Fetch next batch starting just after the last record we sent.
                    // Add 1 millisecond to avoid re-fetching the exact same record.
                    NsTime nextStart = new NsTime(slExt.LastDbEndTime.Value + 1_000_000L);

                    var nextBatch = archive.QueryByTime(
                        slExt.DbQueryStreamIds, nextStart, slExt.DbQueryEndTime, 10000);

                    if (nextBatch.Count > 0)
                    {
                        slExt.DbRecords = new Queue<MseedQueryResult>(nextBatch);
                        Logging.lprintf(1, "[{0}] DB pagination: refetched {1} more records from {2}",
                            cinfo.Hostname, nextBatch.Count,
                            nextBatch[0].StartTime.ToDateTime().ToString("yyyy-MM-ddTHH:mm:ss"));
                        return; // Queue is now filled — StreamPackets will pick up from here
                    }
                }
                catch (Exception ex)
                {
                    Logging.lprintf(0, "[{0}] DB pagination refetch failed: {1}",
                        cinfo.Hostname, ex.Message);
                }
            }
        }

        // No more DB data — try ring, otherwise fall back to live
        var rb = new RingBuffer(cinfo.RingParams);
        ulong repositioned = rb.After(cinfo.Reader!, slExt.LastDbEndTime, 0);

        if (repositioned != Constants.RingIdNone && repositioned != Constants.RingIdError)
        {
            Logging.lprintf(2, "[{0}] Ring positioned to pktid={1} at offset={2} after DB",
                cinfo.Hostname, repositioned, cinfo.Reader!.PktOffset);
        }
        else
        {
            Logging.lprintf(1, "[{0}] Ring has no packets after DB end_time, falling back to RINGID_NEXT",
                cinfo.Hostname);
            cinfo.Reader!.PktOffset = -1;
            cinfo.Reader!.PktId = Constants.RingIdNext;
        }
    }

    /// <summary>
    /// Send a single pre-fetched DB record as a SeedLink packet.
    /// Constructs the SeedLink header (v3 or v4) around the raw miniSEED data.
    /// </summary>
    private static int SendDbRecord(ClientInfo cinfo, MseedQueryResult record, int protoMajor)
    {
        byte[] rawData = record.RawData!;
        int dataLen = rawData.Length;

        if (dataLen == 0)
            return 0;

        byte[] header;
        if (protoMajor == 4)
        {
            // v4 header
            string streamId = record.StreamId;
            string staid = "";
            if (streamId.StartsWith("FDSN:"))
            {
                var parts = streamId.Split(':');
                if (parts.Length > 1)
                {
                    var subParts = parts[^1].Split('_');
                    if (subParts.Length >= 2)
                        staid = $"{subParts[0]}_{subParts[1]}";
                }
            }
            else
            {
                staid = streamId;
            }

            byte[] staidBytes = Encoding.ASCII.GetBytes(staid);
            byte staidLen = (byte)staidBytes.Length;

            // Determine format from miniSEED header
            char format = ' ';
            if (dataLen >= 3 && rawData[0] == 'M' && rawData[1] == 'S' && rawData[2] == 3)
                format = '3';
            else if (dataLen >= 8 && (rawData[6] == 'D' || rawData[6] == 'R' || rawData[6] == 'Q' || rawData[6] == 'M'))
                format = '2';

            header = new byte[17 + staidLen];
            header[0] = (byte)'S';
            header[1] = (byte)'E';
            header[2] = (byte)format;
            header[3] = (byte)'D';
            BitConverter.TryWriteBytes(header.AsSpan(4, 4), (uint)dataLen);
            BitConverter.TryWriteBytes(header.AsSpan(8, 8), record.PktId);
            header[16] = staidLen;
            if (staidLen > 0)
                Buffer.BlockCopy(staidBytes, 0, header, 17, staidLen);
        }
        else
        {
            // v3 header: "SL" + 6-hex-digit sequence
            string headerStr = $"SL{(record.PktId & 0xFFFFFF):X6}";
            header = Encoding.ASCII.GetBytes(headerStr);
        }

        int result = SendData.SendBuffersToClient(
            cinfo,
            [header, rawData],
            [header.Length, dataLen],
            2,
            false);
        if (result < 0)
            return result;

        return header.Length + dataLen;
    }

    /// <summary>
    /// Send HELLO response with server capabilities.
    /// C original: SLSERVER_ID "\r\n" + config.serverid + "\r\n"
    /// Two lines: capabilities line, then server description.
    /// </summary>
    private static void SendHello(ClientInfo cinfo)
    {
        var config = RingServer.Config.ServerConfig.Instance;
        string serverId = config.ServerId ?? "Ring Server";
        // Format: "SeedLink v4.0 (...) :: ...\r\n<serverid>\r\n"
        string response = $"{Constants.SlServerId}\r\n{serverId}\r\n";
        byte[] data = Encoding.ASCII.GetBytes(response);
        SendData.SendToClient(cinfo, data, data.Length, false);
    }

    /// <summary>
    /// Send server ID (same format as HELLO per C original).
    /// </summary>
    private static void SendServerId(ClientInfo cinfo)
    {
        // C original uses same HELLO handler for ID command
        SendHello(cinfo);
    }

    /// <summary>
    /// Send catalog of available streams.
    /// </summary>
    private static void SendCatalog(ClientInfo cinfo)
    {
        var ringBuffer = new RingBuffer(cinfo.RingParams);
        var streams = ringBuffer.GetStreamsStack(cinfo.Reader);

        var sb = new StringBuilder();
        sb.AppendLine("CAT");
        while (streams.NotEmpty)
        {
            var stream = (RingStream)streams.Pop()!;
            sb.AppendLine(stream.StreamId);
        }
        sb.AppendLine("END");

        byte[] data = Encoding.ASCII.GetBytes(sb.ToString());
        SendData.SendToClient(cinfo, data, data.Length, false);
    }

    /// <summary>
    /// Handle STATION command.
    /// SeedLink v3: STATION <sta> [<net>]
    /// Sets match pattern and sends OK.
    /// </summary>
    private static void HandleStation(ClientInfo cinfo, string[] parts)
    {
        if (parts.Length < 2)
        {
            SendResponse(cinfo, "ERROR");
            return;
        }

        string sta = parts[1];
        string net = (parts.Length >= 3) ? parts[2] : "*";

        // Build station ID in NET_STA format (C original: reqnet_reqsta)
        string staid = $"{net}_{sta}";

        // Store current station for SELECT command context
        if (cinfo.ExtInfo is SLExtInfo slext)
        {
            slext.CurrentStation = staid;
            slext.InStationMode = true;  // Entered per-station mode
            if (!slext.SelectedStations.Contains(staid))
                slext.SelectedStations.Add(staid);
        }

        // Build regex matching all channels for this station (no selector yet)
        // Pattern: ^(?:FDSN:)?NET_STA_.*(?:/MSEED)?[23]?$
        string pattern = BuildStreamRegex(staid, null);
        RingBuffer.UpdatePattern(ref cinfo.Reader!.Match, pattern, "station");
        RingBuffer.UpdatePattern(ref cinfo.Reader!.Reject, null, "reject");

        Logging.lprintf(2, "[{0}] STATION {1} {2} → regex={3}", cinfo.Hostname, sta, net, pattern);
        SendResponse(cinfo, "OK");
    }

    /// <summary>
    /// Handle SLPROTO command (SeedLink v4 protocol negotiation).
    /// </summary>
    private static void HandleSlProto(ClientInfo cinfo, string[] parts)
    {
        // Ensure ExtInfo is SLExtInfo
        if (cinfo.ExtInfo == null)
        {
            cinfo.ExtInfo = new SLExtInfo();
        }
        var slext = (SLExtInfo)cinfo.ExtInfo;

        // Accept SLPROTO 3.x and 4.0
        if (parts.Length >= 2)
        {
            string versionStr = parts[1];
            var versionParts = versionStr.Split('.');
            if (versionParts.Length > 0 && int.TryParse(versionParts[0], out int major))
            {
                int minor = 0;
                if (versionParts.Length > 1)
                {
                    int.TryParse(versionParts[1], out minor);
                }

                if (major == 3 || (major == 4 && minor == 0))
                {
                    slext.ProtoMajor = major;
                    slext.ProtoMinor = minor;
                    Logging.lprintf(2, "[{0}] SLPROTO {1} accepted", cinfo.Hostname, versionStr);
                    SendResponse(cinfo, "OK");
                    return;
                }
            }
        }

        Logging.lprintf(2, "[{0}] SLPROTO {1} rejected", cinfo.Hostname,
            parts.Length >= 2 ? parts[1] : "?");
        SendResponse(cinfo, "ERROR UNSUPPORTED");
    }

    /// <summary>
    /// Handle DATA or FETCH command.
    /// SeedLink v3: DATA [seq_hex [begin_time [end_time]]] — seq is HEX
    /// SeedLink v4: DATA [seq_decimal [begin_time [end_time]]] — seq is DECIMAL
    /// "ALL" → from earliest, "-1" or no seq → from next.
    /// In station mode (after STATION command): sends OK, stays in command state.
    /// Without STATION (all-station mode): triggers streaming immediately.
    /// </summary>
    private static void HandleData(ClientInfo cinfo, string[] parts, bool isFetch = false)
    {
        var slext = cinfo.ExtInfo as SLExtInfo;
        bool inStationMode = slext?.InStationMode ?? false;
        int protoMajor = slext?.ProtoMajor ?? 3;

        // Parse optional starting sequence number
        if (parts.Length >= 2)
        {
            string seqStr = parts[1];

            if (seqStr.Equals("ALL", StringComparison.OrdinalIgnoreCase))
            {
                cinfo.Reader!.PktId = Constants.RingIdEarliest;
                Logging.lprintf(2, "[{0}] DATA ALL → from earliest", cinfo.Hostname);
            }
            else if (seqStr == "-1")
            {
                cinfo.Reader!.PktId = Constants.RingIdNext;
                Logging.lprintf(2, "[{0}] DATA -1 → from next", cinfo.Hostname);
            }
            else
            {
                // Parse sequence: v4 uses decimal, v3 uses hex
                ulong startpacket;
                if (protoMajor == 4)
                {
                    // v4: decimal sequence
                    if (!ulong.TryParse(seqStr, System.Globalization.NumberStyles.None,
                        null, out startpacket) || startpacket == 0)
                    {
                        // Unparseable or 0 → start from next
                        cinfo.Reader!.PktId = Constants.RingIdNext;
                        Logging.lprintf(2, "[{0}] DATA seq={1} (invalid/0) → from next", cinfo.Hostname, seqStr);
                        goto afterSeqParse;
                    }
                }
                else
                {
                    // v3: hex sequence
                    if (!uint.TryParse(seqStr, System.Globalization.NumberStyles.HexNumber,
                        null, out uint hexseq) || hexseq == 0xFFFFFF)
                    {
                        cinfo.Reader!.PktId = Constants.RingIdNext;
                        Logging.lprintf(2, "[{0}] DATA seq={1} (invalid/FFFFFF) → from next", cinfo.Hostname, seqStr);
                        goto afterSeqParse;
                    }
                    startpacket = hexseq;
                }

                // SeedLink clients resume by requesting: lastpacket + 1
                // The ring needs the actual last packet ID for ReadNext()
                if (startpacket < Constants.RingIdMaximum && startpacket > 0)
                    startpacket -= 1;

                cinfo.Reader!.PktId = startpacket;
                Logging.lprintf(2, "[{0}] DATA seq={1} → from pktid {2}", cinfo.Hostname, seqStr, startpacket);
            }
        }
        else
        {
            // No sequence: start from next new packet
            cinfo.Reader!.PktId = Constants.RingIdNext;
            Logging.lprintf(2, "[{0}] DATA (no seq) → from next", cinfo.Hostname);
        }

    afterSeqParse:

        if (isFetch && slext != null)
        {
            // FETCH = dial-up mode, mark it (ignored for now, same as DATA)
        }

        if (inStationMode)
        {
            // In station mode: send OK and stay in command state, wait for END
            SendResponse(cinfo, "OK");
            Logging.lprintf(2, "[{0}] DATA in station mode → OK, waiting for END", cinfo.Hostname);
        }
        else
        {
            // All-station mode: trigger streaming immediately (no OK reply for DATA)
            cinfo.State = ClientState.Stream;
            Logging.lprintf(2, "[{0}] DATA → streaming", cinfo.Hostname);
        }
    }

    /// <summary>
    /// Handle TIME command (SeedLink v3).
    /// TIME start_time [end_time]
    /// Format: YYYY,MM,DD,HH,MM,SS (e.g., 2026,07,06,08,50,46)
    /// In station mode: stores times, sends OK.
    /// Without STATION: sets ring position by time and starts streaming.
    /// </summary>
    private static void HandleTime(ClientInfo cinfo, string[] parts)
    {
        if (parts.Length < 2)
        {
            SendResponse(cinfo, "ERROR arguments");
            return;
        }

        NsTime startTime = ParseSeedLinkTime(parts[1]);
        NsTime endTime = NsTime.Unset;

        if (startTime == NsTime.Error)
        {
            Logging.lprintf(0, "[{0}] TIME: bad start time '{1}'", cinfo.Hostname, parts[1]);
            SendResponse(cinfo, "ERROR arguments");
            return;
        }

        if (parts.Length >= 3)
        {
            endTime = ParseSeedLinkTime(parts[2]);
            if (endTime == NsTime.Error)
            {
                Logging.lprintf(0, "[{0}] TIME: bad end time '{1}'", cinfo.Hostname, parts[2]);
                SendResponse(cinfo, "ERROR arguments");
                return;
            }
        }

        var slext = cinfo.ExtInfo as SLExtInfo;

        if (slext?.InStationMode == true)
        {
            // Station mode: store times, send OK, stay in command state
            if (slext != null)
            {
                slext.StartTime = startTime;
                // Only update EndTime if explicitly provided.
                // SWARM sends two TIME commands: first with start+end, then start-only.
                // Overwriting EndTime to Unset would break DB pagination upper bound.
                if (!endTime.IsUnset)
                    slext.EndTime = endTime;
            }
            Logging.lprintf(2, "[{0}] TIME in station mode: start={1} end={2} (prev_end={3})",
                cinfo.Hostname, parts[1],
                parts.Length >= 3 ? parts[2] : "none",
                slext?.EndTime.IsUnset == false ? slext.EndTime.ToIsoString() : "unset");
            SendResponse(cinfo, "OK");
        }
        else
        {
            // All-station mode: position ring by time and start streaming
            Logging.lprintf(2, "[{0}] TIME all-station: start={1} end={2}",
                cinfo.Hostname, parts[1], parts.Length >= 3 ? parts[2] : "none");

            if (slext != null)
            {
                slext.StartTime = startTime;
                // Only update EndTime if explicitly provided (same guard as station mode)
                if (!endTime.IsUnset)
                    slext.EndTime = endTime;
            }

            // Pre-fetch historical data from PostgreSQL if archive is available
            var archive = ServerConfig.Archive;
            if (archive != null && slext != null)
            {
                try
                {
                    var streamIds = new List<string> { "%" };
                    slext.DbQueryStreamIds = streamIds;
                    NsTime dbEnd = slext.EndTime.IsUnset ? NsTime.Unset : slext.EndTime;
                    var dbResults = archive.QueryByTime(streamIds, slext.StartTime, dbEnd, 10000);
                    if (dbResults.Count > 0)
                    {
                        slext.DbRecords = new Queue<MseedQueryResult>(dbResults);
                        slext.DbQueryEndTime = dbEnd;
                        Logging.lprintf(1, "[{0}] TIME: pre-fetched {1} historical records from DB (last_end={2}, queried_end={3})",
                            cinfo.Hostname, dbResults.Count,
                            dbResults[^1].EndTime.ToDateTime().ToString("yyyy-MM-ddTHH:mm:ss"),
                            dbEnd.IsUnset ? "unset" : dbEnd.ToDateTime().ToString("yyyy-MM-ddTHH:mm:ss"));
                    }
                }
                catch (Exception ex)
                {
                    Logging.lprintf(0, "[{0}] TIME: DB pre-fetch failed: {1}",
                        cinfo.Hostname, ex.Message);
                }
            }

            var ringBuffer = new RingBuffer(cinfo.RingParams);
            ringBuffer.After(cinfo.Reader!, startTime, 0);
            cinfo.State = ClientState.Stream;
        }
    }

    /// <summary>
    /// Handle END or ENDFETCH command — finalize station selection and start streaming.
    /// Equivalent to C's STATE_RINGCONFIG transition triggered by END.
    ///
    /// C original flow (slclient.c lines 158-250):
    ///   1. Build regex from all accumulated stations (done in HandleStation/HandleSelect)
    ///   2. Iterate stations: configure regexes, find widest time window, find start packet
    ///   3. Position ring to start packet ID via RingPosition(reader, startid, NSTUNSET)
    ///   4. Position ring by start time if no packet ID (via RingPosition with NSTUNSET time)
    ///   5. Fallback: if reader is still unpositioned, starts from next
    /// </summary>
    private static void HandleEnd(ClientInfo cinfo)
    {
        // Guard: if already streaming (e.g., SWARM sends duplicate END),
        // don't re-process — it would reposition the ring reader and
        // corrupt or re-send already-acknowledged data.
        if (cinfo.State == ClientState.Stream)
        {
            Logging.lprintf(2, "[{0}] END: already streaming, ignoring duplicate END",
                cinfo.Hostname);
            return;
        }

        var slext = cinfo.ExtInfo as SLExtInfo;
        var ringBuffer = new RingBuffer(cinfo.RingParams);
        bool positioned = false;

        // Step 1: If HandleData set a specific packet ID (not a special constant),
        // position the ring reader to that packet. The C original does this by
        // iterating stations and calling RingPosition(reader, startid, NSTUNSET).
        if (cinfo.Reader!.PktId > 0 && cinfo.Reader!.PktId < Constants.RingIdMaximum)
        {
            ulong pktid = cinfo.Reader.PktId;
            ulong retval = ringBuffer.Position(cinfo.Reader, pktid, NsTime.Unset);
            if (retval == Constants.RingIdNone)
            {
                // Packet not found — fallback to next available
                Logging.lprintf(1, "[{0}] END: packet {1} not in ring, falling back to next",
                    cinfo.Hostname, pktid);
                cinfo.Reader.PktId = Constants.RingIdNext;
            }
            else if (retval == Constants.RingIdError)
            {
                Logging.lprintf(0, "[{0}] END: RingPosition error for packet {1}",
                    cinfo.Hostname, pktid);
                cinfo.Reader.PktId = Constants.RingIdNext;
            }
            else
            {
                positioned = true;
                Logging.lprintf(2, "[{0}] END: positioned ring to packet {1} (offset={2})",
                    cinfo.Hostname, pktid, cinfo.Reader.PktOffset);
            }
        }

        // Step 2: If start time was set (via TIME command) and not yet positioned,
        // use time-based ring positioning (After = rewind to earliest after ref time)
        if (!positioned && slext != null &&
            slext.StartTime != NsTime.Unset && slext.StartTime != NsTime.Error)
        {
            ringBuffer.After(cinfo.Reader, slext.StartTime, 0);
            positioned = true;
            Logging.lprintf(2, "[{0}] END: positioned ring by start time {1}",
                cinfo.Hostname, slext.StartTime);
        }

        // Step 2b: If PostgreSQL archive is available and the requested start time
        // is earlier than what the ring buffer holds, pre-fetch historical data from DB.
        if (slext != null && slext.StartTime != NsTime.Unset &&
            slext.StartTime != NsTime.Error && !slext.DbPreFetchDone)
        {
            var archive = ServerConfig.Archive;
            if (archive != null)
            {
                try
                {
                    // Build list of stream IDs to query
                    var streamIds = new List<string>();
                    if (slext.SelectedStations.Count > 0)
                    {
                        // Convert NET_STA to FDSN wildcard: "VG_STNM0" → "FDSN:VG_STNM0_%"
                        foreach (var staid in slext.SelectedStations)
                        {
                            streamIds.Add($"FDSN:{staid}_%");
                        }
                    }
                    else
                    {
                        // All stations
                        streamIds.Add("%");
                    }

                    // Save stream patterns for DB pagination refetch
                    slext.DbQueryStreamIds = streamIds;

                    NsTime dbEnd = slext.EndTime.IsUnset ? NsTime.Unset : slext.EndTime;

                    var dbResults = archive.QueryByTime(
                        streamIds, slext.StartTime, dbEnd, 10000);

                    if (dbResults.Count > 0)
                    {
                        slext.DbRecords = new Queue<MseedQueryResult>(dbResults);
                        slext.DbQueryEndTime = dbEnd;
                        Logging.lprintf(1, "[{0}] END: pre-fetched {1} historical records from DB (end_time={2}, queried_end={3})",
                            cinfo.Hostname, dbResults.Count,
                            dbResults[^1].EndTime.ToDateTime().ToString("yyyy-MM-ddTHH:mm:ss"),
                            dbEnd.IsUnset ? "unset" : dbEnd.ToDateTime().ToString("yyyy-MM-ddTHH:mm:ss"));
                    }
                    else
                    {
                        Logging.lprintf(2, "[{0}] END: no historical records in DB for time range",
                            cinfo.Hostname);
                    }
                }
                catch (Exception ex)
                {
                    Logging.lprintf(0, "[{0}] END: DB pre-fetch failed: {1}",
                        cinfo.Hostname, ex.Message);
                }
            }
            slext.DbPreFetchDone = true;
        }

        // Step 3: Fallback — if reader still has RINGID_NONE, set to RINGID_NEXT
        // so streaming starts from the latest new packet (matches C original)
        if (cinfo.Reader.PktId == Constants.RingIdNone)
        {
            cinfo.Reader.PktId = Constants.RingIdNext;
            Logging.lprintf(2, "[{0}] END: reader had RINGID_NONE, setting to RINGID_NEXT",
                cinfo.Hostname);
        }

        cinfo.State = ClientState.Stream;
        Logging.lprintf(2, "[{0}] END → streaming (pktid={1}, pktOffset={2})",
            cinfo.Hostname, cinfo.Reader.PktId, cinfo.Reader.PktOffset);
    }

    /// <summary>
    /// Parse SeedLink v3 time string: YYYY,MM,DD,HH,MM,SS
    /// Returns NsTime.Error on failure.
    /// </summary>
    private static NsTime ParseSeedLinkTime(string s)
    {
        // Format: YYYY,MM,DD,HH,MM,SS  (6 comma-separated integers)
        var p = s.Split(',');
        if (p.Length < 3) return NsTime.Error;
        if (!int.TryParse(p[0], out int year)) return NsTime.Error;
        if (!int.TryParse(p[1], out int month)) return NsTime.Error;
        if (!int.TryParse(p[2], out int day)) return NsTime.Error;
        int hour = 0, min = 0, sec = 0;
        if (p.Length >= 4 && !int.TryParse(p[3], out hour)) return NsTime.Error;
        if (p.Length >= 5 && !int.TryParse(p[4], out min)) return NsTime.Error;
        if (p.Length >= 6 && !int.TryParse(p[5], out sec)) return NsTime.Error;

        try
        {
            var dt = new DateTime(year, month, day, hour, min, sec, DateTimeKind.Utc);
            return NsTime.FromDateTime(dt);
        }
        catch
        {
            return NsTime.Error;
        }
    }

    /// <summary>
    /// Handle SELECT command (channel filter for current STATION).
    /// SeedLink v3: SELECT [neg]LLCCC (5-char: loc+channel, or 3-char: channel only)
    /// </summary>
    private static void HandleSelect(ClientInfo cinfo, string cmd)
    {
        // SELECT is followed by a pattern
        int idx = cmd.IndexOf(' ');
        if (idx >= 0)
        {
            string selector = cmd[(idx + 1)..].Trim();
            if (!string.IsNullOrEmpty(selector))
            {
                // Get station ID from SLExtInfo for regex building
                string? staid = (cinfo.ExtInfo is SLExtInfo slext) ? slext.CurrentStation : null;

                // Build regex combining station + selector
                // C original converts v3 LLCCC to NET_STA_LL_C_C_C format before building regex
                string? convertedSelector = ConvertV3Selector(selector);
                if (convertedSelector == null)
                {
                    Logging.lprintf(0, "[{0}] SELECT invalid pattern: {1}", cinfo.Hostname, selector);
                    SendResponse(cinfo, "ERROR");
                    return;
                }

                string pattern = BuildStreamRegex(staid, convertedSelector);
                RingBuffer.UpdatePattern(ref cinfo.Reader!.Match, pattern, "select");
                Logging.lprintf(2, "[{0}] SELECT {1} → regex={2}", cinfo.Hostname, selector, pattern);
                SendResponse(cinfo, "OK");
            }
        }
    }

    /// <summary>
    /// Convert SeedLink v3 selector (LLCCC or CCC) to v4 style (LL_C_C_C or ??_C_C_C).
    /// Returns null if invalid format.
    /// </summary>
    private static string? ConvertV3Selector(string sel)
    {
        bool negate = sel.StartsWith('!');
        string s = negate ? sel[1..] : sel;
        string prefix = negate ? "!" : "";

        // Strip subtype suffix (anything after '.')
        int dot = s.IndexOf('.');
        if (dot >= 0) s = s[..dot];

        if (s.Length == 5)
        {
            // LLCCC → LL_C_C_C
            return $"{prefix}{s[0]}{s[1]}_{s[2]}_{s[3]}_{s[4]}";
        }
        else if (s.Length == 3)
        {
            // CCC → ??_C_C_C
            return $"{prefix}??_{s[0]}_{s[1]}_{s[2]}";
        }
        else if (s == "*" || s == ".*" || s.Contains('*') || s.Contains('?'))
        {
            // Wildcard — use as-is
            return $"{prefix}{s}";
        }
        return null;
    }

    /// <summary>
    /// Handle MATCH command.
    /// </summary>
    private static void HandleMatch(ClientInfo cinfo, string cmd)
    {
        int idx = cmd.IndexOf(' ');
        if (idx >= 0)
        {
            string pattern = cmd[(idx + 1)..].Trim();
            RingBuffer.UpdatePattern(ref cinfo.Reader!.Match, pattern, "match");
            SendResponse(cinfo, "OK");
        }
    }

    /// <summary>
    /// Handle REJECT command.
    /// </summary>
    private static void HandleReject(ClientInfo cinfo, string cmd)
    {
        int idx = cmd.IndexOf(' ');
        if (idx >= 0)
        {
            string pattern = cmd[(idx + 1)..].Trim();
            RingBuffer.UpdatePattern(ref cinfo.Reader!.Reject, pattern, "reject");
            SendResponse(cinfo, "OK");
        }
    }

    /// <summary>
    /// Handle LINT (limit) command.
    /// </summary>
    private static void HandleLimit(ClientInfo cinfo, string cmd)
    {
        int idx = cmd.IndexOf(' ');
        if (idx >= 0)
        {
            string pattern = cmd[(idx + 1)..].Trim();
            RingBuffer.UpdatePattern(ref cinfo.Reader!.Limit, pattern, "limit");
            SendResponse(cinfo, "OK");
        }
    }

    /// <summary>
    /// Build regex for ring stream ID matching, equivalent to SelectToRegex() in C original.
    /// staid: NET_STA format (may contain * and ? wildcards)
    /// selector: channel selector in v4 format (LL_C_C_C), or null for wildcard
    /// Result: ^(?:FDSN:)?NET_STA_selector(?:/MSEED)?[23]?$
    /// </summary>
    private static string BuildStreamRegex(string? staid, string? selector)
    {
        var sb = new System.Text.StringBuilder();
        sb.Append("^(?:FDSN:)?");

        // Append station ID with wildcard translation
        if (staid != null)
        {
            foreach (char c in staid)
            {
                if (c == '?') sb.Append('.');
                else if (c == '*') sb.Append(".*");
                else sb.Append(c);
            }
        }
        else
        {
            sb.Append(".*");
        }

        sb.Append('_');

        // Append selector with wildcard translation
        if (selector != null)
        {
            // Handle negation prefix — just translate the pattern part
            string s = selector.StartsWith('!') ? selector[1..] : selector;
            foreach (char c in s)
            {
                if (c == '?') sb.Append('.');
                else if (c == '*') sb.Append(".*");
                else sb.Append(c);
            }
        }
        else
        {
            sb.Append(".*");
        }

        sb.Append("(?:/MSEED)?[23]?$");
        return sb.ToString();
    }

    /// <summary>
    /// Send a simple text response.
    /// </summary>
    private static void SendResponse(ClientInfo cinfo, string response)
    {
        string msg = $"{response}\r\n";
        byte[] data = Encoding.ASCII.GetBytes(msg);
        SendData.SendToClient(cinfo, data, data.Length, false);
    }

    /// <summary>
    /// Free SeedLink extended info.
    /// </summary>
    public static void Free(ClientInfo cinfo)
    {
        cinfo.ExtInfo = null;
    }

    private static void HandleInfo(ClientInfo cinfo, string cmd)
    {
        string[] parts = cmd.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        string level = parts.Length > 1 ? parts[1].ToUpperInvariant() : "ID";

        Logging.lprintf(1, "[{0}] INFO {1}", cinfo.Hostname, level);

        if (level == "ID")
        {
            // Send server ID response packed as miniSEED INFO record
            string xml = BuildInfoIdXml();
            SendInfoMseed(cinfo, xml, false);
        }
        else if (level == "STREAMS" || level == "STATIONS")
        {
            // Send stations/streams list as miniSEED INFO record
            bool includeStreams = (level == "STREAMS");
            string xml = BuildInfoStreamsXml(cinfo, includeStreams);
            SendInfoMseed(cinfo, xml, false);
        }
        else if (level == "CAPABILITIES")
        {
            string xml = BuildInfoCapabilitiesXml();
            SendInfoMseed(cinfo, xml, false);
        }
        else if (level == "CONNECTIONS")
        {
            if (!cinfo.Trusted)
            {
                Logging.lprintf(1, "[{0}] Refusing INFO CONNECTIONS request from un-trusted client", cinfo.Hostname);
                string xml = BuildInfoIdXml();
                SendInfoMseed(cinfo, xml, true); // errflag=true
            }
            else
            {
                string xml = BuildInfoConnectionsXml(cinfo);
                SendInfoMseed(cinfo, xml, false);
            }
        }
        else
        {
            // Unknown level — send ID as fallback
            string xml = BuildInfoIdXml();
            SendInfoMseed(cinfo, xml, true); // errflag=true
        }
    }

    /// <summary>
    /// Build INFO CAPABILITIES XML response.
    /// </summary>
    private static string BuildInfoCapabilitiesXml()
    {
        var config = RingServer.Config.ServerConfig.Instance;
        string software = Constants.SlServerId;
        string org = config.ServerId ?? "RingServer";
        string started = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");
        var sb = new StringBuilder();
        sb.Append("<?xml version=\"1.0\"?>\r\n");
        sb.Append($"<seedlink software=\"{EscapeXml(software)}\" organization=\"{EscapeXml(org)}\" started=\"{started}\">\r\n");
        sb.Append("  <capability name=\"dialup\"/>\r\n");
        sb.Append("  <capability name=\"multistation\"/>\r\n");
        sb.Append("  <capability name=\"window-extraction\"/>\r\n");
        sb.Append("  <capability name=\"info:id\"/>\r\n");
        sb.Append("  <capability name=\"info:capabilities\"/>\r\n");
        sb.Append("  <capability name=\"info:stations\"/>\r\n");
        sb.Append("  <capability name=\"info:streams\"/>\r\n");
        sb.Append("  <capability name=\"info:connections\"/>\r\n");
        sb.Append("</seedlink>\r\n");
        return sb.ToString();
    }

    /// <summary>
    /// Build INFO ID XML response (minimal server identification).
    /// </summary>
    private static string BuildInfoIdXml()
    {
        var config = RingServer.Config.ServerConfig.Instance;
        string software = Constants.SlServerId;
        string org = config.ServerId ?? "RingServer";
        string started = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");
        return $"<?xml version=\"1.0\"?>\r\n<seedlink software=\"{EscapeXml(software)}\" organization=\"{EscapeXml(org)}\" started=\"{started}\"/>\r\n";
    }

    /// <summary>
    /// Build INFO STREAMS/STATIONS XML response.
    /// Format matches C original info_xml_slv3_stations().
    /// </summary>
    private static string BuildInfoStreamsXml(ClientInfo cinfo, bool includeStreams)
    {
        var config = RingServer.Config.ServerConfig.Instance;
        string software = Constants.SlServerId;
        string org = config.ServerId ?? "RingServer";
        string started = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");

        var ringBuffer = new RingBuffer(cinfo.RingParams);
        var streamsStack = ringBuffer.GetStreamsStack(null);

        // Group streams by station
        // Stream ID format: FDSN:NET_STA_LOC_B_A_C/MSEED  (FDSN source ID)
        // or legacy:        NET_STA_LOC_CHA
        var stations = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<RingStream>>();

        while (streamsStack.NotEmpty)
        {
            var stream = (RingStream)streamsStack.Pop()!;
            string sid = stream.StreamId.Trim('\0', ' ');

            // Parse NET_STA from stream ID
            string stationKey = ParseStationKey(sid);
            if (!stations.TryGetValue(stationKey, out var list))
            {
                list = new System.Collections.Generic.List<RingStream>();
                stations[stationKey] = list;
            }
            list.Add(stream);
        }

        var sb = new StringBuilder();
        sb.Append("<?xml version=\"1.0\"?>\r\n");
        sb.Append($"<seedlink software=\"{EscapeXml(software)}\" organization=\"{EscapeXml(org)}\" started=\"{started}\">\r\n");

        string? targetStation = (cinfo.ExtInfo is SLExtInfo slext) ? slext.CurrentStation : null;

        foreach (var kvp in stations)
        {
            // stationKey = "NET_STA"
            string stationKey = kvp.Key;

            if (!string.IsNullOrEmpty(targetStation) && !stationKey.Equals(targetStation, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            string net = "", sta = stationKey;
            int us = stationKey.IndexOf('_');
            if (us >= 0) { net = stationKey[..us]; sta = stationKey[(us + 1)..]; }

            ulong beginSeq = ulong.MaxValue, endSeq = 0;
            foreach (var s in kvp.Value)
            {
                if (s.EarliestId < beginSeq) beginSeq = s.EarliestId;
                if (s.LatestId > endSeq) endSeq = s.LatestId;
            }
            if (beginSeq == ulong.MaxValue) beginSeq = 0;

            sb.Append($"  <station name=\"{EscapeXml(sta)}\" network=\"{EscapeXml(net)}\" description=\"Station ID {EscapeXml(stationKey)}\" begin_seq=\"{beginSeq}\" end_seq=\"{endSeq}\" stream_check=\"enabled\">\r\n");

            if (includeStreams)
            {
                foreach (var s in kvp.Value)
                {
                    // Parse location + seedname from stream ID
                    ParseStreamComponents(s.StreamId.Trim('\0', ' '), out string loc, out string seedname);

                    // Format begin/end times as ISO8601 with microseconds (matching C original)
                    string beginTime = (s.EarliestDsTime.IsUnset || s.EarliestDsTime.Value == 0) ? "" : s.EarliestDsTime.ToIsoString();
                    string endTime = (s.LatestDeTime.IsUnset || s.LatestDeTime.Value == 0) ? "" : s.LatestDeTime.ToIsoString();

                    sb.Append($"    <stream location=\"{EscapeXml(loc)}\" seedname=\"{EscapeXml(seedname)}\" type=\"D\" begin_time=\"{EscapeXml(beginTime)}\" end_time=\"{EscapeXml(endTime)}\"/>\r\n");
                }
            }

            sb.Append("  </station>\r\n");
        }

        sb.Append("</seedlink>\r\n");
        return sb.ToString();
    }

    /// <summary>
    /// Build INFO CONNECTIONS XML response.
    /// Equivalent to info_xml_slv3_connections() in infoxml.c.
    /// Each connected client is wrapped in a &lt;station&gt; element with a &lt;connection&gt; child,
    /// matching SeedLink v3 XML schema for compatibility with slinktool.
    /// </summary>
    private static string BuildInfoConnectionsXml(ClientInfo cinfo)
    {
        var config = RingServer.Config.ServerConfig.Instance;
        string software = Constants.SlServerId;
        string org = config.ServerId ?? "RingServer";
        string started = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");

        var sb = new StringBuilder();
        sb.Append("<?xml version=\"1.0\"?>\r\n");
        sb.Append($"<seedlink software=\"{EscapeXml(software)}\" organization=\"{EscapeXml(org)}\" started=\"{started}\">\r\n");

        lock (ServerConfig.Params.CthreadsLock)
        {
            var curr = ServerConfig.Params.Cthreads;
            while (curr != null)
            {
                if (curr.Td?.PrivatePtr is ClientInfo tc && curr.Td.State == RingServer.Types.ThreadState.Active)
                {
                    // Determine network code from connection type (matching C original)
                    string network = tc.Type switch
                    {
                        ClientType.DataLink => "DL",
                        ClientType.SeedLink => "SL",
                        ClientType.Http => "HP",
                        _ => "??"
                    };

                    sb.Append($"  <station name=\"CLIENT\" network=\"{network}\" description=\"Ringserver Client\" begin_seq=\"0\" end_seq=\"0\" stream_check=\"enabled\">\r\n");

                    // Build connection element
                    string ctime = tc.ConnTime.IsUnset ? "" : tc.ConnTime.ToIsoString();
                    string pktid = (tc.Reader?.PktId > 0 && tc.Reader.PktId <= Constants.RingIdMaximum) ? tc.Reader.PktId.ToString() : "0";
                    string txcount = tc.TxPackets[0].ToString();
                    string txbytes = tc.TxBytes[0].ToString();

                    sb.Append($"    <connection host=\"{EscapeXml(tc.Hostname)}\"");
                    sb.Append($" port=\"{EscapeXml(tc.PortStr)}\"");
                    sb.Append($" ctime=\"{EscapeXml(ctime)}\"");
                    sb.Append($" begin_seq=\"0\"");
                    sb.Append($" current_seq=\"{pktid}\"");
                    sb.Append($" sequence_gaps=\"0\"");
                    sb.Append($" txcount=\"{txcount}\"");
                    sb.Append($" totBytes=\"{txbytes}\"");
                    sb.Append($" begin_seq_valid=\"yes\"");
                    sb.Append($" realtime=\"yes\"");
                    sb.Append($" end_of_data=\"no\"/>");

                    sb.Append("\r\n  </station>\r\n");
                }
                curr = curr.Next;
            }
        }

        sb.Append("</seedlink>\r\n");
        return sb.ToString();
    }

    /// <summary>
    /// Parse station key "NET_STA" from FDSN stream ID.
    /// e.g. "FDSN:VG_STNM0_00_E_H_Z/MSEED" → "VG_STNM0"
    /// </summary>
    private static string ParseStationKey(string sid)
    {
        // Strip "FDSN:" prefix
        if (sid.StartsWith("FDSN:", StringComparison.OrdinalIgnoreCase))
            sid = sid[5..];
        // Strip "/MSEED" or similar suffix
        int slash = sid.IndexOf('/');
        if (slash >= 0) sid = sid[..slash];

        // Format: NET_STA_LOC_B_A_C
        // First two underscore-separated parts = NET_STA
        int first = sid.IndexOf('_');
        if (first < 0) return sid;
        int second = sid.IndexOf('_', first + 1);
        if (second < 0) return sid;
        return sid[..second]; // "NET_STA"
    }

    /// <summary>
    /// Parse location code and 3-char seedname from FDSN stream ID.
    /// "FDSN:VG_STNM0_00_E_H_Z/MSEED" → loc="00", seedname="EHZ"
    /// </summary>
    private static void ParseStreamComponents(string sid, out string loc, out string seedname)
    {
        loc = "  ";
        seedname = "   ";

        if (sid.StartsWith("FDSN:", StringComparison.OrdinalIgnoreCase))
            sid = sid[5..];
        int slash = sid.IndexOf('/');
        if (slash >= 0) sid = sid[..slash];

        // NET_STA_LOC_B_A_C
        var parts = sid.Split('_');
        // parts[0]=NET, [1]=STA, [2]=LOC, [3]=B, [4]=A, [5]=C
        if (parts.Length >= 3) loc = parts[2];
        if (parts.Length >= 6)
            seedname = $"{parts[3]}{parts[4]}{parts[5]}";
        else if (parts.Length == 5)
            seedname = $"{parts[2]}{parts[3]}{parts[4]}";
    }

    private static string EscapeXml(string s)
    {
        return s.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;")
                .Replace("\"", "&quot;").Replace("'", "&apos;");
    }

    /// <summary>
    /// Pack XML string into miniSEED 512-byte INFO records and send to client.
    /// Equivalent to the miniSEED packing loop in HandleInfo_v3() in C original.
    /// Each record is prefixed with "SL" + 6-hex-digit sequence (SeedLink framing).
    /// Station="INFO ", Network="XX", Channel="INF" (or "ERR" if errflag).
    /// </summary>
    private static void SendInfoMseed(ClientInfo cinfo, string xmlStr, bool errflag)
    {
        const int RecordSize = 512;
        const int Blkt1000Offset = 48;   // Offset of blockette 1000 in record
        const int DataOffset = 56;       // Where XML data starts in record
        const int MaxDataPerRecord = RecordSize - DataOffset; // 456 bytes

        byte[] xmlBytes = Encoding.ASCII.GetBytes(xmlStr);
        int xmlLen = xmlBytes.Length;

        // Current UTC time components for FSDH
        var now = DateTime.UtcNow;
        ushort year = (ushort)now.Year;
        ushort yday = (ushort)now.DayOfYear;
        byte hour = (byte)now.Hour;
        byte min = (byte)now.Minute;
        byte sec = (byte)now.Second;

        int seqnum = 1;
        int offset = 0;
        bool lastRecord = false;

        while (offset < xmlLen || (offset == 0 && xmlLen == 0))
        {
            int nsamps = Math.Min(xmlLen - offset, MaxDataPerRecord);
            lastRecord = (offset + nsamps >= xmlLen);

            byte[] record = new byte[RecordSize]; // zero-filled

            // FSDH: sequence number (6 ASCII digits at offset 0)
            byte[] seqBytes = Encoding.ASCII.GetBytes($"{seqnum:D6}");
            Array.Copy(seqBytes, 0, record, 0, 6);

            record[6] = (byte)'D';          // Data quality
            record[7] = (byte)' ';          // Reserved
            // Station (5 chars, space-padded): "INFO "
            Encoding.ASCII.GetBytes("INFO ").CopyTo(record, 8);
            // Location (2 chars): "  "
            record[13] = (byte)' '; record[14] = (byte)' ';
            // Channel (3 chars): "INF" or "ERR"
            Encoding.ASCII.GetBytes(errflag ? "ERR" : "INF").CopyTo(record, 15);
            // Network (2 chars): "XX"
            Encoding.ASCII.GetBytes("XX").CopyTo(record, 18);
            // Year (big-endian uint16)
            WriteBEUInt16(record, 20, year);
            // Day of year (big-endian uint16)
            WriteBEUInt16(record, 22, yday);
            // Hour, min, sec
            record[24] = hour; record[25] = min; record[26] = sec;
            record[27] = 0; // unused
            WriteBEUInt16(record, 28, 0); // 0.0001 sec frac
            // Number of samples (big-endian uint16)
            WriteBEUInt16(record, 30, (ushort)nsamps);
            // Sample rate factor = 0, multiplier = 0 (text encoding, no sample rate)
            WriteBEInt16(record, 32, 0);
            WriteBEInt16(record, 34, 0);
            // Activity, IO, DQ flags
            record[36] = 0; record[37] = 0; record[38] = 0;
            // Number of blockettes
            record[39] = 1;
            // Time correction (int32, BE)
            WriteBEInt32(record, 40, 0);
            // Data offset (uint16, BE): 56
            WriteBEUInt16(record, 44, DataOffset);
            // First blockette offset (uint16, BE): 48
            WriteBEUInt16(record, 46, Blkt1000Offset);

            // Blockette 1000 at offset 48
            WriteBEUInt16(record, 48, 1000);   // blockette type
            WriteBEUInt16(record, 50, 0);       // next blockette (0 = none)
            record[52] = 0;  // encoding = 0 (text/ASCII)
            record[53] = 1;  // byte order = 1 (big endian)
            record[54] = 9;  // record length = 2^9 = 512
            record[55] = 0;  // reserved

            // Copy XML data into record at DataOffset
            if (nsamps > 0)
                Array.Copy(xmlBytes, offset, record, DataOffset, nsamps);
            // Remaining bytes are already 0 (NUL padding)

            // SeedLink framing for INFO records: "SLINFO *" (more records follow) or "SLINFO  " (last record)
            string slHeader = lastRecord ? "SLINFO  " : "SLINFO *";
            byte[] header = Encoding.ASCII.GetBytes(slHeader); // 8 bytes

            int result = SendData.SendBuffersToClient(
                cinfo,
                [header, record],
                [header.Length, RecordSize],
                2,
                false);

            if (result < 0)
                return;

            seqnum++;
            offset += nsamps;

            if (lastRecord) break;
        }
    }

    private static void WriteBEUInt16(byte[] buf, int off, ushort val)
    {
        buf[off]     = (byte)(val >> 8);
        buf[off + 1] = (byte)(val & 0xFF);
    }
    private static void WriteBEInt16(byte[] buf, int off, short val)
    {
        buf[off]     = (byte)(val >> 8);
        buf[off + 1] = (byte)(val & 0xFF);
    }
    private static void WriteBEInt32(byte[] buf, int off, int val)
    {
        buf[off]     = (byte)(val >> 24);
        buf[off + 1] = (byte)(val >> 16);
        buf[off + 2] = (byte)(val >> 8);
        buf[off + 3] = (byte)(val & 0xFF);
    }

    /// <summary>
    /// Validate that data looks like a miniSEED record.
    /// Equivalent to MS2_ISVALIDHEADER / MS3_ISVALIDHEADER macros in libmseed.
    /// Returns true if the data is a valid miniSEED v2 or v3 header.
    /// </summary>
    private static bool IsValidMiniSeedHeader(byte[] data, int dataLen)
    {
        // Need at least byte 6 for v2 check (quality indicator) or byte 2 for v3
        if (dataLen < 27)
        {
            // MS3 only needs 15 bytes
            if (dataLen >= 15 && data[0] == (byte)'M' && data[1] == (byte)'S' && data[2] == 3)
            {
                return data[12] <= 23 && data[13] <= 59 && data[14] <= 60;
            }
            return false;
        }

        // Check MS3 first (fast check: 'M','S',3)
        if (data[0] == (byte)'M' && data[1] == (byte)'S' && data[2] == 3)
        {
            bool ms3ok = data[12] <= 23 && data[13] <= 59 && data[14] <= 60;
            if (!ms3ok)
            {
                Logging.lprintf(2, "Invalid MS3 header. h:{0}, m:{1}, s:{2}",
                    data[12], data[13], data[14]);
            }
            return ms3ok;
        }

        // Check MS2: offsets 0-5 = digits/spaces/NULL, 6 = quality, 7 = space/NULL
        //           24=hour, 25=min, 26=sec
        bool ms2Check =
            (IsDigitOrSpaceOrNull(data[0])) &&
            (IsDigitOrSpaceOrNull(data[1])) &&
            (IsDigitOrSpaceOrNull(data[2])) &&
            (IsDigitOrSpaceOrNull(data[3])) &&
            (IsDigitOrSpaceOrNull(data[4])) &&
            (IsDigitOrSpaceOrNull(data[5])) &&
            IsSeedLinkDataIndicator((char)data[6]) &&
            (data[7] == (byte)' ' || data[7] == 0) &&
            data[24] <= 23 &&
            data[25] <= 59 &&
            data[26] <= 60;

        if (!ms2Check)
        {
            Logging.lprintf(2, "Invalid MS2 header. b0-5:'{0}{1}{2}{3}{4}{5}' b6:'{6}' b7:{7} h:{8} m:{9} s:{10}",
                (char)data[0], (char)data[1], (char)data[2], (char)data[3], (char)data[4], (char)data[5],
                (char)data[6], data[7], data[24], data[25], data[26]);
        }

        return ms2Check;
    }

    private static bool IsDigitOrSpaceOrNull(byte b)
    {
        return (b >= (byte)'0' && b <= (byte)'9') || b == (byte)' ' || b == 0;
    }

    private static bool IsSeedLinkDataIndicator(char c)
    {
        return c == 'D' || c == 'R' || c == 'Q' || c == 'M';
    }
}