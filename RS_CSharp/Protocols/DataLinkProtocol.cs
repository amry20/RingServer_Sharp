/**************************************************************************
 * DataLinkProtocol.cs
 *
 * DataLink protocol handler.
 * Equivalent to dlclient.c / dlclient.h in the C version.
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
using System.Text;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using RingServer.Net;
using RingServer.Ring;
using RingServer.Types;
using RingServer.Config;
using RingServer.Mseed;
using System.Collections.Generic;

namespace RingServer.Protocols;

public class DLExtInfo
{
    public Regex? LegacyMseedStreamIdMatch;
    /// <summary>Cached RingPacket for StreamPackets (avoid per-call GC allocations).</summary>
    internal RingPacket? CachedPacket;
    /// <summary>Cached packet data buffer for StreamPackets.</summary>
    internal byte[]? CachedPacketData;
    /// <summary>Cached size of the packet data buffer.</summary>
    internal int CachedPacketDataSize;
}

public static class DataLinkProtocol
{
    public static int HandleCommand(ClientInfo cinfo, int lineLen)
    {
        if (cinfo.ExtInfo == null)
        {
            var dlinfo = new DLExtInfo();
            try
            {
                dlinfo.LegacyMseedStreamIdMatch = new Regex(Constants.LegacyMseedStreamIdPattern,
                    RegexOptions.Compiled | RegexOptions.CultureInvariant);
            }
            catch (Exception ex)
            {
                Logging.lprintf(0, "[{0}] Error compiling legacy stream ID pattern: {1}", cinfo.Hostname, ex.Message);
                return -1;
            }
            cinfo.ExtInfo = dlinfo;
        }

        string cmd = Encoding.ASCII.GetString(cinfo.DlCommand).TrimEnd('\0').Trim();
        if (string.IsNullOrEmpty(cmd)) return 0;

        Logging.lprintf(3, "[{0}] DataLink command: {1}", cinfo.Hostname, cmd);

        string[] parts = cmd.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0) return 0;

        string command = parts[0].ToUpperInvariant();

        if (command == "WRITE")
        {
            if (!cinfo.WritePerm)
            {
                Logging.lprintf(1, "[{0}] Data packet received from client without write permission", cinfo.Hostname);
                SendPacket(cinfo, "ERROR", "Write permission not granted, no soup for you!", 0, false, false);
                return -1;
            }
            return HandleWrite(cinfo, parts, cmd);
        }
        else if (command == "INFO")
        {
            return HandleInfo(cinfo, parts);
        }
        else if (command == "READ")
        {
            cinfo.State = ClientState.Command;
            return HandleRead(cinfo, parts);
        }
        else if (command == "STREAM")
        {
            if (cinfo.Reader!.PktId == Constants.RingIdNone)
                cinfo.Reader.PktId = Constants.RingIdNext;
            cinfo.State = ClientState.Stream;
            return 0;
        }
        else if (command == "ENDSTREAM")
        {
            if (SendPacket(cinfo, "ENDSTREAM", (string?)null, 0, false, false) != 0) return -1;
            cinfo.State = ClientState.Command;
            return 0;
        }
        else
        {
            if (command != "ID") cinfo.State = ClientState.Command;
            return HandleNegotiation(cinfo, command, parts, cmd);
        }
    }

    private static int HandleNegotiation(ClientInfo cinfo, string command, string[] parts, string cmd)
    {
        if (command == "ID")
        {
            if (parts.Length > 1)
            {
                int idIdx = cmd.IndexOf(parts[1]);
                if (idIdx > 0)
                {
                    cinfo.ClientId = cmd[idIdx..].Trim();
                    if (cinfo.ClientId.Length > 255) cinfo.ClientId = cinfo.ClientId[..255];
                    Logging.lprintf(2, "[{0}] Received ID ({1})", cinfo.Hostname, cinfo.ClientId);
                }
            }
            else
            {
                Logging.lprintf(2, "[{0}] Received ID", cinfo.Hostname);
            }

            ulong pktSize = cinfo.RingParams.PktSize - RingPacket.SerializedSize;
            string cap = $"PACKETSIZE:{pktSize}{(cinfo.WritePerm ? " WRITE" : "")}";
            return SendPacket(cinfo, $"ID {Constants.DlServerId} {cap}", (string?)null, 0, false, false);
        }
        else if (command == "POSITION") return HandlePosition(cinfo, parts);
        else if (command == "MATCH") return HandleMatch(cinfo, parts, cmd);
        else if (command == "REJECT") return HandleReject(cinfo, parts, cmd);
        else if (command == "BYE")
        {
            Logging.lprintf(2, "[{0}] Received BYE", cinfo.Hostname);
            return 1;
        }
        
        Logging.lprintf(0, "[{0}] Unknown DataLink command: {1}", cinfo.Hostname, command);
        SendPacket(cinfo, "ERROR", "Unknown command", 0, false, false);
        return -1;
    }

    private static int HandlePosition(ClientInfo cinfo, string[] parts)
    {
        if (parts.Length < 3 || parts.Length > 4)
        {
            SendPacket(cinfo, "ERROR", "POSITION requires 2 or 3 arguments", 0, false, false);
            return -1;
        }

        string subcmd = parts[1].ToUpperInvariant();
        string valueStr = parts[2];
        ulong pktid = 0;
        NsTime nstime = NsTime.Unset;
        RingBuffer ringBuffer = new RingBuffer(cinfo.RingParams);

        if (subcmd == "SET")
        {
            if (valueStr.ToUpperInvariant() == "EARLIEST") pktid = Constants.RingIdEarliest;
            else if (valueStr.ToUpperInvariant() == "LATEST") pktid = Constants.RingIdLatest;
            else if (ulong.TryParse(valueStr, out ulong parsedId))
            {
                pktid = parsedId;
                if (parts.Length == 4 && long.TryParse(parts[3], out long ht))
                    nstime = new NsTime(ht * 1000);
            }
            else
            {
                Logging.lprintf(0, "[{0}] Error with POSITION SET value: {1}", cinfo.Hostname, valueStr);
                SendPacket(cinfo, "ERROR", "Error with POSITION SET value", 0, false, false);
                return -1;
            }

            pktid = ringBuffer.Position(cinfo.Reader!, pktid, nstime);
            if (pktid == Constants.RingIdError)
            {
                SendPacket(cinfo, "ERROR", "Error positioning reader", 0, false, false);
                return -1;
            }
            if (pktid == Constants.RingIdNone)
            {
                SendPacket(cinfo, "ERROR", "Packet not found", 0, false, false);
                return -1;
            }
            return SendPacket(cinfo, "OK", $"Positioned to packet ID {pktid}", pktid, true, true);
        }
        else if (subcmd == "AFTER" || subcmd == "AFTERREV")
        {
            if (!long.TryParse(valueStr, out long ht))
            {
                SendPacket(cinfo, "ERROR", "Error with POSITION AFTER time", 0, false, false);
                return -1;
            }
            nstime = new NsTime(ht * 1000);

            if (cinfo.TimeWinLimit == 1.0f)
            {
                pktid = subcmd == "AFTERREV" ? ringBuffer.AfterRev(cinfo.Reader!, nstime, 0, 1) : ringBuffer.After(cinfo.Reader!, nstime, 1);
            }
            else if (cinfo.TimeWinLimit < 1.0f)
            {
                ulong limit = (ulong)(cinfo.TimeWinLimit * cinfo.RingParams.MaxPackets);
                pktid = ringBuffer.AfterRev(cinfo.Reader!, nstime, limit, 1);
            }
            else
            {
                SendPacket(cinfo, "ERROR", "time window search limit is invalid", 0, false, false);
                return -1;
            }

            if (pktid == Constants.RingIdError)
            {
                SendPacket(cinfo, "ERROR", "Error positioning reader", 0, false, false);
                return -1;
            }
            if (pktid == Constants.RingIdNone)
            {
                SendPacket(cinfo, "ERROR", "Packet not found", 0, false, false);
                return -1;
            }
            return SendPacket(cinfo, "OK", $"Positioned to packet ID {pktid}", pktid, true, true);
        }

        SendPacket(cinfo, "ERROR", "Unsupported POSITION subcommand", 0, false, false);
        return -1;
    }
    private static int HandleMatch(ClientInfo cinfo, string[] parts, string cmd)
    {
        if (parts.Length < 2) return -1;
        if (!int.TryParse(parts[1], out int size)) return -1;

        if (size == 0)
        {
            cinfo.MatchStr = null;
            RingBuffer.UpdatePattern(ref cinfo.Reader!.Match, null, "match");
            var rb = new RingBuffer(cinfo.RingParams);
            int count = rb.GetStreamsStack(cinfo.Reader).Count;
            return SendPacket(cinfo, "OK", $"{count} streams selected after match", (ulong)count, true, true);
        }

        if (size > Constants.DlMaxRegexLen)
        {
            SendPacket(cinfo, "ERROR", $"match expression too large, must be <= {Constants.DlMaxRegexLen}", 0, true, true);
            return -1;
        }

        byte[] regexBytes = new byte[size];
        if (RecvData(cinfo, regexBytes, 0, size) < 0) return -1;

        cinfo.MatchStr = Encoding.UTF8.GetString(regexBytes);
        if (!RingBuffer.UpdatePattern(ref cinfo.Reader!.Match, cinfo.MatchStr, "match"))
        {
            SendPacket(cinfo, "ERROR", "Error with match expression", 0, false, false);
            return -1;
        }

        var rbs = new RingBuffer(cinfo.RingParams);
        int c = rbs.GetStreamsStack(cinfo.Reader).Count;
        return SendPacket(cinfo, "OK", $"{c} streams selected after match", (ulong)c, true, true);
    }

    private static int HandleReject(ClientInfo cinfo, string[] parts, string cmd)
    {
        if (parts.Length < 2) return -1;
        if (!int.TryParse(parts[1], out int size)) return -1;

        if (size == 0)
        {
            cinfo.RejectStr = null;
            RingBuffer.UpdatePattern(ref cinfo.Reader!.Reject, null, "reject");
            var rb = new RingBuffer(cinfo.RingParams);
            int count = rb.GetStreamsStack(cinfo.Reader).Count;
            return SendPacket(cinfo, "OK", $"{count} streams selected after reject", (ulong)count, true, true);
        }

        if (size > Constants.DlMaxRegexLen)
        {
            SendPacket(cinfo, "ERROR", $"reject expression too large, must be <= {Constants.DlMaxRegexLen}", 0, true, true);
            return -1;
        }

        byte[] regexBytes = new byte[size];
        if (RecvData(cinfo, regexBytes, 0, size) < 0) return -1;

        cinfo.RejectStr = Encoding.UTF8.GetString(regexBytes);
        if (!RingBuffer.UpdatePattern(ref cinfo.Reader!.Reject, cinfo.RejectStr, "reject"))
        {
            SendPacket(cinfo, "ERROR", "Error with reject expression", 0, false, false);
            return -1;
        }

        var rbs = new RingBuffer(cinfo.RingParams);
        int c = rbs.GetStreamsStack(cinfo.Reader).Count;
        return SendPacket(cinfo, "OK", $"{c} streams selected after reject", (ulong)c, true, true);
    }

    private static int HandleRead(ClientInfo cinfo, string[] parts)
    {
        if (parts.Length < 2)
        {
            SendPacket(cinfo, "ERROR", "READ requires a single pktid argument", 0, false, false);
            return -1;
        }
        if (!ulong.TryParse(parts[1], out ulong pktid)) return -1;

        var rb = new RingBuffer(cinfo.RingParams);
        var p = new RingPacket();
        byte[] db = new byte[cinfo.RingParams.PktSize - RingPacket.SerializedSize];
        ulong readId = rb.ReadNext(cinfo.Reader!, p, db);

        if (readId == Constants.RingIdError)
        {
            SendPacket(cinfo, "ERROR", "Error reading packet from ring", 0, false, false);
            return -1;
        }
        if (readId == Constants.RingIdNone)
        {
            SendPacket(cinfo, "ERROR", "Packet not found", 0, false, false);
            return -1;
        }

        int ts = RingPacket.SerializedSize + (int)p.DataSize;
        byte[] op = new byte[ts];
        p.Serialize(op.AsSpan());
        Buffer.BlockCopy(db, 0, op, RingPacket.SerializedSize, (int)p.DataSize);

        cinfo.TxPackets[0]++;
        cinfo.TxBytes[0] += (ulong)ts;
        if (cinfo.Streams != null && !string.IsNullOrEmpty(p.StreamId))
        {
            lock (cinfo.StreamsLock)
            {
                var n = cinfo.Streams.Find(p.StreamId);
                if (n != null)
                {
                    var sn = (StreamNode)n.Data!;
                    sn.TxPackets++;
                    sn.TxBytes += (ulong)ts;
                }
            }
        }
        return SendPacket(cinfo, $"PACKET {p.StreamId} {p.PktId} {p.PktTime.Value / 1000} {p.DataStart.Value / 1000} {p.DataEnd.Value / 1000} {p.DataSize}", op, 0, false, false);
    }

    private static int HandleInfo(ClientInfo cinfo, string[] parts)
    {
        if (parts.Length < 2) return -1;
        string type = parts[1].ToUpperInvariant();
        string? matchexpr = null;
        if (parts.Length > 2)
        {
            int mIdx = 2;
            matchexpr = string.Join(" ", parts, mIdx, parts.Length - mIdx);
        }

        if (type == "STATUS" || type == "STREAMS")
        {
            string xml = GenerateInfoXml(cinfo, type, matchexpr);
            return SendPacket(cinfo, $"INFO {type}", xml, 0, false, true);
        }
        else if (type == "CONNECTIONS")
        {
            if (!cinfo.Trusted)
            {
                SendPacket(cinfo, "ERROR", "Access to CONNECTIONS denied", 0, true, true);
                return -1;
            }
            string xml = GenerateInfoXml(cinfo, type, matchexpr);
            return SendPacket(cinfo, $"INFO {type}", xml, 0, false, true);
        }
        SendPacket(cinfo, "ERROR", $"Unrecognized INFO request type: {type}", 0, true, true);
        return -1;
    }

    private static string GenerateInfoXml(ClientInfo cinfo, string type, string? matchexpr)
    {
        var rp = cinfo.RingParams;
        string sid = ServerConfig.Instance.ServerId ?? "Ring Server";
        ulong pktSize = rp.PktSize - RingPacket.SerializedSize;
        string caps = $"{Constants.DlCapabilitiesId} PACKETSIZE:{pktSize}{(cinfo.WritePerm ? " WRITE" : "")}";
        
        var sb = new StringBuilder();
        sb.Append($"<DataLink Version=\"{Constants.DlServerVer}\" ServerID=\"{sid}\" Capabilities=\"{caps}\">");
        sb.Append($"<Status StartTime=\"{Iso(ServerConfig.Params.ServerStartTime)}\" RingVersion=\"{Constants.RingVersion}\" RingSize=\"{rp.RingSize}\" PacketSize=\"{rp.PktSize}\" MaximumPackets=\"{rp.MaxPackets}\" MemoryMappedRing=\"{(rp.MmapFlag > 0 ? "TRUE" : "FALSE")}\" VolatileRing=\"{(rp.VolatileFlag > 0 ? "TRUE" : "FALSE")}\" TotalConnections=\"{ServerConfig.Params.ClientCount}\" TotalStreams=\"{rp.StreamCount}\" TXPacketRate=\"{rp.TxPacketRate:F1}\" TXByteRate=\"{rp.TxByteRate:F1}\" RXPacketRate=\"{rp.RxPacketRate:F1}\" RXByteRate=\"{rp.RxByteRate:F1}\" EarliestPacketID=\"{rp.EarliestId}\" LatestPacketID=\"{rp.LatestId}\" EarliestPacketCreationTime=\"{Iso(rp.EarliestPTime)}\" EarliestPacketDataStartTime=\"{Iso(rp.EarliestDsTime)}\" EarliestPacketDataEndTime=\"{Iso(rp.EarliestDeTime)}\" LatestPacketCreationTime=\"{Iso(rp.LatestPTime)}\" LatestPacketDataStartTime=\"{Iso(rp.LatestDsTime)}\" LatestPacketDataEndTime=\"{Iso(rp.LatestDeTime)}\"/>");

        if (type == "STATUS" && cinfo.Trusted)
        {
            int thCnt = 0;
            var tsb = new StringBuilder();
            lock (ServerConfig.Params.SthreadsLock)
            {
                var curr = ServerConfig.Params.Sthreads;
                while (curr != null)
                {
                    thCnt++;
                    string state = curr.Td?.State.ToString().ToUpperInvariant() ?? "UNKNOWN";
                    if (curr.Type == ServerThreadType.Listen && curr.Params is ListenPortParams lpp)
                    {
                        string proto = SendData.GenProtocolString(lpp.Protocols, lpp.Options);
                        tsb.Append($"<Thread Flags=\"{state}\" Type=\"{proto}\" Port=\"{lpp.PortStr}\"/>");
                    }
                    else if (curr.Type == ServerThreadType.MSeedScan)
                    {
                        tsb.Append($"<Thread Flags=\"{state}\" Type=\"miniSEED scanner\"/>");
                    }
                    else tsb.Append($"<Thread Flags=\"{state}\" Type=\"Unknown Thread\"/>");
                    curr = curr.Next;
                }
            }
            sb.Append($"<ServerThreads TotalServerThreads=\"{thCnt}\">{tsb}</ServerThreads>");
        }
        else if (type == "STREAMS")
        {
            Regex? m = null;
            if (!string.IsNullOrEmpty(matchexpr))
            {
                try { m = new Regex(matchexpr, RegexOptions.Compiled | RegexOptions.CultureInvariant); } catch { }
            }
            var rb = new RingBuffer(rp);
            var stk = rb.GetStreamsStack(null);
            int scnt = 0;
            var ssb = new StringBuilder();
            while (stk.NotEmpty)
            {
                var st = (RingStream)stk.Pop()!;
                if (m != null && !m.IsMatch(st.StreamId)) continue;
                scnt++;
                double lat = st.LatestDeTime.IsUnset ? 0.0 : (Generic.NSnow().Value - st.LatestDeTime.Value) / (double)Constants.NsModulus;
                ssb.Append($"<Stream Name=\"{st.StreamId}\" EarliestPacketID=\"{st.EarliestId}\" EarliestPacketDataStartTime=\"{Iso(st.EarliestDsTime)}\" LatestPacketID=\"{st.LatestId}\" LatestPacketDataStartTime=\"{Iso(st.LatestDeTime)}\" LatestPacketDataEndTime=\"{Iso(st.LatestDeTime)}\" DataLatency=\"{lat:F1}\"/>");
            }
            sb.Append($"<StreamList TotalStreams=\"{rp.StreamCount}\" SelectedStreams=\"{scnt}\">{ssb}</StreamList>");
        }
        else if (type == "CONNECTIONS")
        {
            Regex? m = null;
            if (!string.IsNullOrEmpty(matchexpr))
            {
                try { m = new Regex(matchexpr, RegexOptions.Compiled | RegexOptions.CultureInvariant); } catch { }
            }
            int ccnt = 0;
            var csb = new StringBuilder();
            lock (ServerConfig.Params.CthreadsLock)
            {
                var curr = ServerConfig.Params.Cthreads;
                while (curr != null)
                {
                    if (curr.Td?.PrivatePtr is ClientInfo tc)
                    {
                        if (m != null && !m.IsMatch(tc.Hostname) && !m.IsMatch(tc.IpStr) && !m.IsMatch(tc.ClientId))
                        {
                            curr = curr.Next; continue;
                        }
                        ccnt++;
                        string ct = tc.Type switch {
                            ClientType.DataLink => tc.Tls ? "DataLink:TLS" : "DataLink",
                            ClientType.SeedLink => tc.Tls ? "SeedLink:TLS" : "SeedLink",
                            ClientType.Http => tc.Tls ? "HTTPS" : "HTTP",
                            _ => "Unknown"
                        };
                        csb.Append($"<Connection Type=\"{ct}\" Host=\"{tc.Hostname}\" IP=\"{tc.IpStr}\" Port=\"{tc.PortStr}\" ClientID=\"{tc.ClientId}\" ConnectionTime=\"{Iso(tc.ConnTime)}\" Match=\"{tc.MatchStr ?? "-"}\" Reject=\"{tc.RejectStr ?? "-"}\" StreamCount=\"{tc.StreamsCount}\" PacketID=\"{tc.Reader?.PktId ?? 0}\" PacketDataStartTime=\"-\" PacketCreationTime=\"{(tc.Reader?.PktTime.IsUnset == false ? Iso(tc.Reader.PktTime) : "-")}\" TXPacketCount=\"{tc.TxPackets[0]}\" TXPacketRate=\"{tc.TxPacketRate:F1}\" TXByteCount=\"{tc.TxBytes[0]}\" TXByteRate=\"{tc.TxByteRate:F1}\" RXPacketCount=\"{tc.RxPackets[0]}\" RXPacketRate=\"{tc.RxPacketRate:F1}\" RXByteCount=\"{tc.RxBytes[0]}\" RXByteRate=\"{tc.RxByteRate:F1}\"/>");
                    }
                    curr = curr.Next;
                }
            }
            sb.Append($"<ConnectionList>{csb}</ConnectionList>");
        }

        sb.Append("</DataLink>");
        return sb.ToString();
    }

    private static string Iso(NsTime t) => t.IsUnset ? "-" : t.ToIsoString();

    private static int HandleWrite(ClientInfo cinfo, string[] parts, string cmd)
    {
        if (parts.Length < 6)
        {
            SendPacket(cinfo, "ERROR", "Error parsing WRITE command parameters", 0, true, true);
            return -1;
        }

        string streamid = parts[1];
        if (streamid.Length > Constants.MaxStreamId - 1)
        {
            SendPacket(cinfo, "ERROR", "Error, stream ID too long", 0, true, true);
            return -1;
        }

        if (!long.TryParse(parts[2], out long dsHt) || !long.TryParse(parts[3], out long deHt) || !uint.TryParse(parts[5], out uint dSize))
        {
            SendPacket(cinfo, "ERROR", "Error parsing WRITE command parameters", 0, true, true);
            return -1;
        }

        string flags = parts[4];
        ulong pktid = Constants.RingIdNone;
        if (parts.Length >= 7 && flags.Contains('I')) ulong.TryParse(parts[6], out pktid);

        if (cinfo.ExtInfo is DLExtInfo dl && dl.LegacyMseedStreamIdMatch != null && dl.LegacyMseedStreamIdMatch.IsMatch(streamid))
        {
            int lu = streamid.LastIndexOf('_');
            if (lu >= 0 && lu + 4 < streamid.Length)
                streamid = $"FDSN:{streamid[..lu]}_{streamid[lu + 1]}_{streamid[lu + 2]}_{streamid[lu + 3]}{streamid[(lu + 4)..]}";
        }

        if (cinfo.Reader?.Limit != null && !cinfo.Reader.Limit.IsMatch(streamid))
        {
            SendPacket(cinfo, "ERROR", $"Error, permission denied for WRITE of stream ID: {streamid}", 0, true, true);
            return -1;
        }

        if (RingPacket.SerializedSize + dSize > cinfo.RingParams.PktSize)
        {
            SendPacket(cinfo, "ERROR", $"Packet size ({dSize}) is too large for ring, maximum is {cinfo.RingParams.PktSize - RingPacket.SerializedSize} bytes", 0, true, true);
            return -1;
        }

        byte[] db = new byte[dSize];
        if (RecvData(cinfo, db, 0, (int)dSize) < 0) return -1;

        var p = new RingPacket
        {
            StreamId = streamid,
            PktId = pktid,
            DataStart = new NsTime(dsHt * 1000),
            DataEnd = new NsTime(deHt * 1000),
            DataSize = dSize
        };

        var rb = new RingBuffer(cinfo.RingParams);
        int rv = rb.Write(p, db, dSize);
        if (rv != 0)
        {
            SendPacket(cinfo, "ERROR", "Error adding packet to ring", 0, true, true);
            return -1;
        }

        // Archive to PostgreSQL if configured
        var archive = ServerConfig.Archive;
        if (archive != null)
        {
            var mseedRecord = new MseedRecord
            {
                StreamId = streamid,
                StartTime = new NsTime(dsHt * 1000),
                EndTime = new NsTime(deHt * 1000),
                SampleRate = 0f, // Extract from miniSEED header later if needed
                DataSize = dSize,
                RawData = db,
                PktId = p.PktId
            };

            // Non-blocking enqueue for batch write
            archive.Enqueue(mseedRecord);
        }

        if (cinfo.Streams != null)
        {
            lock (cinfo.StreamsLock)
            {
                var n = cinfo.Streams.Find(streamid);
                StreamNode sn;
                if (n == null)
                {
                    sn = new StreamNode { StreamId = streamid };
                    cinfo.Streams.Insert(streamid, sn);
                    cinfo.StreamsCount++;
                }
                else sn = (StreamNode)n.Data!;
                sn.RxPackets++;
                sn.RxBytes += dSize;
            }
        }
        cinfo.RxPackets[0]++;
        cinfo.RxBytes[0] += dSize;

        if (flags.Contains('A'))
        {
            if (SendPacket(cinfo, "OK", (string?)null, p.PktId, true, true) != 0) return -1;
        }
        return cinfo.SocketErr != 0 ? -1 : 0;
    }

    private static int RecvData(ClientInfo cinfo, byte[] buffer, int offset, int length)
    {
        if (cinfo.Socket == null) return -1;
        int total = 0;
        try
        {
            if (cinfo.RecvLength > 0)
            {
                int cpy = Math.Min(cinfo.RecvLength, length);
                Buffer.BlockCopy(cinfo.RecvBuffer!, 0, buffer, offset, cpy);
                if (cpy < cinfo.RecvLength)
                {
                    int rem = cinfo.RecvLength - cpy;
                    Buffer.BlockCopy(cinfo.RecvBuffer!, cpy, cinfo.RecvBuffer!, 0, rem);
                    cinfo.RecvLength = rem;
                }
                else cinfo.RecvLength = 0;
                total += cpy;
                offset += cpy;
                length -= cpy;
            }
            while (length > 0)
            {
                cinfo.Socket.Blocking = true;
                int n = cinfo.Socket.Receive(buffer, offset, length, SocketFlags.None);
                if (n <= 0)
                {
                    cinfo.SocketErr = -2;
                    return -1;
                }
                total += n;
                offset += n;
                length -= n;
            }
            return total;
        }
        catch
        {
            cinfo.SocketErr = -1;
            return -1;
        }
        finally
        {
            try { cinfo.Socket.Blocking = false; } catch { }
        }
    }

    private static int SendPacket(ClientInfo cinfo, string header, string? data, ulong value, bool addVal, bool addSz)
    {
        byte[]? db = data != null ? Encoding.UTF8.GetBytes(data) : null;
        return SendPacket(cinfo, header, db, value, addVal, addSz);
    }

    private static int SendPacket(ClientInfo cinfo, string header, byte[]? data, ulong value, bool addVal, bool addSz)
    {
        int dlen = data?.Length ?? 0;
        string h = header;
        if (addVal && addSz) h = $"{header} {value} {dlen}";
        else if (addVal) h = $"{header} {value}";
        else if (addSz) h = $"{header} {dlen}";

        byte[] hb = Encoding.ASCII.GetBytes(h);
        if (hb.Length > 255) return -1;

        byte[] wp = new byte[3 + hb.Length + dlen];
        wp[0] = (byte)'D';
        wp[1] = (byte)'L';
        wp[2] = (byte)hb.Length;
        Buffer.BlockCopy(hb, 0, wp, 3, hb.Length);
        if (data != null && dlen > 0) Buffer.BlockCopy(data, 0, wp, 3 + hb.Length, dlen);

        return SendData.SendToClient(cinfo, wp, wp.Length, false);
    }

    public static int StreamPackets(ClientInfo cinfo, RingBuffer ringBuffer)
    {
        if (cinfo.State != ClientState.Stream) return 0;
        int sent = 0;

        // Use cached buffers to avoid per-call GC pressure.
        // Same pattern as SeedLinkProtocol.StreamPackets — see discussion there.
        var dlExt = cinfo.ExtInfo as DLExtInfo;
        int packetDataSize = (int)(cinfo.RingParams.PktSize - RingPacket.SerializedSize);

        RingPacket packet;
        byte[] packetData;

        if (dlExt?.CachedPacket != null && dlExt.CachedPacketData != null &&
            dlExt.CachedPacketDataSize == packetDataSize)
        {
            packet = dlExt.CachedPacket;
            packetData = dlExt.CachedPacketData;
        }
        else
        {
            packet = new RingPacket();
            packetData = new byte[packetDataSize];
            if (dlExt != null)
            {
                dlExt.CachedPacket = packet;
                dlExt.CachedPacketData = packetData;
                dlExt.CachedPacketDataSize = packetDataSize;
            }
        }

        for (int i = 0; i < 10; i++)
        {
            Array.Clear(packetData, 0, packetData.Length);
            ulong readId = ringBuffer.ReadNext(cinfo.Reader!, packet, packetData);
            if (readId == Constants.RingIdError) return -1;
            if (readId == Constants.RingIdNone) break;

            int ts = RingPacket.SerializedSize + (int)packet.DataSize;
            byte[] op = new byte[ts];
            packet.Serialize(op.AsSpan());
            if (packet.DataSize > 0) Buffer.BlockCopy(packetData, 0, op, RingPacket.SerializedSize, (int)packet.DataSize);
            
            int rv = SendPacket(cinfo, $"PACKET {packet.StreamId} {packet.PktId} {packet.PktTime.Value / 1000} {packet.DataStart.Value / 1000} {packet.DataEnd.Value / 1000} {packet.DataSize}", op, 0, false, false);
            if (rv < 0) return rv;
            sent += ts;
        }
        return sent;
    }

    public static void Free(ClientInfo cinfo)
    {
        cinfo.ExtInfo = null;
    }
}
