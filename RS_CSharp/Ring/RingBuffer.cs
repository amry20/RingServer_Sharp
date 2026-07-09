/**************************************************************************
 * RingBuffer.cs
 *
 * Fundamental ring buffer routines. Implements a generic ring buffer
 * with the packet buffer either in memory or as a memory-mapped file.
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
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using RingServer.Types;
using Microsoft.Win32.SafeHandles;

namespace RingServer.Ring;

/// <summary>
/// Core ring buffer operations.
/// Manages a ring buffer of packets with stream index in a red-black tree.
/// </summary>
public class RingBuffer
{
    private readonly RingParams _ringParams;

    public RingParams RingParams => _ringParams;

    public RingBuffer(RingParams ringParams)
    {
        _ringParams = ringParams;
    }

    /// <summary>
    /// Initialize ring files in specified directory either loading and
    /// validating the existing ring files or creating new files.
    /// Returns 0 on success, negative on error.
    /// </summary>
    public static int Initialize(string ringfilename, string streamfilename,
                                  ulong ringsize, uint pktsize,
                                  bool mmapflag, bool volatileflag,
                                  out int ringfd, out RingParams ringParams)
    {
        ringfd = -1;
        ringParams = null!;

        // Sanity check
        if (!volatileflag && (string.IsNullOrEmpty(ringfilename) || string.IsNullOrEmpty(streamfilename)))
        {
            Logging.lprintf(0, "RingInitialize(): ring file and stream file must be specified");
            return -2;
        }

        if (volatileflag)
            mmapflag = false;

        // Determine system page size
        int pagesize = Environment.SystemPageSize;

        // Determine number of pages needed for header
        uint headersize = (uint)pagesize;
        while (headersize < RingPacket.SerializedSize + 128)
            headersize += (uint)pagesize;

        // Adjust headersize to cover RingParams (estimated size of persisted fields)
        int ringParamsSize = 256; // Estimated size of RingParams persisted fields
        uint paramHeadersize = headersize;
        while (paramHeadersize < (uint)ringParamsSize + 64)
            paramHeadersize += (uint)pagesize;

        headersize = Math.Max(headersize, paramHeadersize);

        // Sanity check ring size
        if (ringsize < (headersize + 2 * pktsize))
        {
            Logging.lprintf(0, $"RingInitialize(): ring size ({ringsize}) must be enough for 2 packets ({pktsize} each) and header ({headersize})");
            return -2;
        }

        // Max packets after header
        ulong maxpackets = (ringsize - headersize) / pktsize;
        long maxoffset = (long)((maxpackets - 1) * pktsize);

        // Create RingParams
        ringParams = new RingParams
        {
            RingSize = ringsize,
            PktSize = pktsize,
            MaxPackets = maxpackets,
            MaxOffset = maxoffset,
            HeaderSize = headersize,
            MmapFlag = mmapflag ? (byte)1 : (byte)0,
            VolatileFlag = volatileflag ? (byte)1 : (byte)0,
            StreamIdx = RBTree.CreateStringTree(),
            StreamCount = 0,
            StreamLock = new object(),
            WriteLock = new object(),
            DataOffset = (int)headersize
        };

        // Initialize the data buffer
        if (!volatileflag)
        {
            // Allocate buffer
            ringParams.DataBuffer = new byte[ringsize];

            if (File.Exists(ringfilename))
            {
                var fileInfo = new FileInfo(ringfilename);
                if (fileInfo.Length == (long)ringsize)
                {
                    Logging.lprintf(1, "Recovering existing ring packet buffer file");
                    using var fs = new FileStream(ringfilename, FileMode.Open, FileAccess.Read);
                    fs.ReadExactly(ringParams.DataBuffer, 0, (int)ringsize);
                }
                else
                {
                    Logging.lprintf(1, "Re-creating ring packet buffer file");
                }
            }
            else
            {
                Logging.lprintf(1, "Creating new ring packet buffer file");
            }
            ringfd = 1; // File is managed via FileStream
        }
        else
        {
            ringParams.DataBuffer = new byte[ringsize];
            ringfd = -1;
        }

        // Read header from buffer
        if (ringParams.DataBuffer != null && ringParams.DataBuffer.Length >= headersize)
        {
            var headerSpan = ringParams.DataBuffer.AsSpan(0, (int)headersize);
            ringParams.Signature = headerSpan.Slice(0, 4).ToArray();
            ringParams.Version = BitConverter.ToUInt16(headerSpan.Slice(4, 2));
            ringParams.RingSize = BitConverter.ToUInt64(headerSpan.Slice(6, 8));
            ringParams.PktSize = BitConverter.ToUInt32(headerSpan.Slice(14, 4));
            ringParams.MaxPackets = BitConverter.ToUInt64(headerSpan.Slice(18, 8));
            ringParams.MaxOffset = BitConverter.ToInt64(headerSpan.Slice(26, 8));
            ringParams.HeaderSize = BitConverter.ToUInt32(headerSpan.Slice(34, 4));
            ringParams.CorruptFlag = headerSpan[38];
            ringParams.FluxFlag = headerSpan[39];
            ringParams.EarliestId = BitConverter.ToUInt64(headerSpan.Slice(144, 8));
            ringParams.EarliestPTime = new NsTime(BitConverter.ToInt64(headerSpan.Slice(152, 8)));
            ringParams.EarliestDsTime = new NsTime(BitConverter.ToInt64(headerSpan.Slice(160, 8)));
            ringParams.EarliestDeTime = new NsTime(BitConverter.ToInt64(headerSpan.Slice(168, 8)));
            ringParams.EarliestOffset = BitConverter.ToInt64(headerSpan.Slice(176, 8));
            ringParams.LatestId = BitConverter.ToUInt64(headerSpan.Slice(184, 8));
            ringParams.LatestPTime = new NsTime(BitConverter.ToInt64(headerSpan.Slice(192, 8)));
            ringParams.LatestDsTime = new NsTime(BitConverter.ToInt64(headerSpan.Slice(200, 8)));
            ringParams.LatestDeTime = new NsTime(BitConverter.ToInt64(headerSpan.Slice(208, 8)));
            ringParams.LatestOffset = BitConverter.ToInt64(headerSpan.Slice(216, 8));

            // Check signature
            string sig = Encoding.ASCII.GetString(ringParams.Signature).TrimEnd('\0');
            bool reset = volatileflag || sig != Constants.RingSignature ||
                         ringParams.Version != Constants.RingVersion ||
                         ringParams.RingSize != ringsize ||
                         ringParams.PktSize != pktsize ||
                         ringParams.MaxPackets != maxpackets ||
                         ringParams.MaxOffset != maxoffset ||
                         ringParams.HeaderSize != headersize;

            if (reset)
            {
                if (!volatileflag && ringParams.DataBuffer != null)
                {
                    if (sig != Constants.RingSignature)
                        Logging.lprintf(0, $"** Packet buffer signature mismatch: {sig} <-> {Constants.RingSignature}");
                    if (ringParams.Version != Constants.RingVersion)
                        Logging.lprintf(0, $"** Packet buffer version change: {ringParams.Version} -> {Constants.RingVersion}");
                    if (ringParams.RingSize != ringsize)
                        Logging.lprintf(0, $"** Packet buffer size change: {ringParams.RingSize} -> {ringsize}");
                    if (ringParams.PktSize != pktsize)
                        Logging.lprintf(0, $"** Packet size change: {ringParams.PktSize} -> {pktsize}");
                }

                Logging.lprintf(0, "Resetting ring packet buffer parameters");
                ResetRingParams(ringParams, ringsize, pktsize, maxpackets, maxoffset, headersize);
            }
            else if (!volatileflag && ringParams.EarliestOffset >= 0)
            {
                Logging.lprintf(1, "Recovering stream index");
                // Load stream index from file
                if (File.Exists(streamfilename))
                {
                    using var fs = new FileStream(streamfilename, FileMode.Open, FileAccess.Read);
                    var streamData = new byte[60 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8]; // RingStream size
                    while (fs.Read(streamData, 0, streamData.Length) == streamData.Length)
                    {
                        var stream = DeserializeRingStream(streamData);
                        if (stream != null)
                        {
                            ringParams.StreamIdx.Insert(stream.StreamId, stream);
                            ringParams.StreamCount++;
                            Logging.lprintf(1, $"  Stream: {stream.StreamId}");
                        }
                    }
                }
            }

            // Validate offsets
            if (ringParams.EarliestOffset > ringParams.MaxOffset)
            {
                Logging.lprintf(0, "RingInitialize(): error earliest offset > maxoffset, ring corrupted");
                CleanupAndReturn(ringParams);
                return -1;
            }
            if (ringParams.LatestOffset > ringParams.MaxOffset)
            {
                Logging.lprintf(0, "RingInitialize(): error latest offset > maxoffset, ring corrupted");
                CleanupAndReturn(ringParams);
                return -1;
            }
        }
        else
        {
            // New ring
            ResetRingParams(ringParams, ringsize, pktsize, maxpackets, maxoffset, headersize);
        }

        ringParams.RingStart = Generic.NSnow();
        Logging.lprintf(0, "Ring initialized");
        return 0;
    }

    private static void ResetRingParams(RingParams rp, ulong ringsize, uint pktsize,
                                          ulong maxpackets, long maxoffset, uint headersize)
    {
        rp.Signature = Encoding.ASCII.GetBytes(Constants.RingSignature);
        Array.Resize(ref rp.Signature, 4);
        rp.Version = Constants.RingVersion;
        rp.RingSize = ringsize;
        rp.PktSize = pktsize;
        rp.MaxPackets = maxpackets;
        rp.MaxOffset = maxoffset;
        rp.HeaderSize = headersize;
        rp.CorruptFlag = 0;
        rp.FluxFlag = 0;
        rp.EarliestId = Constants.RingIdNone;
        rp.EarliestPTime = NsTime.Unset;
        rp.EarliestDsTime = NsTime.Unset;
        rp.EarliestDeTime = NsTime.Unset;
        rp.EarliestOffset = -1;
        rp.LatestId = Constants.RingIdNone;
        rp.LatestPTime = NsTime.Unset;
        rp.LatestDsTime = NsTime.Unset;
        rp.LatestDeTime = NsTime.Unset;
        rp.LatestOffset = -1;
        rp.TxPacketRate = 0.0;
        rp.TxByteRate = 0.0;
        rp.RxPacketRate = 0.0;
        rp.RxByteRate = 0.0;

        // Initialize signature in buffer
        if (rp.DataBuffer != null && rp.DataBuffer.Length > 4)
        {
            Encoding.ASCII.GetBytes(Constants.RingSignature).CopyTo(rp.DataBuffer, 0);
        }
    }

    private static void CleanupAndReturn(RingParams rp)
    {
        rp.StreamIdx.Destroy();
        rp.DataBuffer = null!;
    }

    /// <summary>
    /// Shutdown the ring buffer, writing data and stream index to files.
    /// Returns 0 on success, -1 on error.
    /// </summary>
    public static int Shutdown(int ringfd, string streamfilename, RingParams ringParams)
    {
        if (ringParams == null)
            return -1;

        if (ringParams.VolatileFlag == 0 && string.IsNullOrEmpty(streamfilename))
            return -1;

        // If volatile, just cleanup memory
        if (ringParams.VolatileFlag > 0)
        {
            ringParams.StreamIdx.Destroy();
            return 0;
        }

        // Write ring params header to buffer
        WriteHeaderToBuffer(ringParams);

        // Set flux flag
        ringParams.FluxFlag = 1;

        // Write stream index
        try
        {
            using var fs = new FileStream(streamfilename, FileMode.Create, FileAccess.Write);
            var streams = new Types.Stack<object>();
            ringParams.StreamIdx.BuildStack(streams);

            while (streams.NotEmpty)
            {
                var stream = (RingStream)streams.Pop()!;
                var data = SerializeRingStream(stream);
                fs.Write(data, 0, data.Length);
            }
        }
        catch (Exception ex)
        {
            Logging.lprintf(0, $"RingShutdown(): error writing stream file: {ex.Message}");
        }

        // Clear flux flag
        ringParams.FluxFlag = 0;

        // Cleanup
        ringParams.StreamIdx.Destroy();

        // Write ring buffer file
        string ringfilename = "";
        try
        {
            if (!string.IsNullOrEmpty(streamfilename))
            {
                // Derive ring filename from stream filename
                ringfilename = streamfilename.Replace("streamidx", "packetbuf")
                    .Replace("streamidx", "packetbuf");
            }

            if (ringParams.DataBuffer != null)
            {
                string dir = Path.GetDirectoryName(streamfilename)!;
                string packetFile = Path.Combine(dir, "packetbuf");
                File.WriteAllBytes(packetFile, ringParams.DataBuffer);
                Logging.lprintf(1, "Writing and closing ring buffer file");
            }
        }
        catch (Exception ex)
        {
            Logging.lprintf(0, $"RingShutdown(): error writing ring buffer: {ex.Message}");
            return -1;
        }

        return 0;
    }

    /// <summary>
    /// Write header from RingParams to the data buffer
    /// </summary>
    private static void WriteHeaderToBuffer(RingParams rp)
    {
        if (rp.DataBuffer == null) return;

        var span = rp.DataBuffer.AsSpan();
        int offset = 0;

        // Signature (4 bytes)
        var sigBytes = Encoding.ASCII.GetBytes(Constants.RingSignature);
        sigBytes.CopyTo(span.Slice(0, Math.Min(sigBytes.Length, 4)));
        offset = 4;

        // Version (2)
        BitConverter.TryWriteBytes(span.Slice(offset, 2), rp.Version); offset += 2;
        // RingSize (8)
        BitConverter.TryWriteBytes(span.Slice(offset, 8), rp.RingSize); offset += 8;
        // PktSize (4)
        BitConverter.TryWriteBytes(span.Slice(offset, 4), rp.PktSize); offset += 4;
        // MaxPackets (8)
        BitConverter.TryWriteBytes(span.Slice(offset, 8), rp.MaxPackets); offset += 8;
        // MaxOffset (8)
        BitConverter.TryWriteBytes(span.Slice(offset, 8), rp.MaxOffset); offset += 8;
        // HeaderSize (4)
        BitConverter.TryWriteBytes(span.Slice(offset, 4), rp.HeaderSize); offset += 4;
        // CorruptFlag, FluxFlag (2)
        span[offset] = rp.CorruptFlag; offset++;
        span[offset] = rp.FluxFlag; offset++;

        // Skip to earliest/latest fields (at offset 144 in the original layout)
        offset = 144;
        // EarliestId (8)
        BitConverter.TryWriteBytes(span.Slice(offset, 8), rp.EarliestId); offset += 8;
        // EarliestPTime (8)
        BitConverter.TryWriteBytes(span.Slice(offset, 8), rp.EarliestPTime.Value); offset += 8;
        // EarliestDsTime (8)
        BitConverter.TryWriteBytes(span.Slice(offset, 8), rp.EarliestDsTime.Value); offset += 8;
        // EarliestDeTime (8)
        BitConverter.TryWriteBytes(span.Slice(offset, 8), rp.EarliestDeTime.Value); offset += 8;
        // EarliestOffset (8)
        BitConverter.TryWriteBytes(span.Slice(offset, 8), rp.EarliestOffset); offset += 8;
        // LatestId (8)
        BitConverter.TryWriteBytes(span.Slice(offset, 8), rp.LatestId); offset += 8;
        // LatestPTime (8)
        BitConverter.TryWriteBytes(span.Slice(offset, 8), rp.LatestPTime.Value); offset += 8;
        // LatestDsTime (8)
        BitConverter.TryWriteBytes(span.Slice(offset, 8), rp.LatestDsTime.Value); offset += 8;
        // LatestDeTime (8)
        BitConverter.TryWriteBytes(span.Slice(offset, 8), rp.LatestDeTime.Value); offset += 8;
        // LatestOffset (8)
        BitConverter.TryWriteBytes(span.Slice(offset, 8), rp.LatestOffset);
    }

    /// <summary>
    /// Add packet to the ring including updates to the packet and stream indexes.
    /// Returns 0 on success, -1 on non-corruption error, -2 on corrupt ring error.
    /// </summary>
    public int Write(RingPacket packet, byte[] packetdata, uint datasize)
    {
        if (_ringParams == null || packet == null || packetdata == null)
            return -1;

        // Check packet size
        if ((RingPacket.SerializedSize + datasize) > _ringParams.PktSize)
        {
            Logging.lprintf(0, $"RingWrite(): {packet.StreamId} packet size too large ({RingPacket.SerializedSize + datasize}), maximum is {_ringParams.PktSize} bytes");
            return -1;
        }

        RingPacket? earliest = null;
        RingPacket? latest = null;

        lock (_ringParams.WriteLock)
        {
            lock (_ringParams.StreamLock)
            {
                _ringParams.FluxFlag = 1;

                // Get earliest and latest packets
                if (_ringParams.EarliestOffset >= 0)
                {
                    earliest = ReadPacketAtOffset(_ringParams.EarliestOffset);
                }
                if (_ringParams.LatestOffset >= 0)
                {
                    latest = ReadPacketAtOffset(_ringParams.LatestOffset);
                }

                // Determine next packet ID and offset
                long offset;
                ulong pktid;

                if (latest != null)
                {
                    offset = NextOffset(latest.Offset, _ringParams.MaxOffset, _ringParams.PktSize);
                    pktid = latest.PktId + 1;

                    if (pktid > Constants.RingIdMaximum)
                        pktid = 1;
                }
                else
                {
                    pktid = 1;
                    offset = 0;
                }

                // Update packet details
                packet.PktId = (packet.PktId == Constants.RingIdNone) ? pktid : packet.PktId;
                packet.Offset = offset;
                packet.PktTime = Generic.NSnow();
                packet.NextInStream = -1;

                // Remove earliest packet if ring is full
                if (earliest != null && latest != null && earliest != latest)
                {
                    if (offset == _ringParams.EarliestOffset)
                    {
                        long nextOffset = NextOffset(earliest.Offset, _ringParams.MaxOffset, _ringParams.PktSize);
                        var nextInRing = ReadPacketAtOffset(nextOffset);
                        var nextInStream = earliest.NextInStream >= 0 ? ReadPacketAtOffset(earliest.NextInStream) : null;

                        var streamOfEarliest = GetStreamIdx(earliest.StreamId);

                        Logging.lprintf(3, $"Removing packet for stream {earliest.StreamId} (id: {earliest.PktId}, offset: {earliest.Offset})");

                        // Update RingParams
                        _ringParams.EarliestId = nextInRing?.PktId ?? Constants.RingIdNone;
                        _ringParams.EarliestPTime = nextInRing?.PktTime ?? NsTime.Unset;
                        _ringParams.EarliestDsTime = nextInRing?.DataStart ?? NsTime.Unset;
                        _ringParams.EarliestDeTime = nextInRing?.DataEnd ?? NsTime.Unset;
                        _ringParams.EarliestOffset = nextInRing?.Offset ?? -1;

                        if (streamOfEarliest != null)
                        {
                            // Delete stream if this is the only packet
                            if (earliest.Offset == streamOfEarliest.EarliestOffset &&
                                earliest.Offset == streamOfEarliest.LatestOffset)
                            {
                                Logging.lprintf(2, $"Removing stream index entry for {earliest.StreamId}");
                                DelStreamIdx(earliest.StreamId);
                                _ringParams.StreamCount--;
                            }
                            else if (nextInStream != null)
                            {
                                streamOfEarliest.EarliestDsTime = nextInStream.DataStart;
                                streamOfEarliest.EarliestDeTime = nextInStream.DataEnd;
                                streamOfEarliest.EarliestPTime = nextInStream.PktTime;
                                streamOfEarliest.EarliestId = nextInStream.PktId;
                                streamOfEarliest.EarliestOffset = nextInStream.Offset;
                            }
                        }
                    }
                }

                // Find or create stream entry
                var stream = GetStreamIdx(packet.StreamId);
                if (stream == null)
                {
                    stream = new RingStream
                    {
                        StreamId = packet.StreamId,
                        EarliestDsTime = packet.DataStart,
                        EarliestDeTime = packet.DataEnd,
                        EarliestPTime = packet.PktTime,
                        EarliestId = packet.PktId,
                        EarliestOffset = packet.Offset,
                        LatestOffset = -1
                    };

                    _ringParams.StreamIdx.Insert(stream.StreamId, stream);
                    _ringParams.StreamCount++;
                    Logging.lprintf(2, $"Added stream entry for {packet.StreamId}");
                }

                // Write packet header into ring buffer
                var packetSpan = new byte[_ringParams.PktSize];
                packet.Serialize(packetSpan.AsSpan());
                if (packetdata != null && datasize > 0)
                {
                    Array.Copy(packetdata, 0, packetSpan, RingPacket.SerializedSize, Math.Min((int)datasize, packetdata.Length));
                }

                // Copy to ring buffer at the right offset
                int bufferOffset = _ringParams.DataOffset + (int)offset;
                packetSpan.CopyTo(_ringParams.DataBuffer.AsSpan(bufferOffset));

                // Update RingParams with latest
                _ringParams.LatestId = packet.PktId;
                _ringParams.LatestPTime = packet.PktTime;
                _ringParams.LatestDsTime = packet.DataStart;
                _ringParams.LatestDeTime = packet.DataEnd;
                _ringParams.LatestOffset = packet.Offset;

                // Update earliest for initial packet
                if (earliest == null)
                {
                    _ringParams.EarliestId = packet.PktId;
                    _ringParams.EarliestPTime = packet.PktTime;
                    _ringParams.EarliestDsTime = packet.DataStart;
                    _ringParams.EarliestDeTime = packet.DataEnd;
                    _ringParams.EarliestOffset = packet.Offset;
                }

                // Update previous packet in stream
                if (stream.LatestOffset >= 0)
                {
                    var prevLatest = ReadPacketAtOffset(stream.LatestOffset);
                    if (prevLatest != null)
                    {
                        prevLatest.NextInStream = packet.Offset;
                        // Re-write ONLY the header (not the full PktSize which would
                        // zero out the miniSEED payload data).  The header is
                        // SerializedSize (112) bytes; the data follows at offset 112.
                        // In C, only the 'nextinstream' field is updated in-place via
                        // pointer, so we must NOT write past the header boundary.
                        var prevSpan = new byte[RingPacket.SerializedSize];
                        prevLatest.Serialize(prevSpan.AsSpan());
                        int prevBufOff = _ringParams.DataOffset + (int)stream.LatestOffset;
                        prevSpan.CopyTo(_ringParams.DataBuffer.AsSpan(prevBufOff));
                    }
                }

                // Update stream entry
                stream.LatestDsTime = packet.DataStart;
                stream.LatestDeTime = packet.DataEnd;
                stream.LatestPTime = packet.PktTime;
                stream.LatestId = packet.PktId;
                stream.LatestOffset = packet.Offset;

                _ringParams.FluxFlag = 0;
            }
        }

        Logging.lprintf(3, $"Added packet for stream {packet.StreamId}, pktid: {packet.PktId}, offset: {packet.Offset}");
        return 0;
    }

    /// <summary>
    /// Read a requested packet ID from the ring.
    /// Returns packet ID on success, RINGID_NONE when not found, RINGID_ERROR on error.
    /// </summary>
    public ulong Read(RingReader reader, ulong reqid, RingPacket packet, byte[]? packetdata)
    {
        if (reader == null || packet == null)
            return Constants.RingIdError;

        if (reqid > Constants.RingIdMaximum)
        {
            Logging.lprintf(0, $"RingRead(): unsupported position value: {reqid}");
            return Constants.RingIdError;
        }

        ulong pktid = reqid;

        // Find the offset
        long offset = FindOffsetForID(pktid, out _);
        if (offset < 0)
            return Constants.RingIdNone;

        var pkt = ReadPacketAtOffset(offset);
        if (pkt == null)
            return Constants.RingIdNone;

        // Copy packet header
        packet.Offset = pkt.Offset;
        packet.PktId = pkt.PktId;
        packet.PktTime = pkt.PktTime;
        packet.NextInStream = pkt.NextInStream;
        packet.StreamId = pkt.StreamId;
        packet.DataStart = pkt.DataStart;
        packet.DataEnd = pkt.DataEnd;
        packet.DataSize = pkt.DataSize;

        // Copy packet data
        if (packetdata != null && pkt.DataSize > 0)
        {
            int bufOffset = _ringParams.DataOffset + (int)offset + RingPacket.SerializedSize;
            if (bufOffset + (int)pkt.DataSize <= _ringParams.DataBuffer.Length)
            {
                Array.Copy(_ringParams.DataBuffer, bufOffset, packetdata, 0, (int)pkt.DataSize);
            }
        }

        // Sanity check
        if (pktid != pkt.PktId)
            return Constants.RingIdNone;

        // Update reader position
        reader.PktOffset = packet.Offset;
        reader.PktId = packet.PktId;
        reader.PktTime = packet.PktTime;
        reader.DataStart = packet.DataStart;
        reader.DataEnd = packet.DataEnd;

        return pktid;
    }

    /// <summary>
    /// Determine and read the next packet from the ring.
    /// Returns packet ID on success, RINGID_NONE when no next packet, RINGID_ERROR on error.
    /// </summary>
    public ulong ReadNext(RingReader reader, RingPacket packet, byte[]? packetdata)
    {
        if (reader == null || packet == null)
            return Constants.RingIdError;

        var ringparams = _ringParams;

        long latestOffset = ringparams.LatestOffset;
        long earliestOffset = ringparams.EarliestOffset;

        // If ring is empty
        if (latestOffset < 0)
        {
            if (reader.PktOffset < 0 && reader.PktId == Constants.RingIdNext)
                reader.PktId = Constants.RingIdEarliest;
            return Constants.RingIdNone;
        }

        // Read latest packet fields directly from buffer (avoid RingPacket allocation).
        // Serialized layout: offset(8) + pktid(8) + pkttime(8) + nextinstream(8) +
        //   streamid(60) + datastart(8) + dataend(8) + datasize(4) = 112 bytes
        int latestBufOff = _ringParams.DataOffset + (int)latestOffset;
        if (latestBufOff + RingPacket.SerializedSize > _ringParams.DataBuffer.Length)
            return Constants.RingIdNone;

        ulong latestId = BitConverter.ToUInt64(_ringParams.DataBuffer.AsSpan(latestBufOff + 8));
        NsTime latestPTime = new NsTime(BitConverter.ToInt64(_ringParams.DataBuffer.AsSpan(latestBufOff + 16)));
        NsTime latestDsTime = new NsTime(BitConverter.ToInt64(_ringParams.DataBuffer.AsSpan(latestBufOff + 92)));
        NsTime latestDeTime = new NsTime(BitConverter.ToInt64(_ringParams.DataBuffer.AsSpan(latestBufOff + 100)));

        long offset;

        // Determine offset for initial read or relative positions
        if (reader.PktOffset < 0)
        {
            if (reader.PktId == Constants.RingIdNext)
            {
                reader.PktOffset = latestOffset;
                reader.PktId = latestId;
                reader.PktTime = latestPTime;
                reader.DataStart = latestDsTime;
                reader.DataEnd = latestDeTime;
                return Constants.RingIdNone;
            }
            else if (reader.PktId == Constants.RingIdLatest)
            {
                offset = latestOffset;
            }
            else if (reader.PktId == Constants.RingIdEarliest)
            {
                offset = earliestOffset;
            }
            else
            {
                Logging.lprintf(0, $"RingReadNext(): unsupported packet ID value: {reader.PktId}");
                return Constants.RingIdError;
            }
        }
        else
        {
            offset = NextOffset(reader.PktOffset, ringparams.MaxOffset, ringparams.PktSize);
        }

        long eobOffset = NextOffset(latestOffset, ringparams.MaxOffset, ringparams.PktSize);

        // Loop until we have a matching packet or reached end of buffer
        bool skip = true;
        uint skipped = 0;

        while (skip && offset != eobOffset)
        {
            skip = false;

            var pkt = ReadPacketAtOffset(offset);
            if (pkt == null)
                return Constants.RingIdNone;

            NsTime pktTime = pkt.PktTime;

            // Check if packet is valid
            if (pktTime > latestPTime)
            {
                offset = ringparams.EarliestOffset;
                skipped++;
                if (skipped >= 100)
                {
                    Logging.lprintf(0, $"RingReadNext(): skipped off trailing edge of buffer {skipped} times");
                    return Constants.RingIdNone;
                }
                skip = true;
                continue;
            }

            skipped = 0;

            // Update reader position
            reader.PktOffset = offset;
            reader.PktId = pkt.PktId;
            reader.PktTime = pkt.PktTime;
            reader.DataStart = pkt.DataStart;
            reader.DataEnd = pkt.DataEnd;

            // Test limit/match/reject expressions
            if (reader.Limit != null)
            {
                if (!reader.Limit.IsMatch(pkt.StreamId))
                    skip = true;
            }
            if (reader.Match != null && !skip)
            {
                if (!reader.Match.IsMatch(pkt.StreamId))
                    skip = true;
            }
            if (reader.Reject != null && !skip)
            {
                if (reader.Reject.IsMatch(pkt.StreamId))
                    skip = true;
            }

            if (skip)
            {
                offset = NextOffset(offset, ringparams.MaxOffset, ringparams.PktSize);
            }
            else
            {
                // Copy packet header
                packet.Offset = pkt.Offset;
                packet.PktId = pkt.PktId;
                packet.PktTime = pkt.PktTime;
                packet.NextInStream = pkt.NextInStream;
                packet.StreamId = pkt.StreamId;
                packet.DataStart = pkt.DataStart;
                packet.DataEnd = pkt.DataEnd;
                packet.DataSize = pkt.DataSize;

                // Copy packet data
                if (packetdata != null && pkt.DataSize > 0)
                {
                    int bufOffset = _ringParams.DataOffset + (int)offset + RingPacket.SerializedSize;
                    if (bufOffset + (int)pkt.DataSize <= _ringParams.DataBuffer.Length)
                    {
                        Array.Copy(_ringParams.DataBuffer, bufOffset, packetdata, 0, (int)pkt.DataSize);
                    }
                    else
                    {
                        Logging.lprintf(0, $"RingReadNext(): packet data at offset {offset} spans beyond buffer end "
                            + $"(bufOffset={bufOffset}, dataSize={pkt.DataSize}, bufferLen={_ringParams.DataBuffer.Length}) — DATA LOST");
                    }
                }

                // Sanity check: re-read PktTime directly from the live ring buffer
                // (not from the deserialized copy) to detect if the slot was
                // overwritten during our processing (race condition / ring wrap).
                // Equivalent to C original: if (pkttime != pkt->pkttime) return RINGID_NONE;
                // where pkt is a LIVE pointer to shared memory.
                int pktTimeBufOff = _ringParams.DataOffset + (int)offset + 16;
                if (pktTimeBufOff + 8 <= _ringParams.DataBuffer.Length)
                {
                    long livePktTime = BitConverter.ToInt64(_ringParams.DataBuffer.AsSpan(pktTimeBufOff, 8));
                    if (pktTime.Value != livePktTime)
                    {
                        Logging.lprintf(2, $"RingReadNext(): pkttime mismatch (copy={pktTime.Value}, live={livePktTime}) — slot overwritten, skipping");
                        return Constants.RingIdNone;
                    }
                }

                return packet.PktId;
            }
        }

        if (offset == eobOffset)
            return Constants.RingIdNone;

        return Constants.RingIdNone;
    }

    /// <summary>
    /// Set the ring reading position to the specified packet ID.
    /// Returns packet ID on success, RINGID_NONE when not found, RINGID_ERROR on error.
    /// </summary>
    public ulong Position(RingReader reader, ulong pktid, NsTime pkttime)
    {
        if (reader == null)
            return Constants.RingIdError;

        // Resolve relative positions
        if (pktid == Constants.RingIdEarliest)
            pktid = _ringParams.EarliestId;
        else if (pktid == Constants.RingIdLatest)
            pktid = _ringParams.LatestId;

        if (pktid > Constants.RingIdMaximum)
        {
            Logging.lprintf(0, $"RingPosition(): unsupported position value: {pktid}");
            return Constants.RingIdError;
        }

        // Find offset
        long offset = FindOffsetForID(pktid, out NsTime ptime);
        if (offset < 0)
            return Constants.RingIdNone;

        var pkt = ReadPacketAtOffset(offset);
        if (pkt == null)
            return Constants.RingIdNone;

        // Check pkttime
        if (pkttime != NsTime.Unset && pkttime != NsTime.Error)
        {
            if (pkttime != ptime)
                return Constants.RingIdNone;
        }

        NsTime datastart = pkt.DataStart;
        NsTime dataend = pkt.DataEnd;

        // Sanity check
        if (pktid != pkt.PktId)
            return Constants.RingIdNone;

        // Update reader position
        reader.PktOffset = offset;
        reader.PktId = pktid;
        reader.PktTime = ptime;
        reader.DataStart = datastart;
        reader.DataEnd = dataend;

        return pktid;
    }

    /// <summary>
    /// Find the first packet after the specified time.
    /// </summary>
    public ulong After(RingReader reader, NsTime reftime, int whence)
    {
        // Simple linear scan implementation
        var ringparams = _ringParams;
        if (ringparams.LatestOffset < 0)
            return Constants.RingIdNone;

        long offset = (whence == 0) ? ringparams.EarliestOffset : 0;

        if (offset < 0) offset = 0;

        long eobOffset = NextOffset(ringparams.LatestOffset, ringparams.MaxOffset, ringparams.PktSize);

        while (offset != eobOffset)
        {
            var pkt = ReadPacketAtOffset(offset);
            if (pkt == null) break;

            if (pkt.DataStart >= reftime)
            {
                reader.PktOffset = offset;
                reader.PktId = pkt.PktId;
                reader.PktTime = pkt.PktTime;
                reader.DataStart = pkt.DataStart;
                reader.DataEnd = pkt.DataEnd;
                return pkt.PktId;
            }

            offset = NextOffset(offset, ringparams.MaxOffset, ringparams.PktSize);
        }

        return Constants.RingIdNone;
    }

    /// <summary>
    /// Find packets after the specified time in reverse order.
    /// </summary>
    public ulong AfterRev(RingReader reader, NsTime reftime, ulong pktlimit, int whence)
    {
        var ringparams = _ringParams;
        if (ringparams.LatestOffset < 0)
            return Constants.RingIdNone;

        ulong count = 0;
        long offset = ringparams.LatestOffset;

        while (offset >= 0 && count < pktlimit)
        {
            var pkt = ReadPacketAtOffset(offset);
            if (pkt == null) break;

            if (pkt.DataStart >= reftime)
            {
                reader.PktOffset = offset;
                reader.PktId = pkt.PktId;
                reader.PktTime = pkt.PktTime;
                reader.DataStart = pkt.DataStart;
                reader.DataEnd = pkt.DataEnd;
                count++;

                if (count >= pktlimit)
                    return pkt.PktId;
            }

            // Go to previous offset
            if (offset == 0)
                offset = ringparams.MaxOffset;
            else
                offset -= ringparams.PktSize;

            if (offset == ringparams.LatestOffset)
                break;
        }

        return count > 0 ? reader.PktId : Constants.RingIdNone;
    }

    /// <summary>
    /// Get a stack of streams matching the reader's filter
    /// </summary>
    public Types.Stack<object> GetStreamsStack(RingReader? reader)
    {
        var stack = new Types.Stack<object>();

        lock (_ringParams.StreamLock)
        {
            _ringParams.StreamIdx.Traverse(node =>
            {
                var stream = (RingStream)node.Data!;
                if (reader != null)
                {
                    if (reader.Limit != null && !reader.Limit.IsMatch(stream.StreamId))
                        return;
                    if (reader.Match != null && !reader.Match.IsMatch(stream.StreamId))
                        return;
                    if (reader.Reject != null && !reader.Reject.IsMatch(stream.StreamId))
                        return;
                }
                stack.Push(stream);
            });
        }

        return stack;
    }

    /// <summary>
    /// Log ring parameters
    /// </summary>
    public void LogParameters()
    {
        Logging.lprintf(0, $"Ring Parameters:");
        Logging.lprintf(0, $"  Ring size: {Generic.HumanSizeString(_ringParams.RingSize)}");
        Logging.lprintf(0, $"  Packet size: {Generic.HumanSizeString(_ringParams.PktSize)}");
        Logging.lprintf(0, $"  Max packets: {_ringParams.MaxPackets}");
        Logging.lprintf(0, $"  Max offset: {_ringParams.MaxOffset}");
        Logging.lprintf(0, $"  Header size: {_ringParams.HeaderSize}");
        Logging.lprintf(0, $"  Memory map: {(_ringParams.MmapFlag > 0 ? "yes" : "no")}");
        Logging.lprintf(0, $"  Volatile: {(_ringParams.VolatileFlag > 0 ? "yes" : "no")}");
        Logging.lprintf(0, $"  Earliest ID: {_ringParams.EarliestId}");
        Logging.lprintf(0, $"  Latest ID: {_ringParams.LatestId}");
        Logging.lprintf(0, $"  Stream count: {_ringParams.StreamCount}");
    }

    /// <summary>
    /// Static overload: log ring parameters from a RingParams instance.
    /// </summary>
    public static void LogParameters(RingParams rp)
    {
        if (rp == null) return;
        Logging.lprintf(0, $"Ring Parameters:");
        Logging.lprintf(0, $"  Ring size: {Generic.HumanSizeString(rp.RingSize)}");
        Logging.lprintf(0, $"  Packet size: {Generic.HumanSizeString(rp.PktSize)}");
        Logging.lprintf(0, $"  Max packets: {rp.MaxPackets}");
        Logging.lprintf(0, $"  Max offset: {rp.MaxOffset}");
        Logging.lprintf(0, $"  Header size: {rp.HeaderSize}");
        Logging.lprintf(0, $"  Memory map: {(rp.MmapFlag > 0 ? "yes" : "no")}");
        Logging.lprintf(0, $"  Volatile: {(rp.VolatileFlag > 0 ? "yes" : "no")}");
        Logging.lprintf(0, $"  Earliest ID: {rp.EarliestId}");
        Logging.lprintf(0, $"  Latest ID: {rp.LatestId}");
        Logging.lprintf(0, $"  Stream count: {rp.StreamCount}");
    }

    /// <summary>
    /// Update a regex pattern for a ring reader
    /// </summary>
    public static bool UpdatePattern(ref System.Text.RegularExpressions.Regex? code, string? pattern, string description)
    {
        if (string.IsNullOrEmpty(pattern))
        {
            code = null;
            return true;
        }

        try
        {
            code = new System.Text.RegularExpressions.Regex(pattern,
                System.Text.RegularExpressions.RegexOptions.Compiled |
                System.Text.RegularExpressions.RegexOptions.CultureInvariant);
            Logging.lprintf(2, $"Compiled {description} expression: /{pattern}/");
            return true;
        }
        catch (Exception ex)
        {
            Logging.lprintf(0, $"Error compiling {description} expression '{pattern}': {ex.Message}");
            code = null;
            return false;
        }
    }

    // ================== Private helpers ==================

    private RingPacket? ReadPacketAtOffset(long offset)
    {
        if (_ringParams.DataBuffer == null || offset < 0)
            return null;

        int bufOffset = _ringParams.DataOffset + (int)offset;
        if (bufOffset + RingPacket.SerializedSize > _ringParams.DataBuffer.Length)
            return null;

        try
        {
            return RingPacket.Deserialize(_ringParams.DataBuffer.AsSpan(bufOffset));
        }
        catch
        {
            return null;
        }
    }

    private long FindOffsetForID(ulong pktid, out NsTime pkttime)
    {
        pkttime = NsTime.Unset;
        var ringparams = _ringParams;

        if (ringparams.EarliestOffset < 0 || ringparams.LatestOffset < 0)
            return -1;

        // Linear scan from earliest to latest
        long offset = ringparams.EarliestOffset;
        long eobOffset = NextOffset(ringparams.LatestOffset, ringparams.MaxOffset, ringparams.PktSize);

        while (offset != eobOffset)
        {
            var pkt = ReadPacketAtOffset(offset);
            if (pkt == null) break;

            if (pkt.PktId == pktid)
            {
                pkttime = pkt.PktTime;
                return offset;
            }

            offset = NextOffset(offset, ringparams.MaxOffset, ringparams.PktSize);

            // Safety: break if we wrapped around
            if (offset == ringparams.EarliestOffset)
                break;
        }

        return -1;
    }

    private RingStream? GetStreamIdx(string streamid)
    {
        var node = _ringParams.StreamIdx.Find(streamid);
        return node?.Data as RingStream;
    }

    private bool DelStreamIdx(string streamid)
    {
        var node = _ringParams.StreamIdx.Find(streamid);
        if (node != null)
        {
            _ringParams.StreamIdx.Delete(node);
            return true;
        }
        return false;
    }

    private static long NextOffset(long offset, long maxOffset, uint pktSize)
    {
        long result = offset + pktSize;
        return (result > maxOffset) ? 0 : result;
    }

    private static byte[] SerializeRingStream(RingStream stream)
    {
        var data = new byte[60 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8];
        int offset = 0;

        // StreamId (fixed 60 bytes)
        var sidBytes = Encoding.ASCII.GetBytes(stream.StreamId.PadRight(60, '\0'));
        Array.Copy(sidBytes, 0, data, offset, Math.Min(60, sidBytes.Length)); offset += 60;

        BitConverter.TryWriteBytes(data.AsSpan(offset), stream.EarliestDsTime.Value); offset += 8;
        BitConverter.TryWriteBytes(data.AsSpan(offset), stream.EarliestDeTime.Value); offset += 8;
        BitConverter.TryWriteBytes(data.AsSpan(offset), stream.EarliestPTime.Value); offset += 8;
        BitConverter.TryWriteBytes(data.AsSpan(offset), stream.EarliestId); offset += 8;
        BitConverter.TryWriteBytes(data.AsSpan(offset), stream.EarliestOffset); offset += 8;
        BitConverter.TryWriteBytes(data.AsSpan(offset), stream.LatestDsTime.Value); offset += 8;
        BitConverter.TryWriteBytes(data.AsSpan(offset), stream.LatestDeTime.Value); offset += 8;
        BitConverter.TryWriteBytes(data.AsSpan(offset), stream.LatestPTime.Value); offset += 8;
        BitConverter.TryWriteBytes(data.AsSpan(offset), stream.LatestId); offset += 8;
        BitConverter.TryWriteBytes(data.AsSpan(offset), stream.LatestOffset);

        return data;
    }

    private static RingStream? DeserializeRingStream(byte[] data)
    {
        if (data.Length < 60 + 80) return null;

        var stream = new RingStream();
        int offset = 0;

        stream.StreamId = Encoding.ASCII.GetString(data, offset, 60).TrimEnd('\0'); offset += 60;
        stream.EarliestDsTime = new NsTime(BitConverter.ToInt64(data, offset)); offset += 8;
        stream.EarliestDeTime = new NsTime(BitConverter.ToInt64(data, offset)); offset += 8;
        stream.EarliestPTime = new NsTime(BitConverter.ToInt64(data, offset)); offset += 8;
        stream.EarliestId = BitConverter.ToUInt64(data, offset); offset += 8;
        stream.EarliestOffset = BitConverter.ToInt64(data, offset); offset += 8;
        stream.LatestDsTime = new NsTime(BitConverter.ToInt64(data, offset)); offset += 8;
        stream.LatestDeTime = new NsTime(BitConverter.ToInt64(data, offset)); offset += 8;
        stream.LatestPTime = new NsTime(BitConverter.ToInt64(data, offset)); offset += 8;
        stream.LatestId = BitConverter.ToUInt64(data, offset); offset += 8;
        stream.LatestOffset = BitConverter.ToInt64(data, offset);

        return stream;
    }
}
