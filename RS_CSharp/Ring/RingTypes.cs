/**************************************************************************
 * RingTypes.cs
 *
 * Core ring buffer data structures.
 *
 * This file is part of the ringserver C# port.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 **************************************************************************/

using System;
using System.Text.RegularExpressions;
using System.Threading;
using RingServer.Types;

namespace RingServer.Ring;

/// <summary>
/// Ring parameters, stored at the beginning of the packet buffer file.
/// Equivalent to RingParams in ring.h.
/// </summary>
public class RingParams
{
    // Header fields (persisted to disk)
    public byte[] Signature = new byte[4];       // RING_SIGNATURE
    public ushort Version;                        // RING_VERSION
    public ulong RingSize;                        // Ring size in bytes
    public uint PktSize;                          // Packet size in bytes
    public ulong MaxPackets;                      // Maximum number of packets
    public long MaxOffset;                        // Maximum packet offset
    public uint HeaderSize;                       // Size of ring header
    public byte CorruptFlag;                      // Flag indicating the ring is corrupt
    public byte FluxFlag;                         // Flag indicating the ring is in flux

    // Runtime fields (not persisted)
    public byte MmapFlag;                         // Memory mapped flag
    public byte VolatileFlag;                     // Volatile ring flag
    public object WriteLock = new();              // Mutex lock for ring write access
    public RBTree StreamIdx = null!;              // Binary tree of streams
    public object StreamLock = new();             // Mutex lock for stream index
    public uint StreamCount;                      // Count of streams in index

    public ulong EarliestId;                      // Earliest packet ID
    public NsTime EarliestPTime;                  // Earliest packet creation time
    public NsTime EarliestDsTime;                 // Earliest packet data start time
    public NsTime EarliestDeTime;                 // Earliest packet data end time
    public long EarliestOffset;                   // Earliest packet offset in bytes
    public ulong LatestId;                        // Latest packet ID
    public NsTime LatestPTime;                    // Latest packet creation time
    public NsTime LatestDsTime;                   // Latest packet data start time
    public NsTime LatestDeTime;                   // Latest packet data end time
    public long LatestOffset;                     // Latest packet offset in bytes
    public NsTime RingStart;                      // Ring initialization time

    public double TxPacketRate;                   // Transmission packet rate in Hz
    public double TxByteRate;                     // Transmission byte rate in Hz
    public double RxPacketRate;                   // Reception packet rate in Hz
    public double RxByteRate;                     // Reception byte rate in Hz

    // Data buffer - the packet data area
    public byte[] DataBuffer = null!;             // Packet data buffer
    public int DataOffset;                        // Offset where data area begins

    // File descriptor for the ring file
    public int RingFd = -1;

    /// <summary>
    /// Get a span over the data area of the ring buffer
    /// </summary>
    public Span<byte> DataSpan => DataBuffer.AsSpan(DataOffset);

    /// <summary>
    /// Get the full buffer span (including header)
    /// </summary>
    public Span<byte> FullSpan => DataBuffer.AsSpan();
}

/// <summary>
/// Ring packet header structure, data follows header in the ring.
/// Equivalent to RingPacket in ring.h.
/// </summary>
public class RingPacket
{
    public long Offset;                           // RW: Offset in ring
    public ulong PktId;                           // RW: Packet ID
    public NsTime PktTime;                        // RW: Packet creation time
    public long NextInStream;                     // RW: Offset of next packet in stream, -1 if none
    public string StreamId = new string(' ', Constants.MaxStreamId); // Packet stream ID
    public NsTime DataStart;                      // Packet data start time
    public NsTime DataEnd;                        // Packet data end time
    public uint DataSize;                         // Packet data size in bytes

    /// <summary>
    /// Serialized size of a RingPacket header in bytes.
    /// Layout: offset(8) + pktid(8) + pkttime(8) + nextinstream(8) +
    ///         streamid(60) + datastart(8) + dataend(8) + datasize(4) = 112
    /// </summary>
    public const int SerializedSize = 8 + 8 + 8 + 8 + Constants.MaxStreamId + 8 + 8 + 4;

    /// <summary>
    /// Serialize this packet header to a byte span
    /// </summary>
    public void Serialize(Span<byte> span)
    {
        int offset = 0;
        BitConverter.TryWriteBytes(span[offset..], Offset); offset += 8;
        BitConverter.TryWriteBytes(span[offset..], PktId); offset += 8;
        BitConverter.TryWriteBytes(span[offset..], PktTime.Value); offset += 8;
        BitConverter.TryWriteBytes(span[offset..], NextInStream); offset += 8;

        // Write stream ID as fixed-length ASCII
        var sidBytes = System.Text.Encoding.ASCII.GetBytes(StreamId);
        int copyLen = Math.Min(sidBytes.Length, Constants.MaxStreamId);
        sidBytes.AsSpan(0, copyLen).CopyTo(span[offset..]);
        span.Slice(offset + copyLen, Constants.MaxStreamId - copyLen).Clear();
        offset += Constants.MaxStreamId;

        BitConverter.TryWriteBytes(span[offset..], DataStart.Value); offset += 8;
        BitConverter.TryWriteBytes(span[offset..], DataEnd.Value); offset += 8;
        BitConverter.TryWriteBytes(span[offset..], DataSize); offset += 4;
    }

    /// <summary>
    /// Deserialize a packet header from a byte span
    /// </summary>
    public static RingPacket Deserialize(ReadOnlySpan<byte> span)
    {
        var pkt = new RingPacket();
        int offset = 0;
        pkt.Offset = BitConverter.ToInt64(span[offset..]); offset += 8;
        pkt.PktId = BitConverter.ToUInt64(span[offset..]); offset += 8;
        pkt.PktTime = new NsTime(BitConverter.ToInt64(span[offset..])); offset += 8;
        pkt.NextInStream = BitConverter.ToInt64(span[offset..]); offset += 8;

        // Read fixed-length stream ID, trim at first NUL
        pkt.StreamId = System.Text.Encoding.ASCII.GetString(span.Slice(offset, Constants.MaxStreamId))
                           .Split('\0')[0];
        offset += Constants.MaxStreamId;

        pkt.DataStart = new NsTime(BitConverter.ToInt64(span[offset..])); offset += 8;
        pkt.DataEnd = new NsTime(BitConverter.ToInt64(span[offset..])); offset += 8;
        pkt.DataSize = BitConverter.ToUInt32(span[offset..]); offset += 4;
        return pkt;
    }
}

/// <summary>
/// Ring stream structure used for the stream index.
/// Equivalent to RingStream in ring.h.
/// </summary>
public class RingStream
{
    public string StreamId = new string(' ', Constants.MaxStreamId); // Packet stream ID
    public NsTime EarliestDsTime;                 // Earliest packet data start time
    public NsTime EarliestDeTime;                 // Earliest packet data end time
    public NsTime EarliestPTime;                  // Earliest packet creation time
    public ulong EarliestId;                      // ID of earliest packet
    public long EarliestOffset;                   // Offset of earliest packet
    public NsTime LatestDsTime;                   // Latest packet data start time
    public NsTime LatestDeTime;                   // Latest packet data end time
    public NsTime LatestPTime;                    // Latest packet creation time
    public ulong LatestId;                        // ID of latest packet
    public long LatestOffset;                     // Offset of latest packet
}

/// <summary>
/// Ring reader parameters.
/// Equivalent to RingReader in ring.h.
/// </summary>
public class RingReader
{
    public RingParams RingParams = null!;         // Ring parameters for this reader
    public long PktOffset;                        // Current packet offset in ring
    public ulong PktId;                           // Current packet ID
    public NsTime PktTime;                        // Current packet creation time
    public NsTime DataStart;                      // Current packet data start time
    public NsTime DataEnd;                        // Current packet data end time

    // Regex patterns for stream filtering (replaces PCRE2)
    public Regex? Limit;                          // Compiled limit expression
    public Regex? Match;                          // Compiled match expression
    public Regex? Reject;                         // Compiled reject expression
}
