/**************************************************************************
 * ClientInfo.cs
 *
 * Client connection and state data structures.
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
using System.Net;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using RingServer.Ring;
using RingServer.Types;

namespace RingServer;

/// <summary>
/// Connection information for client threads.
/// Equivalent to ClientInfo in clients.h.
/// </summary>
public class ClientInfo
{
    // Socket information
    public Socket? Socket;                        // Socket descriptor
    public bool Tls;                              // Flag identifying TLS connection
    public object? TlsCtx;                        // TLS context
    public int SocketErr;                         // Socket error flag (-1: error, -2: orderly shutdown)

    // Buffers
    public byte[]? SendBuffer;                    // Client specific send buffer
    public int SendBufferSize;                    // Length of send buffer
    public byte[]? RecvBuffer;                    // Client specific receive buffer
    public int RecvBufferSize;                    // Length of receive buffer
    public int RecvLength;                        // Length of data in recvbuf
    public int RecvConsumed;                      // Bytes of recvbuf that have been consumed

    // DataLink command buffer (256 bytes)
    public byte[] DlCommand = new byte[256];

    // Packet
    public RingPacket Packet = new();             // Client specific ring packet header

    // Address info
    public byte[]? AddressBytes;                  // Client socket address bytes
    public int AddressLength;                     // Length of client socket structure
    public string IpStr = "";                     // Client host IP address
    public string PortStr = "";                   // Client port
    public string Hostname = "";                  // Client hostname
    public string ClientId = "";                  // Client identifier string
    public string ServerPort = "";                // Server port

    // State
    public ClientState State = ClientState.Command;
    public ClientType Type = ClientType.Undetermined;
    public ListenProtocols Protocols;

    // WebSocket
    public bool WebSocket;
    public uint[] WsMask = new uint[4];           // Masking key for WebSocket message
    public int WsMaskIdx;                         // Index for unmasking WebSocket message
    public ulong WsPayload;                       // Length of WebSocket payload

    // Permissions
    public bool WritePerm;
    public bool Trusted;
    public float TimeWinLimit = Constants.DefaultTimeWinLimit;

    // Ring
    public RingParams RingParams = null!;
    public RingReader? Reader;

    // Connection timing
    public NsTime ConnTime;

    // Patterns
    public string? LimitStr;                      // Regular expression string to limit streams
    public string? MatchStr;                      // Regular expression string to match streams
    public string? RejectStr;                     // Regular expression string to reject streams

    // HTTP
    public string? HttpHeaders;                   // Fixed headers to add to HTTP responses

    // Packet tracking
    public ulong LastId;                          // Last packet ID sent to client
    public NsTime StartTime;                      // Requested start time
    public NsTime EndTime;                        // Requested end time

    // Archive
    public DataStream? MsWrite;                   // miniSEED data write parameters

    // Stream tracking
    public RBTree? Streams;                       // Tracking of streams transferred
    public object StreamsLock = new();            // Mutex for streams tree
    public int StreamsCount;                      // Count of streams in tree

    // Client progression
    public int PercentLag;                        // Percent lag of client in ring buffer
    public NsTime LastXchange;                    // Time of last data transmission/reception

    // Rate tracking
    public ulong[] TxPackets = new ulong[2];      // Track total number of packets transmitted
    public double TxPacketRate;                   // Track rate of packet transmission
    public ulong[] TxBytes = new ulong[2];        // Track total number of data bytes transmitted
    public double TxByteRate;                     // Track rate of data byte transmission
    public ulong[] RxPackets = new ulong[2];      // Track total number of packets received
    public double RxPacketRate;                   // Track rate of packet reception
    public ulong[] RxBytes = new ulong[2];        // Track total number of data bytes received
    public double RxByteRate;                     // Track rate of data byte reception
    public NsTime RateTime;                       // Time stamp for TX/RX rate calculations

    // Extended protocol info
    public object? ExtInfo;                       // Extended client info, protocol specific

    // Additional C#-specific fields for easier management
    public string? StreamId;                      // Current stream ID for this client
}

/// <summary>
/// Structure used as the data for B-tree of stream tracking.
/// Equivalent to StreamNode in clients.h.
/// </summary>
public class StreamNode
{
    public string StreamId = "";                  // Stream ID
    public ulong TxPackets;                       // Total packets transmitted
    public ulong TxBytes;                         // Total bytes transmitted
    public ulong RxPackets;                       // Total packets received
    public ulong RxBytes;                         // Total bytes received
    public bool EndTimeReached;                   // End time reached, for window requests
}

/// <summary>
/// IPNet: A structure to list IP address ranges.
/// Equivalent to IPNet_s in ringserver.h.
/// </summary>
public class IPNet
{
    public byte[]? Network;                       // Network address in network byte order
    public byte[]? Netmask;                       // Netmask in network byte order
    public AddressFamily Family;                  // AddressFamily.InterNetwork or InterNetworkV6
    public string? LimitStr;                      // Limit string
    public IPNet? Next;                           // Next entry in linked list
}

/// <summary>
/// Listen port parameters.
/// Equivalent to ListenPortParams in ringserver.h.
/// </summary>
public class ListenPortParams
{
    public string PortStr = "";                    // Port number as string
    public ListenProtocols Protocols;              // Protocol flags for this connection
    public ListenOptions Options;                  // Options for this connection
    public Socket? ListenSocket;                   // Listening socket
}