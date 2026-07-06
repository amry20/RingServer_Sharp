/**************************************************************************
 * HttpProtocol.cs
 *
 * HTTP protocol handler.
 * Equivalent to http.c / http.h in the C version.
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
using RingServer.Config;
using RingServer.Net;
using RingServer.Ring;
using RingServer.Types;

namespace RingServer.Protocols;

/// <summary>
/// HTTP protocol handler.
/// Equivalent to http.c in the C version.
/// </summary>
public static class HttpProtocol
{
    /// <summary>
    /// Handle an HTTP request from the client.
    /// Returns 0 on success, non-zero to close connection.
    /// Equivalent to HandleHTTP() in http.c.
    /// </summary>
    public static int HandleRequest(ClientInfo cinfo)
    {
        if (cinfo.RecvLength <= 0)
            return 0;

        string request = Encoding.ASCII.GetString(cinfo.RecvBuffer!, 0, cinfo.RecvLength);

        Logging.lprintf(3, "[{0}] HTTP request: {1}", cinfo.Hostname,
            request.Split('\n')[0].Trim());

        // Parse the request line
        var lines = request.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        if (lines.Length == 0)
            return 0;

        var requestLine = lines[0].Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (requestLine.Length < 2)
        {
            SendHttpError(cinfo, 400, "Bad Request");
            return 0;
        }

        string method = requestLine[0].ToUpperInvariant();
        string path = requestLine[1];

        if (method == "GET")
        {
            return HandleGet(cinfo, path);
        }
        else if (method == "POST")
        {
            return HandlePost(cinfo, path, request);
        }
        else
        {
            SendHttpError(cinfo, 405, "Method Not Allowed");
            return 0;
        }
    }

    /// <summary>
    /// Handle HTTP GET request.
    /// </summary>
    private static int HandleGet(ClientInfo cinfo, string path)
    {
        // Serve ring info or static files
        if (path == "/" || path == "/index.html")
        {
            if (!string.IsNullOrEmpty(ServerConfig.Instance.WebRoot))
            {
                return ServeStaticFile(cinfo, path);
            }
            else
            {
                return ServeRingInfo(cinfo);
            }
        }
        else if (path.StartsWith("/info"))
        {
            return ServeRingInfo(cinfo);
        }
        else if (path.StartsWith("/streams"))
        {
            return ServeStreamList(cinfo);
        }
        else
        {
            return ServeStaticFile(cinfo, path);
        }
    }

    /// <summary>
    /// Handle HTTP POST request (data submission).
    /// </summary>
    private static int HandlePost(ClientInfo cinfo, string path, string request)
    {
        // Simple data submission via POST
        if (path == "/data" || path == "/dl")
        {
            // Extract body after headers
            int bodyStart = request.IndexOf("\n\n");
            if (bodyStart < 0)
                bodyStart = request.IndexOf("\r\n\r\n");
            if (bodyStart < 0)
                return 0;

            bodyStart = request.IndexOf('\n', bodyStart + 1);
            if (bodyStart < 0) bodyStart = request.Length;

            string body = request[bodyStart..].Trim();

            if (!string.IsNullOrEmpty(body))
            {
                // Parse as DataLink-style data
                var packet = new RingPacket
                {
                    StreamId = "HTTP",
                    PktId = Constants.RingIdNone,
                    DataStart = Generic.NSnow(),
                    DataEnd = Generic.NSnow(),
                    DataSize = (uint)body.Length
                };

                byte[] data = Encoding.ASCII.GetBytes(body);
                var ringBuffer = new RingBuffer(cinfo.RingParams);
                ringBuffer.Write(packet, data, (uint)body.Length);
            }

            SendHttpOk(cinfo, "OK");
            return 0;
        }

        SendHttpError(cinfo, 404, "Not Found");
        return 0;
    }

    /// <summary>
    /// Serve ring info as HTML.
    /// </summary>
    private static int ServeRingInfo(ClientInfo cinfo)
    {
        var sb = new StringBuilder();
        sb.Append("<!DOCTYPE html><html><head><title>RingServer Info</title>");
        sb.Append("<style>body{font-family:sans-serif;margin:2em}");
        sb.Append("table{border-collapse:collapse}td,th{border:1px solid #ddd;padding:8px}");
        sb.Append("tr:nth-child(even){background:#f5f5f5}</style></head><body>");
        sb.Append("<h1>RingServer</h1>");

        var ringBuffer = new RingBuffer(cinfo.RingParams);
        var rp = cinfo.RingParams;

        sb.Append($"<h2>Ring Parameters</h2>");
        sb.Append($"<table><tr><th>Parameter</th><th>Value</th></tr>");
        sb.Append($"<tr><td>Ring Size</td><td>{Generic.HumanSizeString(rp.RingSize)}</td></tr>");
        sb.Append($"<tr><td>Packet Size</td><td>{Generic.HumanSizeString(rp.PktSize)}</td></tr>");
        sb.Append($"<tr><td>Max Packets</td><td>{rp.MaxPackets}</td></tr>");
        sb.Append($"<tr><td>Stream Count</td><td>{rp.StreamCount}</td></tr>");
        sb.Append($"<tr><td>Earliest Packet</td><td>{rp.EarliestId}</td></tr>");
        sb.Append($"<tr><td>Latest Packet</td><td>{rp.LatestId}</td></tr>");
        sb.Append($"<tr><td>TX Rate</td><td>{rp.TxPacketRate:F1} pkt/s, {rp.TxByteRate:F1} B/s</td></tr>");
        sb.Append($"<tr><td>RX Rate</td><td>{rp.RxPacketRate:F1} pkt/s, {rp.RxByteRate:F1} B/s</td></tr>");
        sb.Append("</table>");

        // List streams
        var streams = ringBuffer.GetStreamsStack(cinfo.Reader);
        sb.Append("<h2>Streams</h2><ul>");
        while (streams.NotEmpty)
        {
            var stream = (RingStream)streams.Pop()!;
            sb.Append($"<li>{stream.StreamId}</li>");
        }
        sb.Append("</ul>");

        sb.Append("</body></html>");

        byte[] data = Encoding.UTF8.GetBytes(sb.ToString());
        SendHttpResponse(cinfo, data, "text/html");
        return 0;
    }

    /// <summary>
    /// Serve stream list as plain text.
    /// </summary>
    private static int ServeStreamList(ClientInfo cinfo)
    {
        var ringBuffer = new RingBuffer(cinfo.RingParams);
        var streams = ringBuffer.GetStreamsStack(cinfo.Reader);

        var sb = new StringBuilder();
        while (streams.NotEmpty)
        {
            var stream = (RingStream)streams.Pop()!;
            sb.AppendLine(stream.StreamId);
        }

        byte[] data = Encoding.ASCII.GetBytes(sb.ToString());
        SendHttpResponse(cinfo, data, "text/plain");
        return 0;
    }

    /// <summary>
    /// Serve a static file from the web root.
    /// </summary>
    private static int ServeStaticFile(ClientInfo cinfo, string path)
    {
        string webRoot = ServerConfig.Instance.WebRoot ?? ".";
        string filePath = Path.Combine(webRoot, path.TrimStart('/'));
        filePath = Path.GetFullPath(filePath);

        // Security: ensure path is within web root
        if (!filePath.StartsWith(Path.GetFullPath(webRoot), StringComparison.OrdinalIgnoreCase))
        {
            SendHttpError(cinfo, 403, "Forbidden");
            return 0;
        }

        if (!File.Exists(filePath))
        {
            SendHttpError(cinfo, 404, "Not Found");
            return 0;
        }

        try
        {
            byte[] data = File.ReadAllBytes(filePath);
            string contentType = GetContentType(filePath);
            SendHttpResponse(cinfo, data, contentType);
        }
        catch
        {
            SendHttpError(cinfo, 500, "Internal Server Error");
        }

        return 0;
    }

    /// <summary>
    /// Send an HTTP response with content.
    /// </summary>
    private static void SendHttpResponse(ClientInfo cinfo, byte[] body, string contentType)
    {
        var header = new StringBuilder();
        header.Append("HTTP/1.1 200 OK\r\n");
        header.Append($"Content-Type: {contentType}\r\n");
        header.Append($"Content-Length: {body.Length}\r\n");

        // Add custom headers if configured
        if (!string.IsNullOrEmpty(cinfo.HttpHeaders))
        {
            foreach (var h in cinfo.HttpHeaders.Split(';', StringSplitOptions.RemoveEmptyEntries))
            {
                header.Append(h.Trim());
                header.Append("\r\n");
            }
        }

        header.Append("Connection: close\r\n");
        header.Append("\r\n");

        byte[] headerBytes = Encoding.ASCII.GetBytes(header.ToString());
        int result = SendData.SendToClient(cinfo, headerBytes, headerBytes.Length, false);
        if (result == 0 && body.Length > 0)
        {
            SendData.SendToClient(cinfo, body, body.Length, false);
        }
    }

    /// <summary>
    /// Send HTTP OK response.
    /// </summary>
    private static void SendHttpOk(ClientInfo cinfo, string message)
    {
        byte[] data = Encoding.ASCII.GetBytes(message);
        SendHttpResponse(cinfo, data, "text/plain");
    }

    /// <summary>
    /// Send HTTP error response.
    /// </summary>
    private static void SendHttpError(ClientInfo cinfo, int code, string message)
    {
        string body = $"<html><body><h1>{code} {message}</h1></body></html>";
        byte[] data = Encoding.ASCII.GetBytes(body);

        var header = new StringBuilder();
        header.Append($"HTTP/1.1 {code} {message}\r\n");
        header.Append("Content-Type: text/html\r\n");
        header.Append($"Content-Length: {data.Length}\r\n");
        header.Append("Connection: close\r\n");
        header.Append("\r\n");

        byte[] headerBytes = Encoding.ASCII.GetBytes(header.ToString());
        SendData.SendToClient(cinfo, headerBytes, headerBytes.Length, false);
        SendData.SendToClient(cinfo, data, data.Length, false);
    }

    /// <summary>
    /// Get MIME content type from file extension.
    /// </summary>
    private static string GetContentType(string filePath)
    {
        string ext = Path.GetExtension(filePath).ToLowerInvariant();
        return ext switch
        {
            ".html" => "text/html",
            ".css" => "text/css",
            ".js" => "application/javascript",
            ".json" => "application/json",
            ".png" => "image/png",
            ".jpg" or ".jpeg" => "image/jpeg",
            ".gif" => "image/gif",
            ".svg" => "image/svg+xml",
            ".ico" => "image/x-icon",
            ".txt" => "text/plain",
            ".xml" => "application/xml",
            ".zip" => "application/zip",
            ".gz" => "application/gzip",
            _ => "application/octet-stream"
        };
    }
}
