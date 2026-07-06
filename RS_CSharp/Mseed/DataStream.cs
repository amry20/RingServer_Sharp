/***************************************************************************
 * DataStream.cs
 *
 * Routines to archive miniSEED data records, specialized for ringserver.
 *
 * This file is part of the ringserver C# port.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 ***************************************************************************/

using System;

namespace RingServer;

/// <summary>
/// Data stream group for miniSEED archiving.
/// Equivalent to DataStreamGroup in dsarchive.h.
/// </summary>
public class DataStreamGroup
{
    public string? DefKey;
    public bool Filed;
    public DateTime ModTime;
    public string Filename = "";
    public string PostPath = "";
    public DataStreamGroup? Next;
}

/// <summary>
/// DataStream for miniSEED archiving.
/// Equivalent to DataStream in dsarchive.h.
/// </summary>
public class DataStream
{
    public string? Path;
    public int IdleTimeout;
    public int MaxOpenFiles;
    public int OpenFileCount;
    public DataStreamGroup? GroupRoot;
}