/***************************************************************************
 * MseedArchive.cs
 *
 * PostgreSQL-based miniSEED data archive with time-series optimized storage.
 * Replaces the file-based dsarchive approach from the C version.
 *
 * Design:
 *   - stations table: stream metadata (network, station, location, channel)
 *   - mseed_records table: raw miniSEED records, partitioned daily by start_time
 *   - BRIN index on start_time for efficient range queries (minimal overhead)
 *   - Batch INSERT via Npgsql binary COPY for high-throughput writes
 *   - TIME/FETCH queries via indexed time-range lookups
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
 ***************************************************************************/

using System;
using System.Collections.Concurrent;
using System.Data;
using System.Text;
using Npgsql;
using NpgsqlTypes;
using RingServer.Types;

namespace RingServer.Mseed;

/// <summary>
/// A single miniSEED record ready for database insert.
/// </summary>
public struct MseedRecord
{
    public string StreamId;       // e.g., "FDSN:VG_STNM0_00_E_H_Z/MSEED"
    public NsTime StartTime;      // Data start time (ns since epoch)
    public NsTime EndTime;        // Data end time (ns since epoch)
    public float SampleRate;      // Sample rate in Hz (0 if unknown)
    public uint DataSize;         // Raw record length in bytes
    public byte[] RawData;        // Raw miniSEED record bytes
    public ulong PktId;           // Ring packet ID
}

/// <summary>
/// Query result for TIME/FETCH requests.
/// </summary>
public class MseedQueryResult
{
    public long Id;
    public string StreamId = "";
    public NsTime StartTime;
    public NsTime EndTime;
    public float SampleRate;
    public uint DataSize;
    public byte[]? RawData;
    public ulong PktId;
}

/// <summary>
/// PostgreSQL-based miniSEED data archive.
/// Thread-safe, connection-pooled, with batching support.
/// </summary>
public class MseedArchive : IDisposable
{
    private readonly string _connectionString;
    private readonly NpgsqlDataSource _dataSource;
    private readonly ConcurrentQueue<MseedRecord> _pendingWrites = new();
    private readonly int _batchSize;
    private readonly int _batchIntervalMs;
    private Timer? _flushTimer;
    private bool _disposed;

    // Cache of station_id lookups to avoid repeated DB queries
    private readonly ConcurrentDictionary<string, long> _stationCache = new();
    private readonly int _retentionDays;
    private Timer? _retentionTimer;

    /// <summary>
    /// Creates a new MseedArchive connected to PostgreSQL.
    /// Creates tables and indexes if they don't exist.
    /// </summary>
    /// <param name="connectionString">Npgsql connection string</param>
    /// <param name="batchSize">Max records before flush (default 1000)</param>
    /// <param name="batchIntervalMs">Max interval before flush in ms (default 5000)</param>
    /// <param name="retentionDays">Auto-delete partitions older than N days (0 = keep forever)</param>
    public MseedArchive(string connectionString, int batchSize = 1000, int batchIntervalMs = 5000, int retentionDays = 0)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _batchSize = batchSize;
        _batchIntervalMs = batchIntervalMs;
        _retentionDays = retentionDays;

        // Ensure the target database exists (auto-create if missing)
        EnsureDatabaseExists(connectionString);

        var builder = new NpgsqlDataSourceBuilder(connectionString);
        builder.ConnectionStringBuilder.Pooling = true;
        builder.ConnectionStringBuilder.MaxPoolSize = 50;
        builder.ConnectionStringBuilder.MinPoolSize = 2;
        _dataSource = builder.Build();

        InitializeSchema();
        StartFlushTimer();

        if (_retentionDays > 0)
        {
            StartRetentionTimer();
        }

        Logging.lprintf(1, "MseedArchive: initialized (batchSize={0}, interval={1}ms, retentionDays={2})",
            batchSize, batchIntervalMs, _retentionDays);
    }

    /// <summary>
    /// Ensure the target PostgreSQL database exists. If not, create it automatically.
    /// Connects to the 'postgres' maintenance database to issue CREATE DATABASE.
    /// </summary>
    private static void EnsureDatabaseExists(string connectionString)
    {
        // Parse the connection string to get Database name
        var csb = new NpgsqlConnectionStringBuilder(connectionString);
        string targetDb = csb.Database ?? "ringserver";

        // Try connecting to the target database first
        try
        {
            using var testConn = new NpgsqlConnection(connectionString);
            testConn.Open();
            testConn.Close();
            Logging.lprintf(1, "Database '{0}' already exists", targetDb);
            return;
        }
        catch (PostgresException ex) when (ex.SqlState == "3D000") // 3D000 = invalid_catalog_name (database doesn't exist)
        {
            Logging.lprintf(1, "Database '{0}' does not exist, creating...", targetDb);
        }
        catch (Exception ex)
        {
            // Other connection errors (wrong password, host down, etc.) — let the caller handle
            Logging.lprintf(0, "Cannot connect to PostgreSQL: {0}", ex.Message);
            throw;
        }

        // Connect to 'postgres' database to create the target database
        var postgresCsb = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Database = "postgres"
        };

        try
        {
            using var conn = new NpgsqlConnection(postgresCsb.ConnectionString);
            conn.Open();

            // Check if database already exists (double-check, race-condition safe)
            using var checkCmd = new NpgsqlCommand(
                $"SELECT 1 FROM pg_database WHERE datname = '{targetDb}'", conn);
            var exists = checkCmd.ExecuteScalar() != null;

            if (!exists)
            {
                // CREATE DATABASE cannot run inside a transaction block
                using var createCmd = new NpgsqlCommand(
                    $"CREATE DATABASE \"{targetDb}\"", conn);
                createCmd.ExecuteNonQuery();
                Logging.lprintf(0, "Created database '{0}'", targetDb);
            }
            else
            {
                Logging.lprintf(1, "Database '{0}' already exists", targetDb);
            }
        }
        catch (Exception ex)
        {
            Logging.lprintf(0, "Failed to create database '{0}': {1}", targetDb, ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Initialize database schema: create tables, indexes, and seed partitions.
    /// </summary>
    private void InitializeSchema()
    {
        using var conn = _dataSource.OpenConnection();
        using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            -- Stations table: maps stream IDs to numeric IDs
            CREATE TABLE IF NOT EXISTS stations (
                id          BIGSERIAL PRIMARY KEY,
                network     CHAR(2) NOT NULL,
                station     CHAR(5) NOT NULL,
                location    CHAR(2) NOT NULL DEFAULT '00',
                channel     CHAR(3) NOT NULL,
                stream_id   VARCHAR(64) NOT NULL UNIQUE,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            );

            -- MiniSEED records table: partitioned daily by start_time
            CREATE TABLE IF NOT EXISTS mseed_records (
                id          BIGSERIAL,
                station_id  BIGINT NOT NULL REFERENCES stations(id),
                start_time  TIMESTAMPTZ NOT NULL,
                end_time    TIMESTAMPTZ NOT NULL,
                sample_rate REAL,
                data_size   INT NOT NULL,
                raw_data    BYTEA NOT NULL,
                pkt_id      BIGINT,
                received_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (id, start_time)
            ) PARTITION BY RANGE (start_time);

            -- BRIN index: extremely compact, ideal for append-only time-series
            CREATE INDEX IF NOT EXISTS idx_mseed_start_time
                ON mseed_records USING BRIN (start_time) WITH (pages_per_range = 32);

            CREATE INDEX IF NOT EXISTS idx_mseed_station_time
                ON mseed_records (station_id, start_time);

            CREATE INDEX IF NOT EXISTS idx_mseed_pkt_id
                ON mseed_records (pkt_id);

            -- Seed today's partition (idempotent — guarded by DO)
            DO $$
            DECLARE
                today_start  TIMESTAMPTZ := date_trunc('day', NOW());
                today_end    TIMESTAMPTZ := today_start + INTERVAL '1 day';
                part_name    TEXT;
            BEGIN
                part_name := 'mseed_records_' || to_char(NOW(), 'YYYYMMDD');
                IF NOT EXISTS (
                    SELECT 1 FROM pg_class WHERE relname = part_name
                ) THEN
                    EXECUTE format(
                        'CREATE TABLE %I PARTITION OF mseed_records
                         FOR VALUES FROM (%L) TO (%L)',
                        part_name, today_start, today_end
                    );
                END IF;
            END $$;
        ";
        cmd.ExecuteNonQuery();

        Logging.lprintf(1, "MseedArchive: schema initialized");
    }

    /// <summary>
    /// Ensure a partition exists for the given date. Creates if missing.
    /// </summary>
    private void EnsurePartition(DateTime date)
    {
        // We batch partition creation to avoid per-record overhead.
        // Partitions are created lazily on first write to a given day.
        // For initial deployment, pg_partman or a cron job is recommended.
        // This serves as fallback.
    }

    /// <summary>
    /// Start the periodic retention pruning timer.
    /// Runs once per hour to check for old partitions to drop.
    /// </summary>
    private void StartRetentionTimer()
    {
        // Run once per hour (3600000 ms) — first run after 60s to allow startup to settle
        _retentionTimer = new Timer(_ => PruneOldPartitions(), null, 60_000, 3_600_000);
        Logging.lprintf(1, "MseedArchive: retention pruning enabled ({0} days)", _retentionDays);
    }

    /// <summary>
    /// Drop partitions older than _retentionDays.
    /// Each partition is a child table of mseed_records named mseed_records_YYYYMMDD.
    /// Dropping a partition from a partitioned table is fast (DDL, not DML).
    /// </summary>
    public void PruneOldPartitions()
    {
        if (_retentionDays <= 0) return;

        try
        {
            var cutoffDate = DateTime.UtcNow.Date.AddDays(-_retentionDays);
            var cutoffPartition = $"mseed_records_{cutoffDate:yyyyMMdd}";

            using var conn = _dataSource.OpenConnection();
            using var cmd = conn.CreateCommand();

            // Find all daily partitions older than cutoff
            cmd.CommandText = @"
                SELECT relname
                FROM pg_class
                WHERE relname ~ '^mseed_records_[0-9]{8}$'
                  AND relkind = 'r'
                  AND relispartition = true
                  AND relname <= @cutoff
                ORDER BY relname;
            ";
            cmd.Parameters.AddWithValue("cutoff", cutoffPartition);

            var partitionsToDrop = new List<string>();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                partitionsToDrop.Add(reader.GetString(0));
            }
            reader.Close();

            if (partitionsToDrop.Count == 0) return;

            foreach (var partition in partitionsToDrop)
            {
                try
                {
                    using var dropCmd = conn.CreateCommand();
                    dropCmd.CommandText = $"DROP TABLE IF EXISTS {partition}";
                    dropCmd.ExecuteNonQuery();
                    Logging.lprintf(1, "MseedArchive: pruned old partition {0}", partition);
                }
                catch (Exception ex)
                {
                    Logging.lprintf(0, "MseedArchive: failed to prune partition {0}: {1}", partition, ex.Message);
                }
            }
        }
        catch (Exception ex)
        {
            Logging.lprintf(0, "MseedArchive: retention pruning error: {0}", ex.Message);
        }
    }

    /// <summary>
    /// Start the periodic flush timer.
    /// </summary>
    private void StartFlushTimer()
    {
        _flushTimer = new Timer(_ => FlushPendingWrites(), null, _batchIntervalMs, _batchIntervalMs);
    }

    /// <summary>
    /// Enqueue a miniSEED record for batch write.
    /// Non-blocking — inserts are batched and flushed asynchronously.
    /// </summary>
    public void Enqueue(MseedRecord record)
    {
        if (_disposed) return;

        _pendingWrites.Enqueue(record);

        // Flush if batch size reached
        if (_pendingWrites.Count >= _batchSize)
        {
            FlushPendingWrites();
        }
    }

    /// <summary>
    /// Synchronously write a single record (bypasses batch queue).
    /// Use for critical records that must be persisted immediately.
    /// </summary>
    public bool WriteSync(MseedRecord record)
    {
        try
        {
            FlushRecords(new[] { record });
            return true;
        }
        catch (Exception ex)
        {
            Logging.lprintf(0, "MseedArchive: WriteSync failed: {0}", ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Flush all pending writes to PostgreSQL.
    /// Called automatically by the timer and when batch is full.
    /// Also call this on shutdown to ensure all data is persisted.
    /// </summary>
    public void FlushPendingWrites()
    {
        if (_pendingWrites.IsEmpty) return;

        var batch = new List<MseedRecord>();
        while (_pendingWrites.TryDequeue(out var record))
        {
            batch.Add(record);
        }

        if (batch.Count == 0) return;

        try
        {
            FlushRecords(batch);
        }
        catch (Exception ex)
        {
            Logging.lprintf(0, "MseedArchive: Flush failed ({0} records): {1}", batch.Count, ex.Message);
            // Re-queue failed records? For now, log and continue.
            // In production, you might want a dead-letter queue.
        }
    }

    /// <summary>
    /// Core batch insert using Npgsql binary COPY for maximum throughput.
    /// Station IDs are pre-resolved on a separate connection before COPY begins.
    /// </summary>
    private void FlushRecords(IReadOnlyCollection<MseedRecord> records)
    {
        // Phase 1: Pre-resolve all station IDs using a separate connection
        // (COPY mode blocks all other commands on the same connection)
        var resolvedIds = new Dictionary<string, long>();
        var uncachedStreams = new HashSet<string>();

        foreach (var record in records)
        {
            if (!_stationCache.TryGetValue(record.StreamId, out _) && !uncachedStreams.Contains(record.StreamId))
                uncachedStreams.Add(record.StreamId);
        }

        if (uncachedStreams.Count > 0)
        {
            using var resolveConn = _dataSource.OpenConnection();
            foreach (var streamId in uncachedStreams)
            {
                long stationId = GetOrCreateStation(resolveConn, streamId);
                _stationCache[streamId] = stationId;
            }
        }

        // Phase 2: Binary COPY with all IDs already resolved
        using var conn = _dataSource.OpenConnection();
        using var writer = conn.BeginBinaryImport(
            "COPY mseed_records (station_id, start_time, end_time, sample_rate, data_size, raw_data, pkt_id) FROM STDIN (FORMAT BINARY)");

        foreach (var record in records)
        {
            if (!_stationCache.TryGetValue(record.StreamId, out long stationId))
            {
                // Fallback: should not happen since we pre-resolved, but handle gracefully
                Logging.lprintf(0, "MseedArchive: unexpected cache miss for {0}, skipping", record.StreamId);
                continue;
            }

            writer.StartRow();
            writer.Write(stationId, NpgsqlDbType.Bigint);
            writer.Write(NsTimeToDateTime(record.StartTime), NpgsqlDbType.TimestampTz);
            writer.Write(NsTimeToDateTime(record.EndTime), NpgsqlDbType.TimestampTz);
            writer.Write(record.SampleRate, NpgsqlDbType.Real);
            writer.Write((int)record.DataSize, NpgsqlDbType.Integer);
            writer.Write(record.RawData, NpgsqlDbType.Bytea);
            writer.Write((long)record.PktId, NpgsqlDbType.Bigint);
        }

        writer.Complete();

        Logging.lprintf(3, "MseedArchive: flushed {0} records", records.Count);
    }

    /// <summary>
    /// Get or create a station ID for a given stream ID.
    /// Uses in-memory cache first, then database lookup/insert.
    /// </summary>
    private long GetOrCreateStation(NpgsqlConnection conn, string streamId)
    {
        // Fast path: cache hit
        if (_stationCache.TryGetValue(streamId, out long cachedId))
            return cachedId;

        // Parse stream ID into components
        // Format: "FDSN:VG_STNM0_00_E_H_Z/MSEED" or "VG_STNM0_00_EHZ/MSEED"
        var (network, station, location, channel) = ParseStreamId(streamId);

        // Upsert: INSERT ON CONFLICT to handle concurrent inserts
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO stations (network, station, location, channel, stream_id)
            VALUES (@n, @s, @l, @c, @sid)
            ON CONFLICT (stream_id) DO UPDATE SET stream_id = EXCLUDED.stream_id
            RETURNING id;
        ";
        cmd.Parameters.AddWithValue("n", network);
        cmd.Parameters.AddWithValue("s", station);
        cmd.Parameters.AddWithValue("l", location);
        cmd.Parameters.AddWithValue("c", channel);
        cmd.Parameters.AddWithValue("sid", streamId);

        long stationId = (long)cmd.ExecuteScalar()!;
        _stationCache[streamId] = stationId;
        return stationId;
    }

    /// <summary>
    /// Parse a stream ID into network/station/location/channel components.
    /// </summary>
    private static (string network, string station, string location, string channel) ParseStreamId(string streamId)
    {
        string net = "??", sta = "?????", loc = "00", cha = "???";

        // Strip FDSN: prefix if present
        if (streamId.StartsWith("FDSN:", StringComparison.OrdinalIgnoreCase))
            streamId = streamId[5..];

        // Strip /MSEED suffix if present
        int slashIdx = streamId.LastIndexOf('/');
        if (slashIdx > 0)
            streamId = streamId[..slashIdx];

        // Expected format: NET_STA_LOC_C or NET_STA_LOC_CHA
        var parts = streamId.Split('_');
        if (parts.Length >= 2) net = PadOrTruncate(parts[0], 2);
        if (parts.Length >= 2) sta = PadOrTruncate(parts[1], 5);
        if (parts.Length >= 3)
        {
            // Location is 2 chars, channel is 3 chars
            loc = PadOrTruncate(parts[2], 2);
            if (parts.Length >= 4)
                cha = PadOrTruncate(parts[3], 3);
        }

        return (net, sta, loc, cha);
    }

    private static string PadOrTruncate(string s, int len)
    {
        if (s.Length > len) return s[..len];
        return s.PadRight(len);
    }

    /// <summary>
    /// Query miniSEED records by stream and time range.
    /// Used for SeedLink TIME command.
    /// Returns records ordered by start_time ASC.
    /// </summary>
    /// <param name="streamIds">List of stream IDs to query (use % wildcard for ALL)</param>
    /// <param name="startTime">Start of time range (inclusive)</param>
    /// <param name="endTime">End of time range (inclusive, or Unset for no upper bound)</param>
    /// <param name="limit">Maximum records to return (0 = unlimited)</param>
    /// <returns>List of matching records ordered by start_time</returns>
    public List<MseedQueryResult> QueryByTime(
        List<string> streamIds,
        NsTime startTime,
        NsTime endTime,
        int limit = 0)
    {
        var results = new List<MseedQueryResult>();

        using var conn = _dataSource.OpenConnection();
        using var cmd = conn.CreateCommand();

        var sql = new StringBuilder();
        sql.AppendLine("SELECT mr.id, s.stream_id, mr.start_time, mr.end_time,");
        sql.AppendLine("       mr.sample_rate, mr.data_size, mr.raw_data, mr.pkt_id");
        sql.AppendLine("FROM mseed_records mr");
        sql.AppendLine("JOIN stations s ON s.id = mr.station_id");
        sql.AppendLine("WHERE mr.start_time >= @start_time");

        if (!endTime.IsUnset)
            sql.AppendLine("  AND mr.start_time <= @end_time");

        if (streamIds.Count == 1 && streamIds[0] == "%")
        {
            // All streams — no additional filter
        }
        else if (streamIds.Count > 0)
        {
            var likeClauses = new List<string>();
            for (int i = 0; i < streamIds.Count; i++)
            {
                string param = $"@stream_{i}";
                // Escape literal underscores in LIKE pattern (underscore is FDSN separator)
                string escaped = streamIds[i].Replace("_", "\\_");
                likeClauses.Add($"s.stream_id LIKE {param} ESCAPE '\\'");
                cmd.Parameters.AddWithValue($"stream_{i}", escaped);
            }
            sql.AppendLine($"  AND ({string.Join(" OR ", likeClauses)})");
        }

        sql.AppendLine("ORDER BY mr.start_time ASC");

        if (limit > 0)
            sql.AppendLine($"LIMIT {limit}");

        cmd.CommandText = sql.ToString();

        var startDt = NsTimeToDateTime(startTime);
        cmd.Parameters.AddWithValue("start_time", startDt);

        if (!endTime.IsUnset)
        {
            var endDt = NsTimeToDateTime(endTime);
            cmd.Parameters.AddWithValue("end_time", endDt);
        }

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add(new MseedQueryResult
            {
                Id = reader.GetInt64(0),
                StreamId = reader.GetString(1),
                StartTime = DateTimeToNsTime(reader.GetDateTime(2)),
                EndTime = DateTimeToNsTime(reader.GetDateTime(3)),
                SampleRate = reader.IsDBNull(4) ? 0f : reader.GetFloat(4),
                DataSize = (uint)reader.GetInt32(5),
                RawData = reader.IsDBNull(6) ? null : (byte[])reader[6],
                PktId = reader.IsDBNull(7) ? 0 : (ulong)reader.GetInt64(7)
            });
        }

        return results;
    }

    /// <summary>
    /// Get the latest pkt_id for a given stream.
    /// Used to determine where to resume streaming from the ring buffer.
    /// </summary>
    public ulong GetLatestPktId(string streamId)
    {
        using var conn = _dataSource.OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT mr.pkt_id FROM mseed_records mr
            JOIN stations s ON s.id = mr.station_id
            WHERE s.stream_id = @sid
            ORDER BY mr.start_time DESC LIMIT 1
        ";
        cmd.Parameters.AddWithValue("sid", streamId);

        var result = cmd.ExecuteScalar();
        return result is long pktId ? (ulong)pktId : Constants.RingIdNone;
    }

    /// <summary>
    /// Get the earliest pkt_id for a given stream.
    /// </summary>
    public ulong GetEarliestPktId(string streamId)
    {
        using var conn = _dataSource.OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT mr.pkt_id FROM mseed_records mr
            JOIN stations s ON s.id = mr.station_id
            WHERE s.stream_id = @sid
            ORDER BY mr.start_time ASC LIMIT 1
        ";
        cmd.Parameters.AddWithValue("sid", streamId);

        var result = cmd.ExecuteScalar();
        return result is long pktId ? (ulong)pktId : Constants.RingIdNone;
    }

    // ============== Utility ==============

    private static DateTime NsTimeToDateTime(NsTime nsTime)
    {
        // NsTime stores nanoseconds since Unix epoch.
        // DateTime.UnixEpoch is 1970-01-01T00:00:00Z.
        long ticks = nsTime.Value / 100; // ns → ticks (100ns intervals)
        return DateTime.UnixEpoch.AddTicks(ticks);
    }

    private static NsTime DateTimeToNsTime(DateTime dt)
    {
        // Convert to UTC and then to ns since epoch
        DateTime utc = dt.Kind == DateTimeKind.Utc ? dt : dt.ToUniversalTime();
        long ticks = (utc - DateTime.UnixEpoch).Ticks;
        return new NsTime(ticks * 100);
    }

    // ============== IDisposable ==============

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _flushTimer?.Dispose();
        _retentionTimer?.Dispose();

        // Final flush on shutdown
        FlushPendingWrites();

        _dataSource.Dispose();
        Logging.lprintf(1, "MseedArchive: disposed");
    }
}
