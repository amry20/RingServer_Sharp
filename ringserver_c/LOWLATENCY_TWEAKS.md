# Ringserver Low Latency Tweaks

## Changes Made for Real-Time Streaming

### 1. Client Throttle Reduction (clients.c)

**File:** `src/clients.c`

**Original Values:**
```c
#define THROTTLE_STEPPING 50  /* 50 milliseconds */
#define THROTTLE_MAXIMUM 500  /* 1/2 second */
```

**Optimized Values:**
```c
#define THROTTLE_STEPPING 10  /* 10 milliseconds */
#define THROTTLE_MAXIMUM 100  /* 0.1 second */
```

**Impact:**
- **5x faster response** when no data available (50ms → 10ms)
- **5x faster idle check** (500ms → 100ms)
- Lower CPU usage impact (throttle only active when idle)
- Better responsiveness for real-time streaming

### 2. TCP Configuration (Already Enabled)

Ringserver already has **TCP_NODELAY** enabled by default in `ringserver.c:879`:
```c
setsockopt (clientsocket, tcpprotonumber, TCP_NODELAY, ...);
```

This disables Nagle's algorithm, reducing packet buffering delays.

## Compilation

To apply these changes:

```bash
cd /opt/application/nsi/NSI-AD24_NEO1_SYS/SeedLink/ringserver
make clean
make
```

Or for optimized build:
```bash
make clean
make CFLAGS="-O3 -march=native"
```

## Expected Latency Improvements

| Scenario | Before | After | Improvement |
|----------|--------|-------|-------------|
| Idle detection | 500ms | 100ms | 5x faster |
| Throttle step | 50ms | 10ms | 5x faster |
| Real-time data | ~2-5s | <1s | 2-5x faster |

## Configuration Checklist

### nsi2ew_adsend.d
```bash
MinimumLatencyMode 1         # Immediate flush
RingServerAddress localhost:4001  # Correct DataLink port
UseSeedLink 1                # Enable DataLink
```

### ring.conf
```bash
DataLinkPort 4001            # Match nsi2ew_adsend
Verbosity 2                  # Monitor performance
```

### Verification

Check logs for immediate data flow:
```bash
tail -f /var/log/syslog | grep ringserver
```

Expected output (within 1 second of data arrival):
```
[localhost] Received ID
[client] Positioned ring to packet ID: XXXXX
```

## Performance Tuning Tips

1. **Monitor CPU usage:**
   ```bash
   top -p $(pidof ringserver)
   ```
   - Should be low (~1-5% on idle)
   - Spikes on data bursts are normal

2. **Check network latency:**
   ```bash
   ping localhost  # Should be < 0.1ms
   ```

3. **Verify data flow rate:**
   ```bash
   slinktool -Q localhost:18000
   ```

4. **Test with SWARM:** Data should appear within 1 second of generation

## Rollback

If issues occur, restore original values:
```c
#define THROTTLE_STEPPING 50
#define THROTTLE_MAXIMUM 500
```

Then recompile.

## Additional Optimizations (Optional)

### System-level tuning:

1. **Increase network buffer sizes:**
   ```bash
   # /etc/sysctl.conf
   net.core.rmem_max = 16777216
   net.core.wmem_max = 16777216
   net.ipv4.tcp_rmem = 4096 87380 16777216
   net.ipv4.tcp_wmem = 4096 65536 16777216
   ```

2. **CPU governor for performance:**
   ```bash
   echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
   ```

3. **Disable power management:**
   ```bash
   # Add to /boot/config.txt (Raspberry Pi)
   arm_freq=1800
   force_turbo=1
   ```

## Monitoring Commands

```bash
# Watch ringserver log in real-time
journalctl -u ringserver -f

# Check DataLink connections
netstat -an | grep 4001

# Monitor packet flow
watch -n 1 'slinktool -Q localhost:18000 | head -20'

# Check latency statistics
slinktool -nt localhost:18000
```

## Notes

- Throttle values only affect **idle periods** (no data available)
- When data is flowing, throttle is **disabled** (set to 0)
- These optimizations have **minimal CPU impact**
- Safe for production use on low-power devices (Raspberry Pi, etc.)

## Success Criteria

✅ Data appears in SWARM within **1 second** of generation  
✅ Ringserver CPU usage remains **< 10%**  
✅ No dropped packets in logs  
✅ Smooth waveform display (no gaps)  
