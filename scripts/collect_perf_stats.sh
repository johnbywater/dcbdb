#!/bin/bash
# Performance monitoring script for grpc_read_bench analysis

set -e

# Configuration
DURATION=${1:-30}  # Default 30 seconds
OUTPUT_DIR="./perf_analysis_$(date +%Y%m%d_%H%M%S)"
BENCHMARK_CMD=${2:-"cargo bench -p umadb-benches --bench grpc_read_bench"}

echo "=================================="
echo "Performance Analysis Collection"
echo "=================================="
echo "Duration: ${DURATION} seconds"
echo "Output directory: ${OUTPUT_DIR}"
echo "Benchmark command: ${BENCHMARK_CMD}"
echo ""

# Create output directory
mkdir -p "${OUTPUT_DIR}"

# Function to find the server process
find_server_pid() {
    pgrep -f "umadb.*server" | head -1
}

# Function to cleanup background jobs
cleanup() {
    echo ""
    echo "Cleaning up monitoring processes..."
    jobs -p | xargs kill 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

echo "Starting background monitoring tools..."

# 1. System-level monitoring with vmstat
echo "  - vmstat (system stats)"
vmstat 1 > "${OUTPUT_DIR}/vmstat.log" &
VMSTAT_PID=$!

# 2. I/O monitoring with iostat (if available)
if command -v iostat &> /dev/null; then
    echo "  - iostat (disk I/O)"
    iostat -x 1 > "${OUTPUT_DIR}/iostat.log" &
    IOSTAT_PID=$!
fi

# 3. CPU monitoring per-core
echo "  - mpstat (per-CPU stats)"
if command -v mpstat &> /dev/null; then
    mpstat -P ALL 1 > "${OUTPUT_DIR}/mpstat.log" &
    MPSTAT_PID=$!
fi

# 4. Memory monitoring
echo "  - memory stats"
while true; do
    date +%s >> "${OUTPUT_DIR}/memory.log"
    vm_stat >> "${OUTPUT_DIR}/memory.log" 2>&1 || free -h >> "${OUTPUT_DIR}/memory.log" 2>&1
    sleep 1
done &
MEM_PID=$!

echo ""
echo "Starting benchmark..."
echo "Command: ${BENCHMARK_CMD}"
echo ""

# Start the benchmark in background
${BENCHMARK_CMD} > "${OUTPUT_DIR}/benchmark_output.log" 2>&1 &
BENCH_PID=$!

# Wait a bit for the server to start
sleep 3

# Try to find the server PID
echo "Waiting for server process..."
for i in {1..10}; do
    SERVER_PID=$(find_server_pid)
    if [ -n "$SERVER_PID" ]; then
        echo "Found server PID: ${SERVER_PID}"
        break
    fi
    sleep 1
done

if [ -z "$SERVER_PID" ]; then
    echo "Warning: Could not find server process. CPU profiling will be skipped."
else
    # 5. CPU profiling with sample (macOS) or perf (Linux)
    echo "Starting CPU profiling..."
    if command -v sample &> /dev/null; then
        # macOS
        echo "  - sample (macOS CPU profiling)"
        sample "$SERVER_PID" ${DURATION} -f "${OUTPUT_DIR}/sample.txt" &
        SAMPLE_PID=$!
    elif command -v perf &> /dev/null; then
        # Linux
        echo "  - perf (Linux CPU profiling)"
        perf record -F 99 -g --call-graph dwarf -p "$SERVER_PID" -o "${OUTPUT_DIR}/perf.data" -- sleep ${DURATION} &
        PERF_PID=$!
    fi

    # 6. Thread monitoring
    echo "  - thread monitoring"
    {
        echo "Timestamp,Threads,CPU%,VSZ,RSS"
        while kill -0 "$SERVER_PID" 2>/dev/null; do
            ps -p "$SERVER_PID" -M -o lstart,pid,thcount,pcpu,vsz,rss 2>/dev/null | tail -n +2 || break
            sleep 1
        done
    } > "${OUTPUT_DIR}/threads.log" &
    THREAD_PID=$!

    # 7. System calls monitoring (optional, can be slow)
    if command -v dtruss &> /dev/null && [ "$ENABLE_DTRACE" = "1" ]; then
        echo "  - dtruss (system calls)"
        sudo dtruss -p "$SERVER_PID" -c > "${OUTPUT_DIR}/syscalls.log" 2>&1 &
        DTRACE_PID=$!
    fi
fi

# Wait for benchmark to complete
wait $BENCH_PID
BENCH_EXIT=$?

echo ""
echo "Benchmark completed with exit code: $BENCH_EXIT"
echo ""

# Give monitoring tools a moment to flush
sleep 2

# Stop monitoring
cleanup

echo ""
echo "=================================="
echo "Generating Analysis Report"
echo "=================================="

# Generate summary report
cat > "${OUTPUT_DIR}/REPORT.md" << 'EOREPORT'
# Performance Analysis Report

## Files Generated

- `benchmark_output.log` - Criterion benchmark results
- `vmstat.log` - System CPU, memory, context switches
- `iostat.log` - Disk I/O statistics (if available)
- `mpstat.log` - Per-CPU utilization (if available)
- `memory.log` - Memory usage over time
- `threads.log` - Thread count and resource usage
- `sample.txt` or `perf.data` - CPU profiling data

## Quick Analysis Commands

### 1. Check CPU utilization trends
```bash
# Average CPU usage
grep -v procs vmstat.log | awk '{sum+=$13} END {print "Avg user CPU:", sum/NR "%"}'
grep -v procs vmstat.log | awk '{sum+=$14} END {print "Avg system CPU:", sum/NR "%"}'
grep -v procs vmstat.log | awk '{sum+=$16} END {print "Avg idle CPU:", sum/NR "%"}'
```

### 2. Check context switches
```bash
# Context switches per second (high = contention)
grep -v procs vmstat.log | awk '{print $12}' | sort -n | tail -20
```

### 3. Analyze CPU profile (macOS)
```bash
# View sample profile
less sample.txt
# Look for hot functions and time spent
```

### 4. Analyze CPU profile (Linux)
```bash
# Generate perf report
perf report -i perf.data
# Generate flamegraph (if installed)
perf script -i perf.data | stackcollapse-perf.pl | flamegraph.pl > flamegraph.svg
```

### 5. Check thread count over time
```bash
# Thread count progression
awk -F, 'NR>1 {print $2}' threads.log | sort -n | uniq -c
```

## Key Metrics to Look For

### Symptoms of the Bottleneck

**If CPU bound but not scaling:**
- User CPU% stays flat despite more clients
- Low idle% but throughput doesn't increase
- → Check for single-threaded hot path in CPU profile

**If lock contention:**
- High system CPU% (context switches)
- High context switch rate in vmstat (cs column)
- → Look for mutex/lock waits in CPU profile

**If blocking thread pool saturated:**
- Thread count plateaus at 512 (or max_blocking_threads)
- → Requests are queuing for available threads

**If I/O bound:**
- High iowait% in mpstat
- High await in iostat
- → Disk is bottleneck (unlikely with mmap)

**If memory bandwidth:**
- Check page faults in vmstat (si/so columns)
- → Memory-mapped read performance degradation
EOREPORT

echo "Report written to: ${OUTPUT_DIR}/REPORT.md"
echo ""

# Basic analysis
echo "=== Quick Stats ==="
echo ""
echo "Context Switches (samples from vmstat):"
grep -v procs "${OUTPUT_DIR}/vmstat.log" 2>/dev/null | awk '{print $12}' | sort -n | tail -5 | head -1 && echo "  (low sample)"
grep -v procs "${OUTPUT_DIR}/vmstat.log" 2>/dev/null | awk '{print $12}' | sort -n | tail -1 && echo "  (high sample)"

echo ""
echo "CPU Utilization (samples from vmstat):"
echo -n "  Avg User: "
grep -v procs "${OUTPUT_DIR}/vmstat.log" 2>/dev/null | awk '{sum+=$13; n++} END {print sum/n "%"}'
echo -n "  Avg System: "
grep -v procs "${OUTPUT_DIR}/vmstat.log" 2>/dev/null | awk '{sum+=$14; n++} END {print sum/n "%"}'
echo -n "  Avg Idle: "
grep -v procs "${OUTPUT_DIR}/vmstat.log" 2>/dev/null | awk '{sum+=$16; n++} END {print sum/n "%"}'

echo ""
echo "Full analysis available in: ${OUTPUT_DIR}/"
echo ""
echo "Next steps:"
echo "  1. Review ${OUTPUT_DIR}/REPORT.md"
echo "  2. Examine CPU profile in sample.txt or perf.data"
echo "  3. Check benchmark_output.log for throughput numbers"
echo "  4. Look for patterns in vmstat.log context switches"
