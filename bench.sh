#!/bin/bash

# SMT Performance Benchmark Script
# Measures CPU usage and overhead during high load task processing

# Set terminal colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${MAGENTA}SMT Performance Benchmark${NC}"
echo -e "${BLUE}=============================${NC}"

# Check if perf tools are available
if ! command -v perf &> /dev/null; then
    echo -e "${RED}Linux perf tools not found. Install with: sudo apt install linux-tools-common${NC}"
    USE_PERF=0
else
    USE_PERF=1
fi

# Determine CPU cores for proper comparison
CPU_CORES=$(nproc)
echo -e "${BLUE}System has ${CPU_CORES} CPU cores${NC}"

# Create benchmark directory for reports
BENCH_DIR="./benchmark_results"
mkdir -p $BENCH_DIR

# Timestamp for this benchmark run
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${BENCH_DIR}/bench_${TIMESTAMP}.log"

echo -e "${YELLOW}Benchmarking SMT performance...${NC}"
echo -e "${CYAN}Results will be saved to ${LOG_FILE}${NC}"

# Record system baseline (idle)
echo -e "${YELLOW}Measuring baseline CPU usage (5 seconds)...${NC}"
mpstat 1 5 > "${BENCH_DIR}/baseline_${TIMESTAMP}.txt" &
MPSTAT_PID=$!
sleep 5

# Run comprehensive benchmark with CPU and task overhead measurement
echo -e "${YELLOW}Running standard load benchmark...${NC}"

# Instrument Go test with CPU profiling
PROFILE_FILE="${BENCH_DIR}/cpu_profile_${TIMESTAMP}.out"
start_time=$(date +%s.%N)

# Run the massive load test with detailed benchmarking
if [ $USE_PERF -eq 1 ]; then
    # Use perf to get detailed CPU usage and scheduling statistics
    perf stat -d -d -d go test -run=TestMassiveTaskLoad -v 2>&1 | tee "${LOG_FILE}"
else
    # Run the test with Go's built-in profiling
    go test -run=TestMassiveTaskLoad -cpuprofile="$PROFILE_FILE" -v 2>&1 | tee "${LOG_FILE}"
fi

end_time=$(date +%s.%N)

# Calculate total execution time
execution_time=$(echo "$end_time - $start_time" | bc -l)
echo -e "${GREEN}Total benchmark execution time: ${execution_time} seconds${NC}" | tee -a "${LOG_FILE}"

# Extract and analyze the results
echo -e "\n${BLUE}======== Performance Analysis ========${NC}" | tee -a "${LOG_FILE}"

# Extract task throughput
THROUGHPUT=$(grep "Throughput:" "${LOG_FILE}" | grep -o "[0-9]\+\.[0-9]\+" | head -1)
if [ ! -z "$THROUGHPUT" ]; then
    echo -e "${GREEN}Task throughput: ${THROUGHPUT} tasks/second${NC}" | tee -a "${LOG_FILE}"
fi

# Extract execution timing stats
AVG_EXEC=$(grep "Execution times: avg=" "${LOG_FILE}" | sed -E 's/.*avg=([0-9]+)ms.*/\1/')
MIN_EXEC=$(grep "Execution times: avg=" "${LOG_FILE}" | sed -E 's/.*min=([0-9]+)ms.*/\1/')
MAX_EXEC=$(grep "Execution times: avg=" "${LOG_FILE}" | sed -E 's/.*max=([0-9]+)ms.*/\1/')

if [ ! -z "$AVG_EXEC" ]; then
    echo -e "${GREEN}Task execution times:${NC}" | tee -a "${LOG_FILE}"
    echo -e "${GREEN}  Average: ${AVG_EXEC}ms${NC}" | tee -a "${LOG_FILE}"
    echo -e "${GREEN}  Minimum: ${MIN_EXEC}ms${NC}" | tee -a "${LOG_FILE}"
    echo -e "${GREEN}  Maximum: ${MAX_EXEC}ms${NC}" | tee -a "${LOG_FILE}"
fi

# Extract command execution timing
CMD_LINE=$(grep "Command execution times: avg=" "${LOG_FILE}")
if [ ! -z "$CMD_LINE" ]; then
    CMD_AVG=$(echo "$CMD_LINE" | grep -o "avg=[0-9.]\+ms" | sed 's/avg=//')
    CMD_MIN=$(echo "$CMD_LINE" | grep -o "min=[0-9.]\+ms" | sed 's/min=//')
    CMD_MAX=$(echo "$CMD_LINE" | grep -o "max=[0-9.]\+ms" | sed 's/max=//')
fi

if [ ! -z "$CMD_AVG" ]; then
    echo -e "${GREEN}Command execution times:${NC}" | tee -a "${LOG_FILE}"
    echo -e "${GREEN}  Average: ${CMD_AVG}${NC}" | tee -a "${LOG_FILE}"
    echo -e "${GREEN}  Minimum: ${CMD_MIN}${NC}" | tee -a "${LOG_FILE}"
    echo -e "${GREEN}  Maximum: ${CMD_MAX}${NC}" | tee -a "${LOG_FILE}"
fi

# Calculate SMT Overhead
echo -e "\n${BLUE}======== SMT Overhead Analysis ========${NC}" | tee -a "${LOG_FILE}"

# Extract total test completion time
TEST_TIME=$(grep "Test completed in" "${LOG_FILE}" | sed -E 's/.*Test completed in ([0-9.]+) seconds.*/\1/')

# Initialize scores
OVERHEAD_SCORE=0
CPU_SCORE=0

if [ ! -z "$TEST_TIME" ] && [ ! -z "$AVG_EXEC" ]; then
    # Convert to milliseconds for comparison
    TEST_TIME_MS=$(echo "$TEST_TIME * 1000" | bc -l)
    
    # Calculate theoretical time if all tasks were executed sequentially
    TOTAL_TASKS=$(grep "Tasks: " "${LOG_FILE}" | sed -E 's/.*Tasks: ([0-9]+) processed.*/\1/')
    if [ ! -z "$TOTAL_TASKS" ]; then
        THEORETICAL_TIME=$(echo "$TOTAL_TASKS * $AVG_EXEC" | bc -l)
        
        # Calculate overhead
        PARALLELIZATION_FACTOR=$(echo "scale=2; $THEORETICAL_TIME / $TEST_TIME_MS" | bc -l)
        OVERHEAD_FACTOR=$(echo "scale=4; $TEST_TIME_MS / ($TOTAL_TASKS * $MIN_EXEC)" | bc -l)
        
        echo -e "${GREEN}Total tasks processed: ${TOTAL_TASKS}${NC}" | tee -a "${LOG_FILE}"
        echo -e "${GREEN}Theoretical sequential time: ${THEORETICAL_TIME}ms${NC}" | tee -a "${LOG_FILE}"
        echo -e "${GREEN}Actual test time: ${TEST_TIME_MS}ms${NC}" | tee -a "${LOG_FILE}"
        echo -e "${GREEN}Effective parallelization factor: ${PARALLELIZATION_FACTOR}x${NC}" | tee -a "${LOG_FILE}"
        echo -e "${GREEN}Overhead factor (1.0 = perfect): ${OVERHEAD_FACTOR}${NC}" | tee -a "${LOG_FILE}"
        
        # Calculate overhead percentage
        if (( $(echo "$OVERHEAD_FACTOR > 1.0" | bc -l) )); then
            OVERHEAD_PCT=$(echo "scale=2; ($OVERHEAD_FACTOR - 1.0) * 100" | bc -l)
            echo -e "${YELLOW}SMT overhead: ${OVERHEAD_PCT}% relative to optimal processing${NC}" | tee -a "${LOG_FILE}"
            
            # Calculate overhead score (lower is better, 100 = perfect)
            if (( $(echo "$OVERHEAD_PCT < 50" | bc -l) )); then
                OVERHEAD_SCORE=$(echo "scale=0; 100 - $OVERHEAD_PCT" | bc -l)
            else
                OVERHEAD_SCORE=$(echo "scale=0; 50 - ($OVERHEAD_PCT - 50) / 2" | bc -l)
            fi
            
            # Ensure score doesn't go below 0
            if (( $(echo "$OVERHEAD_SCORE < 0" | bc -l) )); then
                OVERHEAD_SCORE=0
            fi
        else
            echo -e "${GREEN}SMT overhead: negligible (less than baseline measurement)${NC}" | tee -a "${LOG_FILE}"
            OVERHEAD_SCORE=100
        fi
    fi
fi

# Compare baseline vs. benchmark CPU usage
CPU_UTIL_PCT=0
if [ -f "${BENCH_DIR}/baseline_${TIMESTAMP}.txt" ]; then
    BASELINE_CPU_IDLE=$(grep "all" "${BENCH_DIR}/baseline_${TIMESTAMP}.txt" | awk '{sum+=$12} END {print sum/NR}')
    BASELINE_CPU_USAGE=$(echo "100 - $BASELINE_CPU_IDLE" | bc -l)
    
    # Extract CPU usage during test if available
    if [ $USE_PERF -eq 1 ]; then
        # Extract from perf output
        CPU_UTIL_LINE=$(grep "CPUs utilized" "${LOG_FILE}")
        if [ ! -z "$CPU_UTIL_LINE" ]; then
            CPU_UTIL=$(echo "$CPU_UTIL_LINE" | grep -o "[0-9]\+\.[0-9]\+" | head -1)
            if [ ! -z "$CPU_UTIL" ]; then
                CPU_UTIL_PCT=$(echo "scale=2; $CPU_UTIL * 100 / $CPU_CORES" | bc -l)
                echo -e "${GREEN}Average CPU utilization: ${CPU_UTIL_PCT}% of all cores${NC}" | tee -a "${LOG_FILE}"
                echo -e "${GREEN}Baseline CPU usage: ${BASELINE_CPU_USAGE}%${NC}" | tee -a "${LOG_FILE}"
                
                # Calculate net CPU usage
                NET_CPU=$(echo "$CPU_UTIL_PCT - $BASELINE_CPU_USAGE" | bc -l)
                echo -e "${GREEN}Net SMT CPU usage: ${NET_CPU}%${NC}" | tee -a "${LOG_FILE}"
            else
                echo -e "${YELLOW}Could not parse CPU utilization from: $CPU_UTIL_LINE${NC}" | tee -a "${LOG_FILE}"
                CPU_UTIL_PCT=0
                NET_CPU=0
            fi
        else
            CPU_UTIL_PCT=0
            NET_CPU=0
            
            # Calculate CPU score (100 = best, based on throughput per CPU usage)
            if [ ! -z "$THROUGHPUT" ] && [ ! -z "$CPU_UTIL_PCT" ] && (( $(echo "$CPU_UTIL_PCT > 0" | bc -l) )); then
                TASK_PER_CPU=$(echo "scale=2; $THROUGHPUT / $CPU_UTIL_PCT" | bc -l)
                echo -e "${GREEN}Tasks per CPU percentage: ${TASK_PER_CPU} tasks/sec per CPU%${NC}" | tee -a "${LOG_FILE}"
                
                # Convert to a score out of 100 (higher is better)
                # Assuming 20+ tasks/sec per CPU% is excellent
                if (( $(echo "$TASK_PER_CPU > 20" | bc -l) )); then
                    CPU_SCORE=100
                else
                    CPU_SCORE=$(echo "scale=0; $TASK_PER_CPU * 5" | bc -l)
                fi
            fi
        fi
    else
        # Without perf, we can still extract throughput
        if [ ! -z "$THROUGHPUT" ]; then
            # Convert to a score based on absolute throughput
            # Assuming 1000+ tasks/sec is excellent
            if (( $(echo "$THROUGHPUT > 1000" | bc -l) )); then
                CPU_SCORE=100
            else
                CPU_SCORE=$(echo "scale=0; $THROUGHPUT / 10" | bc -l)
            fi
        fi
    fi
fi

# Run a zero-load benchmark to test idle CPU usage
echo -e "\n${YELLOW}Running zero-load CPU usage test...${NC}"
cd cmd/cpu_test && go run main.go 2>&1 | tee -a "../../${LOG_FILE}" && cd ../..

# Extract idle memory stats
IDLE_HEAP=$(grep "Heap Alloc:" "${LOG_FILE}" | sed -E 's/.*Heap Alloc: ([0-9]+) bytes.*/\1/')
IDLE_OBJECTS=$(grep "Object Count Growth:" "${LOG_FILE}" | sed -E 's/.*Object Count Growth: ([0-9-]+) objects.*/\1/')

# Calculate overall score
if [ ! -z "$OVERHEAD_SCORE" ] && [ ! -z "$CPU_SCORE" ]; then
    # Weight: 60% CPU score, 40% overhead score
    OVERALL_SCORE=$(echo "scale=0; ($CPU_SCORE * 0.6) + ($OVERHEAD_SCORE * 0.4)" | bc -l)
    
    # Print summary
    echo -e "\n${BLUE}======== SMT Benchmark Summary ========${NC}" | tee -a "${LOG_FILE}"
    echo -e "${GREEN}Tasks processed: ${TOTAL_TASKS}${NC}" | tee -a "${LOG_FILE}"
    
    if [ ! -z "$THROUGHPUT" ]; then
        echo -e "${GREEN}Throughput: ${THROUGHPUT} tasks/second${NC}" | tee -a "${LOG_FILE}"
    fi
    
    if [ ! -z "$AVG_EXEC" ]; then
        echo -e "${GREEN}Average task time: ${AVG_EXEC}ms${NC}" | tee -a "${LOG_FILE}"
    fi
    
    if [ ! -z "$CPU_UTIL_PCT" ]; then
        echo -e "${GREEN}CPU utilization: ${CPU_UTIL_PCT}%${NC}" | tee -a "${LOG_FILE}"
    fi
    
    # Display scores
    echo -e "\n${MAGENTA}Performance Scores (100 = best)${NC}" | tee -a "${LOG_FILE}"
    echo -e "${CYAN}CPU Efficiency Score: ${CPU_SCORE}/100${NC}" | tee -a "${LOG_FILE}"
    echo -e "${CYAN}Overhead Score: ${OVERHEAD_SCORE}/100${NC}" | tee -a "${LOG_FILE}"
    echo -e "${MAGENTA}OVERALL SCORE: ${OVERALL_SCORE}/100${NC}" | tee -a "${LOG_FILE}"
    
    # Add idle memory stats if available
    if [ ! -z "$IDLE_HEAP" ]; then
        IDLE_HEAP_MB=$(echo "scale=2; $IDLE_HEAP / 1048576" | bc -l)
        echo -e "${CYAN}Idle Memory Usage: ${IDLE_HEAP_MB} MB${NC}" | tee -a "${LOG_FILE}"
    fi
fi

echo -e "\n${BLUE}Benchmark complete. Results saved to ${LOG_FILE}${NC}"

# Clean up temporary files
rm -f "${BENCH_DIR}/baseline_${TIMESTAMP}.txt"
if [ $USE_PERF -eq 0 ]; then
    echo -e "${YELLOW}CPU profile saved to: ${PROFILE_FILE}${NC}"
    echo -e "${YELLOW}To analyze: go tool pprof ${PROFILE_FILE}${NC}"
fi