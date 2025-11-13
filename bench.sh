#!/bin/bash

# Run SMT concurrent benchmark
echo "[1/2] Running SMT BenchmarkConcurrentAddTask..."
cd /home/fy/Dev/coffyg/smt
go test -bench=BenchmarkConcurrentAddTask -benchtime=3s -timeout=5m > /tmp/smt_concurrent_bench.txt 2>&1
cat /tmp/smt_concurrent_bench.txt | grep "Benchmark\|tasks/sec"

echo ""
echo "[2/2] Running SMT stress test..."
go test -run TestAddTaskStress -v -timeout=5m > /tmp/smt_stress.txt 2>&1
cat /tmp/smt_stress.txt | grep "Throughput\|Duration\|Total tasks"

echo ""
echo "=========================================="
echo ""