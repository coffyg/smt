#!/bin/bash

echo "Running custom benchmarks..."
echo ""

echo "=== Idle/Light Load Benchmarks ==="
go test -bench="BenchmarkTaskManager" -benchtime=10s -run=^$ | grep -E "(Benchmark|ns/op|allocs)"

echo ""
echo "=== Concurrent AddTask Benchmarks ==="
go test -bench="BenchmarkConcurrentAddTask" -benchtime=5s -run=^$ | grep -E "(Benchmark|tasks/sec|ns/op)"

echo ""
echo "=== Global Lock Contention Benchmarks ==="
go test -bench="BenchmarkGlobalLockContention" -benchtime=5s -run=^$ | grep -E "(Benchmark|ns/op)"

echo ""
echo "=== AddTask Stress Test ==="
go test -run=TestAddTaskStress -v | grep -E "(Stress test completed|Total tasks|Duration|Throughput|Fast adds|Slow adds)"