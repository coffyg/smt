package smt

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkServerBatchSizes compares different batch sizes
func BenchmarkServerBatchSizes(b *testing.B) {
	// Test various batch sizes
	batchSizes := []int{10, 50, 100, 500, 1000}
	
	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			// Create a no-op process function
			processFn := func(batch *TaskBatch) {}
			
			// Create a batcher with the current batch size
			batcher := NewServerTaskBatcher(batchSize, 1*time.Hour, processFn)
			
			// Pre-allocate tasks for better benchmarking
			tasks := make([]ServerBatchableTask, b.N)
			for i := 0; i < b.N; i++ {
				tasks[i] = &MockBatchableTaskWithServer{
					MockTask: MockTask{id: fmt.Sprintf("task%d", i)},
					batchKey: "key1",
					targetServer: "server1",
				}
			}
			
			b.ResetTimer()
			
			// Add tasks to the batcher
			for i := 0; i < b.N; i++ {
				batcher.Add(tasks[i])
			}
		})
	}
}

// BenchmarkServerCount compares different server distributions
func BenchmarkServerCount(b *testing.B) {
	// Test various server counts
	serverCounts := []int{1, 2, 4, 8, 16}
	
	for _, serverCount := range serverCounts {
		b.Run(fmt.Sprintf("ServerCount_%d", serverCount), func(b *testing.B) {
			// Create servers
			servers := make([]string, serverCount)
			for i := 0; i < serverCount; i++ {
				servers[i] = fmt.Sprintf("server%d", i+1)
			}
			
			// Create a batcher with a fixed batch size
			processFn := func(batch *TaskBatch) {}
			batcher := NewServerTaskBatcher(100, 1*time.Hour, processFn)
			
			// Pre-allocate tasks
			tasks := make([]ServerBatchableTask, b.N)
			for i := 0; i < b.N; i++ {
				serverIndex := i % serverCount
				tasks[i] = &MockBatchableTaskWithServer{
					MockTask: MockTask{id: fmt.Sprintf("task%d", i)},
					batchKey: "key1", 
					targetServer: servers[serverIndex],
				}
			}
			
			b.ResetTimer()
			
			// Add tasks to the batcher
			for i := 0; i < b.N; i++ {
				batcher.Add(tasks[i])
			}
		})
	}
}