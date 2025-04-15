package smt

import (
	"testing"
	"time"
)

// BenchmarkBatchAddOperation benchmarks the add operation for batching
func BenchmarkBatchAddOperation(b *testing.B) {
	b.Run("WithoutBatching", func(b *testing.B) {
		// Create tasks without using batching
		tasks := make([]BatchableTask, 0, b.N)
		
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			task := &MockBatchableTask{
				MockTask: MockTask{id: "task-nobatch"},
				batchKey: "key1",
			}
			tasks = append(tasks, task)
		}
	})
	
	b.Run("WithBatching", func(b *testing.B) {
		// No-op process function
		processFn := func(batch *TaskBatch) {}
		
		batcher := NewTaskBatcher(100, 1*time.Hour, processFn)
		
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			task := &MockBatchableTask{
				MockTask: MockTask{id: "task-batch"},
				batchKey: "key1",
			}
			batcher.Add(task)
		}
	})
}

// BenchmarkServerBatchAddOperation benchmarks the add operation for server-based batching
func BenchmarkServerBatchAddOperation(b *testing.B) {
	// No-op process function
	processFn := func(batch *TaskBatch) {}
	
	b.Run("WithoutServerBatching", func(b *testing.B) {
		// Create tasks without using batching
		tasks := make([]ServerBatchableTask, 0, b.N)
		
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			task := &MockBatchableTaskWithServer{
				MockTask: MockTask{id: "task-noserverbatch"},
				batchKey: "key1",
				targetServer: "server1",
			}
			tasks = append(tasks, task)
		}
	})
	
	b.Run("WithServerBatching", func(b *testing.B) {
		batcher := NewServerTaskBatcher(100, 1*time.Hour, processFn)
		
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			task := &MockBatchableTaskWithServer{
				MockTask: MockTask{id: "task-serverbatch"},
				batchKey: "key1",
				targetServer: "server1",
			}
			batcher.Add(task)
		}
	})
	
	b.Run("WithMultiServerBatching", func(b *testing.B) {
		batcher := NewServerTaskBatcher(100, 1*time.Hour, processFn)
		
		servers := []string{"server1", "server2", "server3", "server4"}
		serverCount := len(servers)
		
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			server := servers[i%serverCount]
			task := &MockBatchableTaskWithServer{
				MockTask: MockTask{id: "task-multiserverbatch"},
				batchKey: "key1",
				targetServer: server,
			}
			batcher.Add(task)
		}
	})
}