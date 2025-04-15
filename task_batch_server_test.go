package smt

import (
	"sync"
	"testing"
	"time"
)

// MockBatchableTask implements BatchableTask for testing
type MockBatchableTask struct {
	MockTask
	batchKey     string
	canBatchWith func(other BatchableTask) bool
	mergeWith    func(other BatchableTask) BatchableTask
	server       string // Target server for this task
}

func (t *MockBatchableTask) BatchKey() string {
	return t.batchKey
}

func (t *MockBatchableTask) CanBatchWith(other BatchableTask) bool {
	if t.canBatchWith != nil {
		return t.canBatchWith(other)
	}
	// Default implementation: batch with same keys
	return t.batchKey == other.BatchKey()
}

func (t *MockBatchableTask) MergeWith(other BatchableTask) BatchableTask {
	if t.mergeWith != nil {
		return t.mergeWith(other)
	}
	// Default implementation: just return self
	return t
}

func (t *MockBatchableTask) GetTargetServer() string {
	return t.server
}

// Test that tasks with same batch key and server get batched together
func TestServerBatcherSameServerSameBatchKey(t *testing.T) {
	// Process function that counts processed batches
	var processedCount int
	var mu sync.Mutex
	processFn := func(batch *TaskBatch) {
		mu.Lock()
		processedCount++
		mu.Unlock()
	}

	// Create batcher with small wait time for testing
	batcher := NewServerTaskBatcher(5, 50*time.Millisecond, processFn)

	// Create tasks for same server with same batch key
	task1 := &MockBatchableTask{
		MockTask: MockTask{id: "task1", priority: 5},
		batchKey: "key1",
		server:   "server1",
	}
	task2 := &MockBatchableTask{
		MockTask: MockTask{id: "task2", priority: 3},
		batchKey: "key1",
		server:   "server1",
	}
	task3 := &MockBatchableTask{
		MockTask: MockTask{id: "task3", priority: 7},
		batchKey: "key1",
		server:   "server1",
	}

	// Add tasks
	batcher.Add(task1)
	batcher.Add(task2)
	batcher.Add(task3)

	// Check batch size before processing
	server1Batches := batcher.GetBatchesForServer("server1")
	if len(server1Batches) != 1 {
		t.Errorf("Expected 1 batch for server1, got %d", len(server1Batches))
	}
	
	if len(server1Batches) > 0 && server1Batches[0].Size() != 3 {
		t.Errorf("Expected batch size 3, got %d", server1Batches[0].Size())
	}

	// Wait for batch to process due to max wait time
	time.Sleep(100 * time.Millisecond)

	// Verify that batch was processed
	mu.Lock()
	if processedCount != 1 {
		t.Errorf("Expected 1 batch to be processed, got %d", processedCount)
	}
	mu.Unlock()
}

// Test that tasks with different batch keys but same server get separate batches
func TestServerBatcherSameServerDifferentBatchKey(t *testing.T) {
	// Process function that counts processed batches
	var processedCount int
	var mu sync.Mutex
	processFn := func(batch *TaskBatch) {
		mu.Lock()
		processedCount++
		mu.Unlock()
	}

	// Create batcher with small wait time for testing
	batcher := NewServerTaskBatcher(5, 50*time.Millisecond, processFn)

	// Create tasks for same server with different batch keys
	task1 := &MockBatchableTask{
		MockTask: MockTask{id: "task1", priority: 5},
		batchKey: "key1",
		server:   "server1",
	}
	task2 := &MockBatchableTask{
		MockTask: MockTask{id: "task2", priority: 3},
		batchKey: "key2",
		server:   "server1",
	}

	// Add tasks
	batcher.Add(task1)
	batcher.Add(task2)

	// Check batches before processing
	server1Batches := batcher.GetBatchesForServer("server1")
	if len(server1Batches) != 2 {
		t.Errorf("Expected 2 batches for server1, got %d", len(server1Batches))
	}

	// Wait for batches to process due to max wait time
	time.Sleep(100 * time.Millisecond)

	// Verify that batches were processed
	mu.Lock()
	if processedCount != 2 {
		t.Errorf("Expected 2 batches to be processed, got %d", processedCount)
	}
	mu.Unlock()
}

// Test that tasks with same batch key but different servers get separate batches
func TestServerBatcherDifferentServerSameBatchKey(t *testing.T) {
	// Process function that counts processed batches
	var processedCount int
	var mu sync.Mutex
	processFn := func(batch *TaskBatch) {
		mu.Lock()
		processedCount++
		mu.Unlock()
	}

	// Create batcher with small wait time for testing
	batcher := NewServerTaskBatcher(5, 50*time.Millisecond, processFn)

	// Create tasks for different servers with same batch key
	task1 := &MockBatchableTask{
		MockTask: MockTask{id: "task1", priority: 5},
		batchKey: "key1",
		server:   "server1",
	}
	task2 := &MockBatchableTask{
		MockTask: MockTask{id: "task2", priority: 3},
		batchKey: "key1",
		server:   "server2",
	}

	// Add tasks
	batcher.Add(task1)
	batcher.Add(task2)

	// Check batches before processing
	server1Batches := batcher.GetBatchesForServer("server1")
	if len(server1Batches) != 1 {
		t.Errorf("Expected 1 batch for server1, got %d", len(server1Batches))
	}

	server2Batches := batcher.GetBatchesForServer("server2")
	if len(server2Batches) != 1 {
		t.Errorf("Expected 1 batch for server2, got %d", len(server2Batches))
	}

	// Wait for batches to process due to max wait time
	time.Sleep(100 * time.Millisecond)

	// Verify that batches were processed
	mu.Lock()
	if processedCount != 2 {
		t.Errorf("Expected 2 batches to be processed, got %d", processedCount)
	}
	mu.Unlock()
}

// Test batch full triggers immediate processing
func TestServerBatcherBatchFull(t *testing.T) {
	// Process function that counts processed batches
	var processedCount int
	var mu sync.Mutex
	processFn := func(batch *TaskBatch) {
		mu.Lock()
		processedCount++
		mu.Unlock()
	}

	// Create batcher with small batch size and long wait time
	batcher := NewServerTaskBatcher(3, 1*time.Hour, processFn) // Long wait time so only full batches trigger processing

	// Create tasks for same server and batch key
	tasks := make([]*MockBatchableTask, 3)
	for i := 0; i < 3; i++ {
		tasks[i] = &MockBatchableTask{
			MockTask: MockTask{id: "task" + string(rune('1'+i)), priority: 5},
			batchKey: "key1",
			server:   "server1",
		}
	}

	// Add tasks one by one
	for _, task := range tasks {
		batcher.Add(task)
	}

	// Small delay to allow batch processing to occur
	time.Sleep(10 * time.Millisecond)

	// Verify that batch was processed immediately when full
	mu.Lock()
	if processedCount != 1 {
		t.Errorf("Expected 1 batch to be processed immediately when full, got %d", processedCount)
	}
	mu.Unlock()
}

// Test that batch processing removes batch from batcher
func TestServerBatcherBatchProcessingRemovesBatch(t *testing.T) {
	// Create a channel to signal when batch is processed
	processed := make(chan struct{})
	
	// Process function that signals when done
	processFn := func(batch *TaskBatch) {
		processed <- struct{}{}
	}

	// Create batcher with small wait time
	batcher := NewServerTaskBatcher(5, 50*time.Millisecond, processFn)

	// Create and add a task
	task := &MockBatchableTask{
		MockTask: MockTask{id: "task1", priority: 5},
		batchKey: "key1",
		server:   "server1",
	}
	batcher.Add(task)

	// Check batch exists
	if len(batcher.GetBatchesForServer("server1")) != 1 {
		t.Errorf("Expected 1 batch before processing")
	}

	// Wait for batch to be processed
	<-processed

	// Small delay to allow cleanup
	time.Sleep(10 * time.Millisecond)

	// Check batch was removed
	if len(batcher.GetBatchesForServer("server1")) != 0 {
		t.Errorf("Expected 0 batches after processing, got %d", len(batcher.GetBatchesForServer("server1")))
	}
}

// Benchmark server batching vs. no batching
func BenchmarkServerBatcher(b *testing.B) {
	b.Run("WithBatching", func(b *testing.B) {
		// No-op process function for benchmark
		processFn := func(batch *TaskBatch) {}
		
		// Create batcher with reasonably large values
		batcher := NewServerTaskBatcher(100, 5*time.Second, processFn)
		
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			// Create tasks with same batch key and server to maximize batching
			task := &MockBatchableTask{
				MockTask: MockTask{id: "task" + string(rune(i)), priority: 5},
				batchKey: "key1",
				server:   "server1",
			}
			batcher.Add(task)
		}
	})
	
	b.Run("WithoutBatching", func(b *testing.B) {
		// For comparison: no batching, just count tasks
		var tasks []BatchableTask
		
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			// Create tasks and add to simple slice
			task := &MockBatchableTask{
				MockTask: MockTask{id: "task" + string(rune(i)), priority: 5},
				batchKey: "key1",
				server:   "server1",
			}
			tasks = append(tasks, task)
		}
	})
}