package smt

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// BatchableMockTask implements BatchableTask for benchmarking
type BatchableMockTask struct {
	*MockTask
	batchKey string
}

func (t *BatchableMockTask) BatchKey() string {
	return t.batchKey
}

func (t *BatchableMockTask) CanBatchWith(other BatchableTask) bool {
	return t.batchKey == other.BatchKey()
}

func (t *BatchableMockTask) MergeWith(other BatchableTask) BatchableTask {
	// For testing, we just return the first task
	return t
}

// BenchmarkTaskBatcher benchmarks the task batcher
func BenchmarkTaskBatcher(b *testing.B) {
	// Set up a simple processor function
	var processed int
	var processedLock sync.Mutex
	
	processFn := func(batch *TaskBatch) {
		processedLock.Lock()
		processed += batch.Size()
		processedLock.Unlock()
	}
	
	// Create a task batcher
	batcher := NewTaskBatcher(10, 50*time.Millisecond, processFn)
	
	// Create a test provider
	provider := &MockProvider{name: "batchProvider"}
	
	b.ResetTimer()
	
	// Add tasks to the batcher
	for i := 0; i < b.N; i++ {
		// Create 10 different batch keys
		batchKey := fmt.Sprintf("batch%d", i%10)
		
		task := &BatchableMockTask{
			MockTask: &MockTask{
				id:         fmt.Sprintf("task%d", i),
				priority:   i % 5,
				maxRetries: 3,
				createdAt:  time.Now(),
				provider:   provider,
				timeout:    time.Second * 5,
			},
			batchKey: batchKey,
		}
		
		batcher.Add(task)
	}
	
	// Wait for all batches to be processed
	time.Sleep(100 * time.Millisecond)
	
	// Verify all tasks were processed
	processedLock.Lock()
	if processed < b.N {
		b.Logf("Only processed %d of %d tasks", processed, b.N)
	}
	processedLock.Unlock()
}

// BenchmarkWorkerPool benchmarks the worker pool
func BenchmarkWorkerPool(b *testing.B) {
	// Create a worker pool
	pool := NewWorkerPool(4) // Use 4 workers for benchmark
	defer pool.Shutdown()
	
	// Wait group to track completion
	var wg sync.WaitGroup
	wg.Add(b.N)
	
	b.ResetTimer()
	
	// Submit tasks to the pool
	for i := 0; i < b.N; i++ {
		if !pool.Submit(func() {
			// Simulate a quick task
			time.Sleep(time.Microsecond * 50)
			wg.Done()
		}) {
			wg.Done() // Task wasn't submitted, so mark as done
			b.Logf("Failed to submit task %d", i)
		}
	}
	
	// Wait for all tasks to complete
	wg.Wait()
}

// BenchmarkCircuitBreaker benchmarks the circuit breaker
func BenchmarkCircuitBreaker(b *testing.B) {
	// Create a circuit breaker
	cb := NewCircuitBreaker(5, 100*time.Millisecond)
	
	b.ResetTimer()
	
	// Benchmark reporting successes and failures
	for i := 0; i < b.N; i++ {
		if cb.Allow() {
			if i%10 == 0 {
				// Simulate a failure every 10 requests
				cb.ReportFailure()
			} else {
				cb.ReportSuccess()
			}
		}
	}
}

// BenchmarkServerSelection benchmarks the server selection manager
func BenchmarkServerSelection(b *testing.B) {
	// Create a server selection manager
	ssm := NewServerSelectionManager()
	
	// Register some servers
	servers := []string{"server1", "server2", "server3", "server4", "server5"}
	for _, server := range servers {
		ssm.RegisterServer(server)
	}
	
	// Simulate some traffic to generate stats
	for i := 0; i < 1000; i++ {
		server := servers[i%len(servers)]
		if i%20 == 0 {
			ssm.RecordError(server, i%40 == 0)
		} else {
			ssm.RecordSuccess(server, int64(50+(i%10)*10)*1000000)
		}
	}
	
	b.ResetTimer()
	
	// Benchmark server selection
	for i := 0; i < b.N; i++ {
		ssm.GetOptimalServer(servers)
	}
}

// BenchmarkAdvancedTaskProcessing benchmarks the full task processing pipeline
func BenchmarkAdvancedTaskProcessing(b *testing.B) {
	// Setup logger
	logger := zerolog.Nop()
	
	// Create providers
	providerNames := []string{"provider1", "provider2", "provider3"}
	var providers []IProvider
	
	// Wait group to track completion
	var wg sync.WaitGroup
	
	// Create circuit breaker manager
	cbm := NewCircuitBreakerManager(5, 100*time.Millisecond)
	
	// Create server selection manager
	ssm := NewServerSelectionManager()
	
	// Create worker pool
	pool := NewWorkerPool(8) // Use 8 workers for benchmark
	defer pool.Shutdown()
	
	// Define servers for each provider
	servers := map[string][]string{
		"provider1": {"server1", "server2", "server3", "server4"},
		"provider2": {"server5", "server6", "server7", "server8"},
		"provider3": {"server9", "server10", "server11", "server12"},
	}
	
	// Register all servers with the circuit breaker and server selection
	for _, serverList := range servers {
		for _, server := range serverList {
			cbm.GetBreaker(server)
			ssm.RegisterServer(server)
		}
	}
	
	// Create provider handler functions
	for _, name := range providerNames {
		provider := &MockProvider{name: name}
		
		provider.handleFunc = func(task ITask, server string) error {
			// Simulate variable processing time
			time.Sleep(time.Millisecond * time.Duration(1+(task.GetPriority()%5)))
			
			// Get task ID to avoid races in the closure
			taskID := task.GetID()
			
			// Simulate occasional errors
			if taskID[len(taskID)-1] == '7' {
				// Report failure to circuit breaker and server selection
				cbm.ReportFailure(server)
				ssm.RecordError(server, false)
				return fmt.Errorf("simulated error for task %s", taskID)
			}
			
			// Report success
			cbm.ReportSuccess(server)
			ssm.RecordSuccess(server, int64(time.Millisecond)*int64(1+(task.GetPriority()%5)))
			
			// Mark task as done
			wg.Done()
			return nil
		}
		
		providers = append(providers, provider)
	}
	
	// Initialize TaskManager
	getTimeout := func(string, string) time.Duration {
		return time.Second * 2
	}
	
	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()
	
	b.ResetTimer()
	
	// Prepare for benchmark
	wg.Add(b.N)
	
	// Add tasks with worker pool
	for i := 0; i < b.N; i++ {
		i := i // Capture loop variable
		pool.Submit(func() {
			providerIdx := i % len(providers)
			provider := providers[providerIdx]
			
			// Create task
			task := &MockTask{
				id:         fmt.Sprintf("task%d", i),
				priority:   (i % 5) + 5, // Higher priority to ensure fast processing
				maxRetries: 3,
				createdAt:  time.Now(),
				provider:   provider,
				timeout:    time.Second * 2,
			}
			
			// Add task to TaskManager
			tm.AddTask(task)
		})
	}
	
	// Wait for all tasks to complete
	wg.Wait()
}