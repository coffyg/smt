package smt

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// MockBatchableTaskWithServer implements ServerBatchableTask for benchmarking
type MockBatchableTaskWithServer struct {
	MockTask
	batchKey     string
	targetServer string
}

func (t *MockBatchableTaskWithServer) BatchKey() string {
	return t.batchKey
}

func (t *MockBatchableTaskWithServer) CanBatchWith(other BatchableTask) bool {
	// Batch with tasks that have the same batch key
	return t.batchKey == other.BatchKey()
}

func (t *MockBatchableTaskWithServer) MergeWith(other BatchableTask) BatchableTask {
	// Simple implementation that just returns self
	return t
}

func (t *MockBatchableTaskWithServer) GetTargetServer() string {
	return t.targetServer
}

// BenchmarkTaskManagerWithBatching compares performance with and without batching
func BenchmarkTaskManagerWithBatching(b *testing.B) {
	// Set a smaller iteration count for faster benchmarks
	if b.N > 10000 {
		b.N = 10000
	}
	nopLogger := zerolog.Nop()
	
	// Create mock provider and servers
	providerName := "benchProvider"
	provider := &MockProvider{name: providerName}
	provider.handleFunc = func(task ITask, server string) error {
		return nil
	}
	
	providers := []IProvider{provider}
	servers := map[string][]string{
		providerName: {"server1", "server2", "server3", "server4"},
	}
	
	getTimeout := func(string, string) time.Duration {
		return time.Second * 5
	}

	// Run benchmark with batching disabled (baseline)
	b.Run("WithoutBatching", func(b *testing.B) {
		b.StopTimer()
		
		// Create task manager without batching
		tm := NewTaskManagerWithOptions(&providers, servers, &nopLogger, getTimeout, &TaskManagerOptions{
			EnablePooling:  true,
			EnableBatching: false,
		})
		tm.Start()
		defer tm.Shutdown()
		
		// Prepare tasks with same batch key and server to maximize batching potential
		tasks := make([]ITask, b.N)
		for i := 0; i < b.N; i++ {
			tasks[i] = &MockBatchableTaskWithServer{
				MockTask: MockTask{
					id:         fmt.Sprintf("task%d", i),
					priority:   i % 10,
					maxRetries: 3,
					createdAt:  time.Now(),
					provider:   provider,
					timeout:    time.Second * 5,
				},
				batchKey:     "batch1",
				targetServer: "server1",
			}
		}
		
		b.StartTimer()
		
		// Add tasks
		for i := 0; i < b.N; i++ {
			tm.AddTask(tasks[i])
		}
	})
	
	// Run benchmark with batching enabled
	b.Run("WithBatching", func(b *testing.B) {
		b.StopTimer()
		
		// Create task manager with batching
		tm := NewTaskManagerWithOptions(&providers, servers, &nopLogger, getTimeout, &TaskManagerOptions{
			EnablePooling:  true,
			EnableBatching: true,
			BatchMaxSize:   100, // Large enough to batch most tasks
			BatchMaxWait:   10 * time.Millisecond,
		})
		tm.Start()
		defer tm.Shutdown()
		
		// Prepare tasks with same batch key and server to maximize batching
		tasks := make([]ITask, b.N)
		for i := 0; i < b.N; i++ {
			tasks[i] = &MockBatchableTaskWithServer{
				MockTask: MockTask{
					id:         fmt.Sprintf("task%d", i),
					priority:   i % 10,
					maxRetries: 3,
					createdAt:  time.Now(),
					provider:   provider,
					timeout:    time.Second * 5,
				},
				batchKey:     "batch1",
				targetServer: "server1",
			}
		}
		
		b.StartTimer()
		
		// Add tasks
		for i := 0; i < b.N; i++ {
			tm.AddTask(tasks[i])
		}
	})
}

// BenchmarkTaskManagerBatchingMixedServers tests batching with tasks distributed across servers
func BenchmarkTaskManagerBatchingMixedServers(b *testing.B) {
	// Set a smaller iteration count for faster benchmarks
	if b.N > 10000 {
		b.N = 10000
	}
	nopLogger := zerolog.Nop()
	
	// Create mock provider and servers
	providerName := "benchProvider"
	provider := &MockProvider{name: providerName}
	
	var mu sync.Mutex
	handledTasks := make(map[string]int)
	
	provider.handleFunc = func(task ITask, server string) error {
		mu.Lock()
		handledTasks[server]++
		mu.Unlock()
		return nil
	}
	
	servers := []string{"server1", "server2", "server3", "server4"}
	serverMap := map[string][]string{
		providerName: servers,
	}
	
	providers := []IProvider{provider}
	
	getTimeout := func(string, string) time.Duration {
		return time.Second * 5
	}
	
	b.Run("WithMixedServers", func(b *testing.B) {
		b.StopTimer()
		
		// Create task manager with batching
		tm := NewTaskManagerWithOptions(&providers, serverMap, &nopLogger, getTimeout, &TaskManagerOptions{
			EnablePooling:  true,
			EnableBatching: true,
			BatchMaxSize:   10,
			BatchMaxWait:   10 * time.Millisecond,
		})
		tm.Start()
		defer tm.Shutdown()
		
		// Prepare tasks with different servers but same batch key
		tasks := make([]ITask, b.N)
		for i := 0; i < b.N; i++ {
			tasks[i] = &MockBatchableTaskWithServer{
				MockTask: MockTask{
					id:         fmt.Sprintf("task%d", i),
					priority:   i % 10,
					maxRetries: 3,
					createdAt:  time.Now(),
					provider:   provider,
					timeout:    time.Second * 5,
				},
				batchKey:     "batch1",
				targetServer: servers[i%len(servers)], // Distribute across servers
			}
		}
		
		b.StartTimer()
		
		// Add tasks
		for i := 0; i < b.N; i++ {
			tm.AddTask(tasks[i])
		}
	})
}