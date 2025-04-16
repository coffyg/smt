package smt

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// BenchmarkConfigurations compares our two main configurations
func BenchmarkConfigurations(b *testing.B) {
	// Run sub-benchmarks
	b.Run("Minimal", func(b *testing.B) {
		runConfigBenchmark(b, true, false, true)
	})
	
	b.Run("AllFeatures", func(b *testing.B) {
		runConfigBenchmark(b, true, true, true)
	})
}

// runConfigBenchmark runs a benchmark with the specified configuration
func runConfigBenchmark(b *testing.B, pooling, adaptive, twoLevel bool) {
	logger := zerolog.Nop()
	
	// Create providers for testing
	providerCount := 5
	serversPerProvider := 3
	tasksPerProvider := 50
	
	// Create provider list and servers map
	providers := make([]IProvider, 0, providerCount)
	servers := make(map[string][]string)
	
	for i := 0; i < providerCount; i++ {
		providerName := fmt.Sprintf("provider%d", i)
		provider := &ConfigBenchProvider{name: providerName}
		providers = append(providers, provider)
		
		// Create server list for this provider
		serverList := make([]string, 0, serversPerProvider)
		for j := 0; j < serversPerProvider; j++ {
			serverList = append(serverList, fmt.Sprintf("server-%d-%d", i, j))
		}
		servers[providerName] = serverList
	}
	
	// Create task manager with specified configuration
	tm := NewTaskManagerWithOptions(&providers, servers, &logger, func(string, string) time.Duration {
		return 50 * time.Millisecond
	}, &TaskManagerOptions{
		EnablePooling: pooling,
		EnableAdaptiveTimeout: adaptive,
		EnableTwoLevel: twoLevel,
	})
	
	// Start the manager
	tm.Start()
	defer tm.Shutdown()
	
	// Reset timer before the actual benchmark
	b.ResetTimer()
	
	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Setup completion tracking
		var wg sync.WaitGroup
		completeCh := make(chan struct{}, providerCount*tasksPerProvider)
		
		// Process tasks for each provider concurrently
		for p := 0; p < providerCount; p++ {
			wg.Add(1)
			go func(providerIdx int) {
				defer wg.Done()
				
				provider := providers[providerIdx]
				
				// Create and add tasks
				for t := 0; t < tasksPerProvider; t++ {
					task := &ConfigBenchTask{
						id: fmt.Sprintf("task-%d-%d", providerIdx, t),
						priority: t % 10,
						provider: provider,
						// Removed batch key
						server: fmt.Sprintf("server-%d-%d", providerIdx, t%serversPerProvider),
						onCompleteFn: func() {
							completeCh <- struct{}{}
						},
					}
					
					tm.AddTask(task)
				}
			}(p)
		}
		
		// Wait for all tasks to be added
		wg.Wait()
		
		// Wait for all tasks to complete
		totalTasks := providerCount * tasksPerProvider
		for completed := 0; completed < totalTasks; {
			select {
			case <-completeCh:
				completed++
			case <-time.After(500 * time.Millisecond):
				// Safety timeout
				b.Logf("Timing out while waiting for tasks (%d/%d)", completed, totalTasks)
				completed = totalTasks // Force completion
			}
		}
	}
}

// ConfigBenchProvider is a dummy provider for benchmarking
type ConfigBenchProvider struct {
	name string
}

func (p *ConfigBenchProvider) Handle(task ITask, server string) error {
	// Very fast simulated work
	time.Sleep(500 * time.Microsecond)
	return nil
}

func (p *ConfigBenchProvider) Name() string {
	return p.name
}

// ConfigBenchTask is a dummy task for benchmarking
type ConfigBenchTask struct {
	id string
	priority int
	provider IProvider
	maxRetries int
	retries int
	createdAt time.Time
	timeout time.Duration
	completed bool
	failed bool
	// Removed batch key
	server string
	mu sync.Mutex
	onCompleteFn func()
}

func (t *ConfigBenchTask) MarkAsSuccess(execTime int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.completed = true
}

func (t *ConfigBenchTask) MarkAsFailed(execTime int64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failed = true
	t.completed = true
}

func (t *ConfigBenchTask) GetPriority() int {
	return t.priority
}

func (t *ConfigBenchTask) GetID() string {
	return t.id
}

func (t *ConfigBenchTask) GetMaxRetries() int {
	return 1
}

func (t *ConfigBenchTask) GetRetries() int {
	return 0
}

func (t *ConfigBenchTask) GetCreatedAt() time.Time {
	if t.createdAt.IsZero() {
		return time.Now()
	}
	return t.createdAt
}

func (t *ConfigBenchTask) GetTaskGroup() ITaskGroup {
	return nil
}

func (t *ConfigBenchTask) GetProvider() IProvider {
	return t.provider
}

func (t *ConfigBenchTask) UpdateRetries(r int) error {
	t.retries = r
	return nil
}

func (t *ConfigBenchTask) GetTimeout() time.Duration {
	return 50 * time.Millisecond
}

func (t *ConfigBenchTask) UpdateLastError(s string) error {
	return nil
}

func (t *ConfigBenchTask) GetCallbackName() string {
	return "benchmark"
}

func (t *ConfigBenchTask) OnComplete() {
	t.completed = true
	if t.onCompleteFn != nil {
		t.onCompleteFn()
	}
}

func (t *ConfigBenchTask) OnStart() {
	// Nothing to do
}

// Removed batching interface implementation