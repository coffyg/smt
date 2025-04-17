package smt

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// BenchmarkHighLoadConfigurations tests SMT configurations under extreme load
// with many servers and requests to find the optimal configuration
func BenchmarkHighLoadConfigurations(b *testing.B) {
	// Define extreme load configurations
	configs := []struct {
		name                string
		enablePooling       bool
		enableAdaptiveTimeout bool
		enableTwoLevel      bool
	}{
		{
			name:                "Baseline",
			enablePooling:       false,
			enableAdaptiveTimeout: false,
			enableTwoLevel:      false,
		},
		{
			name:                "MemPoolOnly",
			enablePooling:       true,
			enableAdaptiveTimeout: false,
			enableTwoLevel:      false,
		},
		{
			name:                "TwoLevelOnly",
			enablePooling:       false,
			enableAdaptiveTimeout: false,
			enableTwoLevel:      true,
		},
		{
			name:                "MemPool_TwoLevel", // Current optimized configuration
			enablePooling:       true,
			enableAdaptiveTimeout: false,
			enableTwoLevel:      true,
		},
		{
			name:                "AllFeatures",
			enablePooling:       true,
			enableAdaptiveTimeout: true,
			enableTwoLevel:      true,
		},
	}
	
	// Define high load scenarios
	scenarios := []struct {
		name             string
		providerCount    int
		serversPerProvider int
		tasksPerProvider int
		taskDuration     time.Duration
	}{
		{
			name:             "ManyServers_MediumLoad",
			providerCount:    4,
			serversPerProvider: 12, // 48 total servers
			tasksPerProvider: 5000,
			taskDuration:     5 * time.Millisecond,
		},
		{
			name:             "ExtremeLoad_ManyServers",
			providerCount:    10,
			serversPerProvider: 10, // 100 total servers
			tasksPerProvider: 10000,
			taskDuration:     2 * time.Millisecond,
		},
	}
	
	logger := zerolog.Nop()
	
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			for _, config := range configs {
				b.Run(config.name, func(b *testing.B) {
					// Create providers
					providers := make([]IProvider, 0, scenario.providerCount)
					servers := make(map[string][]string)
					
					for i := 0; i < scenario.providerCount; i++ {
						providerName := fmt.Sprintf("provider%d", i)
						
						// Create provider
						provider := &HighLoadProvider{
							name: providerName,
							taskDuration: scenario.taskDuration,
						}
						
						providers = append(providers, provider)
						
						// Create servers for this provider
						serverList := make([]string, 0, scenario.serversPerProvider)
						for j := 0; j < scenario.serversPerProvider; j++ {
							serverList = append(serverList, fmt.Sprintf("server-%d-%d", i, j))
						}
						servers[providerName] = serverList
					}
					
					// Create task manager with config
					tm := NewTaskManagerWithOptions(&providers, servers, &logger, func(string, string) time.Duration {
						return 100 * time.Millisecond
					}, &TaskManagerOptions{
						EnablePooling:         config.enablePooling,
						EnableAdaptiveTimeout: config.enableAdaptiveTimeout,
						EnableTwoLevel:        config.enableTwoLevel,
						PoolConfig: &PoolConfig{
							PreWarmSize:  5000,
							TrackStats:   false,
							PreWarmAsync: true,
						},
					})
					
					// Start task manager
					tm.Start()
					defer tm.Shutdown()
					
					// Reset timer
					b.ResetTimer()
					
					// Run the benchmark
					for i := 0; i < b.N; i++ {
						// Calculate total tasks
						totalTasks := scenario.providerCount * scenario.tasksPerProvider
						
						// Task completion tracking
						var wg sync.WaitGroup
						wg.Add(totalTasks)
						
						// Submit tasks with high concurrency
						for p := 0; p < scenario.providerCount; p++ {
							provider := providers[p]
							
							// Launch a goroutine per provider
							go func(providerIdx int) {
								for t := 0; t < scenario.tasksPerProvider; t++ {
									taskID := fmt.Sprintf("task-%d-%d-%d", i, providerIdx, t)
									priority := t % 10
									
									task := &HighLoadTask{
										id:       taskID,
										priority: priority,
										provider: provider,
										wg:       &wg,
									}
									
									tm.AddTask(task)
									
									// Add throttling to prevent overwhelming the system
									if t%100 == 0 && t > 0 {
										time.Sleep(1 * time.Millisecond)
									}
								}
							}(p)
						}
						
						// Wait for all tasks to complete
						wg.Wait()
					}
					
					// Stop timer
					b.StopTimer()
				})
			}
		})
	}
}

// HighLoadProvider implements IProvider for high load testing
type HighLoadProvider struct {
	name         string
	taskDuration time.Duration
}

func (p *HighLoadProvider) Name() string {
	return p.name
}

func (p *HighLoadProvider) Handle(task ITask, server string) error {
	// Simulate consistent processing time
	time.Sleep(p.taskDuration)
	return nil
}

// HighLoadTask implements ITask for high load testing
type HighLoadTask struct {
	id         string
	priority   int
	provider   IProvider
	timeout    time.Duration
	completed  bool
	retries    int
	wg         *sync.WaitGroup
	mu         sync.Mutex
}

func (t *HighLoadTask) MarkAsSuccess(execTime int64) {
	t.mu.Lock()
	t.completed = true
	t.mu.Unlock()
}

func (t *HighLoadTask) MarkAsFailed(execTime int64, err error) {
	t.mu.Lock()
	t.completed = true
	t.mu.Unlock()
}

func (t *HighLoadTask) GetPriority() int {
	return t.priority
}

func (t *HighLoadTask) GetID() string {
	return t.id
}

func (t *HighLoadTask) GetMaxRetries() int {
	return 1
}

func (t *HighLoadTask) GetRetries() int {
	return t.retries
}

func (t *HighLoadTask) GetCreatedAt() time.Time {
	return time.Now() // Good enough for benchmarking
}

func (t *HighLoadTask) GetTaskGroup() ITaskGroup {
	return nil
}

func (t *HighLoadTask) GetProvider() IProvider {
	return t.provider
}

func (t *HighLoadTask) UpdateRetries(r int) error {
	t.retries = r
	return nil
}

func (t *HighLoadTask) GetTimeout() time.Duration {
	return t.timeout
}

func (t *HighLoadTask) UpdateLastError(s string) error {
	return nil
}

func (t *HighLoadTask) GetCallbackName() string {
	return "benchmark"
}

func (t *HighLoadTask) OnComplete() {
	if t.wg != nil {
		t.wg.Done()
	}
}

func (t *HighLoadTask) OnStart() {
	// Nothing to do
}