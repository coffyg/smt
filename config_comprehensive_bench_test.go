package smt

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// BenchmarkConfigCombinations tests all possible combinations of SMT configuration settings
// with 6 servers to identify the optimal configuration for average time per task
func BenchmarkConfigCombinations(b *testing.B) {
	// Define all possible configurations (8 combinations)
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
			name:                "AdaptiveOnly",
			enablePooling:       false,
			enableAdaptiveTimeout: true,
			enableTwoLevel:      false,
		},
		{
			name:                "MemPool_TwoLevel",
			enablePooling:       true,
			enableAdaptiveTimeout: false,
			enableTwoLevel:      true,
		},
		{
			name:                "MemPool_Adaptive",
			enablePooling:       true,
			enableAdaptiveTimeout: true,
			enableTwoLevel:      false,
		},
		{
			name:                "TwoLevel_Adaptive",
			enablePooling:       false,
			enableAdaptiveTimeout: true,
			enableTwoLevel:      true,
		},
		{
			name:                "AllFeatures", // Default NewTaskManagerSimple behavior
			enablePooling:       true,
			enableAdaptiveTimeout: true,
			enableTwoLevel:      true,
		},
	}

	// Test parameters focused on 6 servers scenario
	logger := zerolog.Nop()
	numServers := 6
	numTasks := 10000    // 10,000 tasks to test with
	fixedTimeout := 100 * time.Millisecond
	processingTime := 10 * time.Millisecond // Fixed processing time for consistent results
	
	for _, config := range configs {
		b.Run(config.name, func(b *testing.B) {
			// Setup the provider with consistent timing
			provider := &CompConfigBenchProvider{
				name: "test-provider",
				processingTime: processingTime,
			}
			
			providers := []IProvider{provider}
			
			// Create 6 servers with consistent naming
			serverNames := make([]string, numServers)
			for i := 0; i < numServers; i++ {
				serverNames[i] = fmt.Sprintf("server%d", i+1)
			}
			
			servers := map[string][]string{
				"test-provider": serverNames,
			}
			
			// Create task manager with specified configuration
			tm := NewTaskManagerWithOptions(&providers, servers, &logger, func(string, string) time.Duration {
				return fixedTimeout
			}, &TaskManagerOptions{
				EnablePooling:         config.enablePooling,
				EnableAdaptiveTimeout: config.enableAdaptiveTimeout,
				EnableTwoLevel:        config.enableTwoLevel,
			})
			
			// Start the task manager
			tm.Start()
			defer tm.Shutdown()
			
			// Reset timer
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				// Synchronization to track task completion
				var wg sync.WaitGroup
				wg.Add(numTasks)
				
				// Add tasks
				for t := 0; t < numTasks; t++ {
					taskID := fmt.Sprintf("task-%d-%d", i, t)
					priority := t % 10
					
					task := &CompConfigBenchTask{
						id:         taskID,
						priority:   priority,
						provider:   provider,
						maxRetries: 1,
						createdAt:  time.Now(),
						timeout:    fixedTimeout,
						wg:         &wg, // Reference to waitgroup for completion tracking
					}
					
					tm.AddTask(task)
					
					// Add small batching delay to prevent overwhelming the system
					if t%100 == 0 && t > 0 {
						time.Sleep(1 * time.Millisecond)
					}
				}
				
				// Wait for all tasks to complete
				wg.Wait()
			}
			
			// Stop timer
			b.StopTimer()
		})
	}
}

// BenchmarkFixedConfigs tests specific configurations in depth with different load patterns
func BenchmarkFixedConfigs(b *testing.B) {
	// Define just two configurations to compare in depth
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
			name:                "Optimal", // Based on previous benchmarks
			enablePooling:       true,
			enableAdaptiveTimeout: false, // Fixed timeout for this test
			enableTwoLevel:      true,
		},
	}
	
	// Define test scenarios
	scenarios := []struct {
		name          string
		tasksPerBatch int   // How many tasks to send in each batch
		batchDelay    time.Duration // Delay between batches
		numTasks      int   // Total number of tasks
		serverCount   int   // Number of servers
	}{
		{
			name:          "SteadyLoad",
			tasksPerBatch: 10,
			batchDelay:    5 * time.Millisecond,
			numTasks:      1000,
			serverCount:   6,
		},
		{
			name:          "BurstyLoad", 
			tasksPerBatch: 100,
			batchDelay:    50 * time.Millisecond,
			numTasks:      1000,
			serverCount:   6,
		},
		{
			name:          "HighConcurrency",
			tasksPerBatch: 500,
			batchDelay:    100 * time.Millisecond,
			numTasks:      5000,
			serverCount:   6,
		},
	}
	
	logger := zerolog.Nop()
	fixedTimeout := 100 * time.Millisecond
	processingTime := 10 * time.Millisecond
	
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			for _, config := range configs {
				b.Run(config.name, func(b *testing.B) {
					// Setup provider
					provider := &CompConfigBenchProvider{
						name: "test-provider",
						processingTime: processingTime,
					}
					
					providers := []IProvider{provider}
					
					// Create servers
					serverNames := make([]string, scenario.serverCount)
					for i := 0; i < scenario.serverCount; i++ {
						serverNames[i] = fmt.Sprintf("server%d", i+1)
					}
					
					servers := map[string][]string{
						"test-provider": serverNames,
					}
					
					// Create task manager
					tm := NewTaskManagerWithOptions(&providers, servers, &logger, func(string, string) time.Duration {
						return fixedTimeout
					}, &TaskManagerOptions{
						EnablePooling:         config.enablePooling,
						EnableAdaptiveTimeout: config.enableAdaptiveTimeout,
						EnableTwoLevel:        config.enableTwoLevel,
					})
					
					tm.Start()
					defer tm.Shutdown()
					
					b.ResetTimer()
					
					for i := 0; i < b.N; i++ {
						var wg sync.WaitGroup
						wg.Add(scenario.numTasks)
						
						// Process in batches
						for batchStart := 0; batchStart < scenario.numTasks; batchStart += scenario.tasksPerBatch {
							// Calculate batch end
							batchEnd := batchStart + scenario.tasksPerBatch
							if batchEnd > scenario.numTasks {
								batchEnd = scenario.numTasks
							}
							
							// Submit tasks in this batch
							for t := batchStart; t < batchEnd; t++ {
								taskID := fmt.Sprintf("task-%d-%d", i, t)
								priority := t % 10
								
								task := &CompConfigBenchTask{
									id:         taskID,
									priority:   priority,
									provider:   provider,
									maxRetries: 1,
									createdAt:  time.Now(),
									timeout:    fixedTimeout,
									wg:         &wg,
								}
								
								tm.AddTask(task)
							}
							
							// Add delay between batches if not the last batch
							if batchEnd < scenario.numTasks {
								time.Sleep(scenario.batchDelay)
							}
						}
						
						// Wait for all tasks to complete
						wg.Wait()
					}
					
					b.StopTimer()
				})
			}
		})
	}
}

// CompConfigBenchProvider implements IProvider for comprehensive benchmarking
type CompConfigBenchProvider struct {
	name           string
	processingTime time.Duration
}

func (p *CompConfigBenchProvider) Name() string {
	return p.name
}

func (p *CompConfigBenchProvider) Handle(task ITask, server string) error {
	// Simulate consistent processing time
	time.Sleep(p.processingTime)
	return nil
}

// CompConfigBenchTask implements ITask for comprehensive benchmarking
type CompConfigBenchTask struct {
	id         string
	priority   int
	provider   IProvider
	maxRetries int
	retries    int
	createdAt  time.Time
	timeout    time.Duration
	completed  bool
	failed     bool
	wg         *sync.WaitGroup // Reference to waitgroup for benchmark coordination
	mu         sync.Mutex
}

func (t *CompConfigBenchTask) MarkAsSuccess(execTime int64) {
	t.mu.Lock()
	t.completed = true
	t.mu.Unlock()
}

func (t *CompConfigBenchTask) MarkAsFailed(execTime int64, err error) {
	t.mu.Lock()
	t.failed = true
	t.completed = true
	t.mu.Unlock()
}

func (t *CompConfigBenchTask) GetPriority() int {
	return t.priority
}

func (t *CompConfigBenchTask) GetID() string {
	return t.id
}

func (t *CompConfigBenchTask) GetMaxRetries() int {
	return t.maxRetries
}

func (t *CompConfigBenchTask) GetRetries() int {
	return t.retries
}

func (t *CompConfigBenchTask) GetCreatedAt() time.Time {
	return t.createdAt
}

func (t *CompConfigBenchTask) GetTaskGroup() ITaskGroup {
	return nil
}

func (t *CompConfigBenchTask) GetProvider() IProvider {
	return t.provider
}

func (t *CompConfigBenchTask) UpdateRetries(r int) error {
	t.retries = r
	return nil
}

func (t *CompConfigBenchTask) GetTimeout() time.Duration {
	return t.timeout
}

func (t *CompConfigBenchTask) UpdateLastError(s string) error {
	return nil
}

func (t *CompConfigBenchTask) GetCallbackName() string {
	return "benchmark"
}

func (t *CompConfigBenchTask) OnComplete() {
	if t.wg != nil {
		t.wg.Done()
	}
}

func (t *CompConfigBenchTask) OnStart() {
	// Nothing to do here
}