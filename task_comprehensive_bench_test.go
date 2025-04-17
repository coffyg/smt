package smt

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// ComprehensiveConfigBenchmark compares performance of different configuration settings
// across varying workloads and provider counts
func BenchmarkComprehensiveConfig(b *testing.B) {
	logger := zerolog.Nop()
	
	// Define configurations to test - testing all possible combinations
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
	
	// Define workload scenarios with focus on 6 server configuration as requested
	scenarios := []struct {
		name             string
		providerCount    int
		serversPerProvider int
		tasksPerProvider int
		taskDuration     time.Duration
		variableExec     bool
		highParallelism  bool
	}{
		// Six server focused scenario with consistent timing (requested scenario)
		{
			name:             "SixServers_ConsistentTiming",
			providerCount:    1,
			serversPerProvider: 6,
			tasksPerProvider: 1000,
			taskDuration:     10 * time.Millisecond,
			variableExec:     false,
			highParallelism:  true,
		},
		
		// Six server scenarios with different workload characteristics
		{
			name:             "SixServers_VariableTiming",
			providerCount:    1,
			serversPerProvider: 6, 
			tasksPerProvider: 1000,
			taskDuration:     10 * time.Millisecond,
			variableExec:     true,
			highParallelism:  true,
		},
		
		// Six server scenario with multiple providers
		{
			name:             "MultiProvider_SixServers",
			providerCount:    3,
			serversPerProvider: 2, // Total of 6 servers
			tasksPerProvider: 500,
			taskDuration:     10 * time.Millisecond,
			variableExec:     false,
			highParallelism:  true,
		},
		
		// High volume scenario
		{
			name:             "HighVolume_SixServers",
			providerCount:    1,
			serversPerProvider: 6,
			tasksPerProvider: 5000,
			taskDuration:     5 * time.Millisecond,
			variableExec:     false,
			highParallelism:  true,
		},
	}
	
	// Run benchmarks for each scenario and configuration
	for _, scenario := range scenarios {
		for _, config := range configs {
			testName := fmt.Sprintf("%s_%s", scenario.name, config.name)
			b.Run(testName, func(b *testing.B) {
				// Create providers based on scenario
				providers := make([]IProvider, 0, scenario.providerCount)
				servers := make(map[string][]string)
				
				for i := 0; i < scenario.providerCount; i++ {
					providerName := fmt.Sprintf("provider%d", i)
					
					// Create provider with appropriate execution time based on scenario
					provider := &CompBenchProvider{
						name: providerName,
						taskDuration: scenario.taskDuration,
						variableExec: scenario.variableExec,
					}
					
					providers = append(providers, provider)
					
					// Create servers for this provider
					serverList := make([]string, 0, scenario.serversPerProvider)
					for j := 0; j < scenario.serversPerProvider; j++ {
						serverList = append(serverList, fmt.Sprintf("server-%d-%d", i, j))
					}
					servers[providerName] = serverList
				}
				
				// Create task manager with specified configuration
				tm := NewTaskManagerWithOptions(&providers, servers, &logger, func(string, string) time.Duration {
					return 200 * time.Millisecond
				}, &TaskManagerOptions{
					EnablePooling:         config.enablePooling,
					EnableAdaptiveTimeout: config.enableAdaptiveTimeout,
					EnableTwoLevel:        config.enableTwoLevel,
				})
				
				// Start the task manager
				tm.Start()
				defer tm.Shutdown()
				
				// Reset timer before benchmark
				b.ResetTimer()
				
				for i := 0; i < b.N; i++ {
					// Define batch sizes based on scenario
					tasksPerBatch := scenario.tasksPerProvider / 10
					if tasksPerBatch < 10 {
						tasksPerBatch = 10
					}
					
					// Create a wait group to track task completion
					var wg sync.WaitGroup
					
					// Setup completion tracking
					completedTasks := make(chan struct{}, scenario.providerCount*scenario.tasksPerProvider)
					
					// Add tasks in batches with appropriate parallelism
					for p := 0; p < scenario.providerCount; p++ {
						provider := providers[p]
						
						// Use concurrent task addition for high parallelism scenarios
						if scenario.highParallelism {
							wg.Add(1)
							go func(providerIndex int) {
								defer wg.Done()
								
								for t := 0; t < scenario.tasksPerProvider; t++ {
									taskID := fmt.Sprintf("task-%d-%d-%d", i, providerIndex, t)
									priority := t % 10
									
									task := &CompBenchTask{
										id:         taskID,
										priority:   priority,
										provider:   provider,
										maxRetries: 3,
										createdAt:  time.Now(),
										server:     fmt.Sprintf("server-%d-%d", providerIndex, t%scenario.serversPerProvider),
										onCompleteFn: func() {
											completedTasks <- struct{}{}
										},
									}
									
									tm.AddTask(task)
									
									// Simulate some delay between task additions
									if t%tasksPerBatch == 0 && t > 0 {
										time.Sleep(1 * time.Millisecond)
									}
								}
							}(p)
						} else {
							// Sequential task addition for low parallelism scenarios
							for t := 0; t < scenario.tasksPerProvider; t++ {
								taskID := fmt.Sprintf("task-%d-%d-%d", i, p, t)
								priority := t % 10
								
								task := &CompBenchTask{
									id:         taskID,
									priority:   priority,
									provider:   provider,
									maxRetries: 3,
									createdAt:  time.Now(),
									server:     fmt.Sprintf("server-%d-%d", p, t%scenario.serversPerProvider),
									onCompleteFn: func() {
										completedTasks <- struct{}{}
									},
								}
								
								tm.AddTask(task)
								
								// Simulate some delay between task additions
								if t%tasksPerBatch == 0 && t > 0 {
									time.Sleep(1 * time.Millisecond)
								}
							}
						}
					}
					
					// Wait for all task additions to complete
					wg.Wait()
					
					// Wait for task completions
					totalTasks := scenario.providerCount * scenario.tasksPerProvider
					for completed := 0; completed < totalTasks; {
						select {
						case <-completedTasks:
							completed++
						case <-time.After(5 * time.Second):
							// Timeout to prevent benchmark from hanging
							b.Fatalf("Timed out waiting for tasks to complete: %d/%d", completed, totalTasks)
							return
						}
					}
				}
				
				b.StopTimer()
			})
		}
	}
}

// CompBenchProvider implements IProvider for benchmarking
type CompBenchProvider struct {
	name         string
	taskDuration time.Duration
	variableExec bool
}

func (p *CompBenchProvider) Name() string {
	return p.name
}

func (p *CompBenchProvider) Handle(task ITask, server string) error {
	duration := p.taskDuration
	
	// Add variability to execution time if needed
	if p.variableExec {
		// Add +/- 50% variability
		variability := float64(p.taskDuration) * (0.5 - float64(task.GetPriority()%10)/10)
		duration = p.taskDuration + time.Duration(variability)
		if duration < 0 {
			duration = 100 * time.Microsecond
		}
	}
	
	// Simulate work
	time.Sleep(duration)
	return nil
}

// CompBenchTask implements ITask and ServerBatchableTask for benchmarking
type CompBenchTask struct {
	id           string
	priority     int
	provider     IProvider
	maxRetries   int
	retries      int
	timeout      time.Duration
	createdAt    time.Time
	completed    bool
	failed       bool
	server       string
	mu           sync.Mutex
	onCompleteFn func()
}

func (t *CompBenchTask) MarkAsSuccess(execTime int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.completed = true
}

func (t *CompBenchTask) MarkAsFailed(execTime int64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failed = true
	t.completed = true
}

func (t *CompBenchTask) GetPriority() int {
	return t.priority
}

func (t *CompBenchTask) GetID() string {
	return t.id
}

func (t *CompBenchTask) GetMaxRetries() int {
	return t.maxRetries
}

func (t *CompBenchTask) GetRetries() int {
	return t.retries
}

func (t *CompBenchTask) GetCreatedAt() time.Time {
	return t.createdAt
}

func (t *CompBenchTask) GetTaskGroup() ITaskGroup {
	return nil
}

func (t *CompBenchTask) GetProvider() IProvider {
	return t.provider
}

func (t *CompBenchTask) UpdateRetries(r int) error {
	t.retries = r
	return nil
}

func (t *CompBenchTask) GetTimeout() time.Duration {
	return t.timeout
}

func (t *CompBenchTask) UpdateLastError(s string) error {
	return nil
}

func (t *CompBenchTask) GetCallbackName() string {
	return "benchmark"
}

func (t *CompBenchTask) OnComplete() {
	t.completed = true
	if t.onCompleteFn != nil {
		t.onCompleteFn()
	}
}

func (t *CompBenchTask) OnStart() {
}