package smt

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// BenchmarkFeatures tests each feature individually
func BenchmarkFeatures(b *testing.B) {
	// Define feature configurations to test
	tests := []struct {
		name                string
		pooling             bool
		adaptiveTimeout     bool
		twoLevel            bool
	}{
		{"Baseline", false, false, false},
		{"PoolingOnly", true, false, false},
		{"AdaptiveTimeoutOnly", false, true, false},
		{"TwoLevelOnly", false, false, true},
		{"MinimalOptimal", true, false, true},
		{"AllFeatures", true, true, true},
	}

	// Configure workload - use higher values to stress the system
	providerCount := 10
	serversPerProvider := 4
	tasksPerProvider := 500
	taskDuration := 500 * time.Microsecond
	isHighLoad := testing.Short() // In short mode, use less load

	if isHighLoad {
		providerCount = 5
		tasksPerProvider = 200
	}

	// Run each test configuration
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			
			// Run the benchmark with the current configuration
			runFeatureBenchmark(b, tt.pooling, tt.adaptiveTimeout, tt.twoLevel, 
				providerCount, serversPerProvider, tasksPerProvider, taskDuration)
		})
	}
}

// runFeatureBenchmark runs a benchmark with specific feature configuration
func runFeatureBenchmark(b *testing.B, pooling, adaptiveTimeout, twoLevel bool,
	providerCount, serversPerProvider, tasksPerProvider int, taskDuration time.Duration) {
	
	logger := zerolog.Nop()
	
	// Setup providers and servers
	providers := make([]IProvider, 0, providerCount)
	servers := make(map[string][]string)
	
	for i := 0; i < providerCount; i++ {
		providerName := fmt.Sprintf("provider%d", i)
		provider := &FeatureBenchProvider{
			name: providerName,
			taskDuration: taskDuration,
			failureRate: 0.05, // 5% of tasks will fail - simulates real-world conditions
		}
		providers = append(providers, provider)
		
		// Create servers for this provider
		serverList := make([]string, 0, serversPerProvider)
		for j := 0; j < serversPerProvider; j++ {
			serverList = append(serverList, fmt.Sprintf("server-%d-%d", i, j))
		}
		servers[providerName] = serverList
	}
	
	// Create task manager with specified feature configuration
	tm := NewTaskManagerWithOptions(&providers, servers, &logger, func(string, string) time.Duration {
		return 50 * time.Millisecond
	}, &TaskManagerOptions{
		EnablePooling:         pooling,
		EnableAdaptiveTimeout: adaptiveTimeout,
		EnableTwoLevel:        twoLevel,
	})
	
	// Start the task manager
	tm.Start()
	defer tm.Shutdown()
	
	// Reset timer before benchmark
	b.ResetTimer()
	
	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Setup synchronization
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
					task := &FeatureBenchTask{
						id: fmt.Sprintf("task-%d-%d-%d", i, providerIdx, t),
						priority: t % 10,
						provider: provider,
						// Removed batch key
						server: fmt.Sprintf("server-%d-%d", providerIdx, t%serversPerProvider),
						onCompleteFn: func() {
							completeCh <- struct{}{}
						},
					}
					
					tm.AddTask(task)
					
					// Simulating realistic task addition rate
					if t%50 == 0 {
						time.Sleep(time.Microsecond * 100)
					}
				}
			}(p)
		}
		
		// Wait for all tasks to be added
		wg.Wait()
		
		// Wait for all tasks to complete (or timeout)
		totalTasks := providerCount * tasksPerProvider
		for completed := 0; completed < totalTasks; {
			select {
			case <-completeCh:
				completed++
			case <-time.After(1 * time.Second):
				// Timeout to prevent deadlock
				b.Logf("Timeout waiting for tasks (%d/%d)", completed, totalTasks)
				completed = totalTasks // Force completion
			}
		}
		
		// Add a small delay between iterations
		time.Sleep(10 * time.Millisecond)
	}
}

// FeatureBenchProvider is a provider for benchmarking
type FeatureBenchProvider struct {
	name         string
	taskDuration time.Duration
	failureRate  float64
}

func (p *FeatureBenchProvider) Handle(task ITask, server string) error {
	// Simulate work
	time.Sleep(p.taskDuration)
	
	// Simulate failures
	if p.failureRate > 0 {
		// Very simple hash of the task ID to ensure consistent failures
		taskID := task.GetID()
		hashValue := 0
		for _, c := range taskID {
			hashValue += int(c)
		}
		
		// If hash % 100 is less than failureRate*100, return an error
		if hashValue%100 < int(p.failureRate*100) {
			return fmt.Errorf("simulated error [task=%s provider=%s server=%s]", 
				task.GetID(), p.name, server)
		}
	}
	
	return nil
}

func (p *FeatureBenchProvider) Name() string {
	return p.name
}

// FeatureBenchTask is a task for benchmarking
type FeatureBenchTask struct {
	id           string
	priority     int
	provider     IProvider
	maxRetries   int
	retries      int
	createdAt    time.Time
	timeout      time.Duration
	completed    bool
	failed       bool
	// Removed batch key
	server       string
	mu           sync.Mutex
	onCompleteFn func()
}

func (t *FeatureBenchTask) MarkAsSuccess(execTime int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.completed = true
}

func (t *FeatureBenchTask) MarkAsFailed(execTime int64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failed = true
	t.completed = true
}

func (t *FeatureBenchTask) GetPriority() int {
	return t.priority
}

func (t *FeatureBenchTask) GetID() string {
	return t.id
}

func (t *FeatureBenchTask) GetMaxRetries() int {
	return 3
}

func (t *FeatureBenchTask) GetRetries() int {
	return t.retries
}

func (t *FeatureBenchTask) GetCreatedAt() time.Time {
	if t.createdAt.IsZero() {
		return time.Now()
	}
	return t.createdAt
}

func (t *FeatureBenchTask) GetTaskGroup() ITaskGroup {
	return nil
}

func (t *FeatureBenchTask) GetProvider() IProvider {
	return t.provider
}

func (t *FeatureBenchTask) UpdateRetries(r int) error {
	t.retries = r
	return nil
}

func (t *FeatureBenchTask) GetTimeout() time.Duration {
	return 30 * time.Millisecond
}

func (t *FeatureBenchTask) UpdateLastError(s string) error {
	return nil
}

func (t *FeatureBenchTask) GetCallbackName() string {
	return "benchmark"
}

func (t *FeatureBenchTask) OnComplete() {
	t.completed = true
	if t.onCompleteFn != nil {
		t.onCompleteFn()
	}
}

func (t *FeatureBenchTask) OnStart() {
	// Nothing to do
}

// Removed batching interface implementation
