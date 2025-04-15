package smt

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// ComprehensiveBenchmarkTask implements ITask and ServerBatchableTask for testing
type ComprehensiveBenchmarkTask struct {
	id         string
	priority   int
	provider   IProvider
	maxRetries int
	retries    int
	createdAt  time.Time
	completed  bool
	failed     bool
	server     string
	batchKey   string
	mu         sync.Mutex
}

func NewComprehensiveBenchmarkTask(id string, priority int, provider IProvider, server, batchKey string) *ComprehensiveBenchmarkTask {
	return &ComprehensiveBenchmarkTask{
		id:         id,
		priority:   priority,
		provider:   provider,
		maxRetries: 3,
		retries:    0,
		createdAt:  time.Now(),
		server:     server,
		batchKey:   batchKey,
	}
}

func (t *ComprehensiveBenchmarkTask) MarkAsSuccess(execTime int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.completed = true
}

func (t *ComprehensiveBenchmarkTask) MarkAsFailed(execTime int64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failed = true
	t.completed = true
}

func (t *ComprehensiveBenchmarkTask) GetPriority() int {
	return t.priority
}

func (t *ComprehensiveBenchmarkTask) GetID() string {
	return t.id
}

func (t *ComprehensiveBenchmarkTask) GetMaxRetries() int {
	return t.maxRetries
}

func (t *ComprehensiveBenchmarkTask) GetRetries() int {
	return t.retries
}

func (t *ComprehensiveBenchmarkTask) GetCreatedAt() time.Time {
	return t.createdAt
}

func (t *ComprehensiveBenchmarkTask) GetTaskGroup() ITaskGroup {
	return nil
}

func (t *ComprehensiveBenchmarkTask) GetProvider() IProvider {
	return t.provider
}

func (t *ComprehensiveBenchmarkTask) UpdateRetries(r int) error {
	t.retries = r
	return nil
}

func (t *ComprehensiveBenchmarkTask) GetTimeout() time.Duration {
	return 1 * time.Second
}

func (t *ComprehensiveBenchmarkTask) UpdateLastError(s string) error {
	return nil
}

func (t *ComprehensiveBenchmarkTask) GetCallbackName() string {
	return "benchmark"
}

func (t *ComprehensiveBenchmarkTask) OnComplete() {
	t.completed = true
}

func (t *ComprehensiveBenchmarkTask) OnStart() {
}

// ServerBatchableTask interface
func (t *ComprehensiveBenchmarkTask) GetTargetServer() string {
	return t.server
}

func (t *ComprehensiveBenchmarkTask) BatchKey() string {
	return t.batchKey
}

func (t *ComprehensiveBenchmarkTask) CanBatchWith(other BatchableTask) bool {
	return true
}

func (t *ComprehensiveBenchmarkTask) MergeWith(other BatchableTask) BatchableTask {
	return t
}

// ComprehensiveBenchmarkProvider implements IProvider for testing
type ComprehensiveBenchmarkProvider struct {
	name string
}

func (p *ComprehensiveBenchmarkProvider) Handle(task ITask, server string) error {
	// Simulate some work
	time.Sleep(5 * time.Microsecond)
	return nil
}

func (p *ComprehensiveBenchmarkProvider) Name() string {
	return p.name
}

// BenchmarkAllFeatures benchmarks different combinations of features
func BenchmarkAllFeatures(b *testing.B) {
	logger := zerolog.Nop()
	
	// Test scenarios
	scenarios := []struct {
		name            string
		enablePooling   bool
		enableBatching  bool
		enableCompaction bool
	}{
		{"Baseline", false, false, false},
		{"PoolingOnly", true, false, false},
		{"BatchingOnly", false, true, false},
		{"CompactionOnly", false, false, true},
		{"PoolingAndBatching", true, true, false},
		{"PoolingAndCompaction", true, false, true},
		{"BatchingAndCompaction", false, true, true},
		{"AllFeaturesEnabled", true, true, true},
	}
	
	// Create provider data
	numProviders := 5
	providers := make([]IProvider, 0, numProviders)
	servers := make(map[string][]string)
	
	for i := 1; i <= numProviders; i++ {
		pName := fmt.Sprintf("provider%d", i)
		provider := &ComprehensiveBenchmarkProvider{name: pName}
		providers = append(providers, provider)
		
		// Add 2 servers per provider
		serverList := []string{
			fmt.Sprintf("server%d-1", i),
			fmt.Sprintf("server%d-2", i),
		}
		servers[pName] = serverList
	}
	
	// Run benchmarks for each scenario
	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			// Create task manager with the specified features
			tm := NewTaskManagerWithOptions(&providers, servers, &logger, func(string, string) time.Duration {
				return 1 * time.Second
			}, &TaskManagerOptions{
				EnablePooling:       sc.enablePooling,
				EnableBatching:      sc.enableBatching,
				BatchMaxSize:        100,
				BatchMaxWait:        100 * time.Millisecond,
				EnableCompaction:    sc.enableCompaction,
				CompactionInterval:  1 * time.Second,
				CompactionThreshold: 50,
			})
			
			// Start the task manager
			tm.Start()
			defer tm.Shutdown()
			
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				// Create tasks with varied characteristics
				taskID := fmt.Sprintf("task-%d", i)
				providerIndex := i % numProviders
				provider := providers[providerIndex]
				priority := i % 10
				serverIndex := i % 2
				server := fmt.Sprintf("server%d-%d", providerIndex+1, serverIndex+1)
				batchKey := fmt.Sprintf("batch-%d", i%5)
				
				task := NewComprehensiveBenchmarkTask(taskID, priority, provider, server, batchKey)
				
				// Add task
				tm.AddTask(task)
				
				// Simulate task completion for 80% of tasks (to test object recycling)
				if i > 0 && i%5 != 0 {
					tm.delTaskInQueue(taskID)
				}
				
				// Trigger compaction periodically
				if sc.enableCompaction && i > 0 && i%1000 == 0 {
					// No need to call compaction manually as it's done in background
				}
			}
			
			b.StopTimer()
		})
	}
}

// BenchmarkCompactionThresholds benchmarks different compaction thresholds
func BenchmarkCompactionThresholds(b *testing.B) {
	logger := zerolog.Nop()
	
	// Test different thresholds
	thresholds := []int{10, 25, 50, 100, 200}
	
	// Create provider data
	numProviders := 5
	providers := make([]IProvider, 0, numProviders)
	servers := make(map[string][]string)
	
	for i := 1; i <= numProviders; i++ {
		pName := fmt.Sprintf("provider%d", i)
		provider := &ComprehensiveBenchmarkProvider{name: pName}
		providers = append(providers, provider)
		
		// Add 2 servers per provider
		serverList := []string{
			fmt.Sprintf("server%d-1", i),
			fmt.Sprintf("server%d-2", i),
		}
		servers[pName] = serverList
	}
	
	// Run benchmarks for each threshold
	for _, threshold := range thresholds {
		b.Run(fmt.Sprintf("Threshold_%d", threshold), func(b *testing.B) {
			// Create task manager with all features enabled
			tm := NewTaskManagerWithOptions(&providers, servers, &logger, func(string, string) time.Duration {
				return 1 * time.Second
			}, &TaskManagerOptions{
				EnablePooling:       true,
				EnableBatching:      true,
				BatchMaxSize:        100,
				BatchMaxWait:        100 * time.Millisecond,
				EnableCompaction:    true,
				CompactionInterval:  1 * time.Second,
				CompactionThreshold: threshold,
			})
			
			// Start the task manager
			tm.Start()
			defer tm.Shutdown()
			
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				// Create tasks with varied characteristics
				taskID := fmt.Sprintf("task-%d", i)
				providerIndex := i % numProviders
				provider := providers[providerIndex]
				priority := i % 10
				serverIndex := i % 2
				server := fmt.Sprintf("server%d-%d", providerIndex+1, serverIndex+1)
				batchKey := fmt.Sprintf("batch-%d", i%5)
				
				task := NewComprehensiveBenchmarkTask(taskID, priority, provider, server, batchKey)
				
				// Add task
				tm.AddTask(task)
				
				// Simulate task completion for 80% of tasks (to test object recycling)
				if i > 0 && i%5 != 0 {
					tm.delTaskInQueue(taskID)
				}
			}
			
			b.StopTimer()
		})
	}
}

// BenchmarkHighThroughput simulates high volume of tasks with/without compaction
func BenchmarkHighThroughput(b *testing.B) {
	logger := zerolog.Nop()
	
	// Create provider data
	numProviders := 5
	providers := make([]IProvider, 0, numProviders)
	servers := make(map[string][]string)
	
	for i := 1; i <= numProviders; i++ {
		providerName := fmt.Sprintf("provider%d", i)
		provider := &ComprehensiveBenchmarkProvider{name: providerName}
		providers = append(providers, provider)
		
		// Add 2 servers per provider
		serverList := []string{
			fmt.Sprintf("server%d-1", i),
			fmt.Sprintf("server%d-2", i),
		}
		servers[providerName] = serverList
	}
	
	// Test with and without compaction in high throughput scenario
	scenarios := []struct {
		name            string
		enableCompaction bool
	}{
		{"WithoutCompaction", false},
		{"WithCompaction", true},
	}
	
	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			// Create task manager with all features enabled except variable compaction
			tm := NewTaskManagerWithOptions(&providers, servers, &logger, func(string, string) time.Duration {
				return 1 * time.Second
			}, &TaskManagerOptions{
				EnablePooling:       true,
				EnableBatching:      true,
				BatchMaxSize:        100,
				BatchMaxWait:        100 * time.Millisecond,
				EnableCompaction:    sc.enableCompaction,
				CompactionInterval:  500 * time.Millisecond, // Frequent compaction for high throughput
				CompactionThreshold: 20,                    // Lower threshold for high throughput
			})
			
			// Start the task manager
			tm.Start()
			defer tm.Shutdown()
			
			// Prepare a large batch of tasks to simulate high throughput
			numTasks := 10000
			tasks := make([]ITask, 0, numTasks)
			
			for i := 0; i < numTasks; i++ {
				taskID := fmt.Sprintf("task-%d", i)
				providerIndex := i % numProviders
				provider := providers[providerIndex]
				priority := i % 10
				serverIndex := i % 2
				server := fmt.Sprintf("server%d-%d", providerIndex+1, serverIndex+1)
				batchKey := fmt.Sprintf("batch-%d", i%5)
				
				tasks = append(tasks, NewComprehensiveBenchmarkTask(taskID, priority, provider, server, batchKey))
			}
			
			b.ResetTimer()
			
			// Run benchmark iterations
			for i := 0; i < b.N; i++ {
				// Add a batch of tasks
				taskBatch := tasks[:(numTasks/b.N)*(i+1)%numTasks+1]
				tm.AddTasks(taskBatch)
				
				// Mark many tasks as completed
				for j, task := range taskBatch {
					if j%5 != 0 { // Mark 80% as completed
						tm.delTaskInQueue(task.GetID())
					}
				}
			}
			
			b.StopTimer()
		})
	}
}

// BlockingProvider is a custom provider that blocks indefinitely for testing
type BlockingProvider struct {
	name string
	waitCh chan struct{}
}

func NewBlockingProvider(name string) *BlockingProvider {
	return &BlockingProvider{
		name: name,
		waitCh: make(chan struct{}),
	}
}

func (p *BlockingProvider) Handle(task ITask, server string) error {
	// Block until test is done
	<-p.waitCh
	return nil
}

func (p *BlockingProvider) Name() string {
	return p.name
}

func (p *BlockingProvider) Unblock() {
	close(p.waitCh)
}

// TestCompactionSimple provides a lightweight test for compaction functionality
func TestCompactionSimple(t *testing.T) {
	// Create a logger that won't output during tests
	logger := zerolog.Nop()
	
	// Create provider that blocks indefinitely
	provider := NewBlockingProvider("test-provider") 
	providers := []IProvider{provider}
	servers := map[string][]string{
		"test-provider": {"server1", "server2"},
	}
	
	// Create task manager with compaction DISABLED (to avoid race conditions)
	tm := NewTaskManagerWithOptions(&providers, servers, &logger, func(string, string) time.Duration {
		return 5 * time.Second // Long timeout
	}, &TaskManagerOptions{
		EnablePooling:        true,
		EnableCompaction:     false, // Disable automatic compaction for test
		CompactionThreshold:  10,    // Low threshold for testing
	})
	
	// Start the task manager
	tm.Start()
	defer func() {
		// Unblock tasks before shutdown
		provider.Unblock()
		tm.Shutdown()
	}()
	
	// Add some tasks
	taskCount := 50
	tasks := make([]ITask, 0, taskCount)
	for i := 0; i < taskCount; i++ {
		// Create a mock task
		task := &MockTask{
			id:          fmt.Sprintf("task-%d", i),
			createdAt:   time.Now(),
			maxRetries:  3,
			priority:    i % 5,
			provider:    provider,
			timeout:     10 * time.Second, // Long timeout
		}
		tm.AddTask(task)
		tasks = append(tasks, task)
	}
	
	// Get provider data with lock to avoid race condition
	pd := tm.providers["test-provider"]
	
	// Wait a bit to ensure all tasks are added to queue
	time.Sleep(100 * time.Millisecond)
	
	// Check queue size before compaction with lock
	pd.taskQueueLock.Lock()
	beforeSize := len(pd.taskQueue)
	pd.taskQueueLock.Unlock()
	
	t.Logf("Added %d tasks, queue size before compaction: %d", taskCount, beforeSize)
	
	if beforeSize < 20 {
		t.Fatalf("Expected at least 20 tasks in queue, got %d", beforeSize)
	}
	
	// Mark half the tasks as completed
	markCount := 25
	for i := 0; i < markCount; i++ {
		tm.delTaskInQueue(tasks[i].GetID())
	}
	
	t.Logf("Marked %d tasks as completed", markCount)
	
	// Wait a bit to ensure task queue map is updated
	time.Sleep(100 * time.Millisecond)
	
	// Force manual compaction (which acquires its own lock)
	compacted := tm.compactProviderQueue("test-provider", pd)
	
	// Check queue size after compaction with lock
	pd.taskQueueLock.Lock()
	afterSize := len(pd.taskQueue)
	pd.taskQueueLock.Unlock()
	
	// Log results
	t.Logf("Before: %d, After: %d, Compacted: %d", beforeSize, afterSize, compacted)
	
	// Test passes if compaction had an effect
	if compacted == 0 {
		t.Errorf("Compaction had no effect")
	}
	
	if afterSize >= beforeSize {
		t.Errorf("Queue size didn't decrease after compaction: %d -> %d", beforeSize, afterSize)
	}
}