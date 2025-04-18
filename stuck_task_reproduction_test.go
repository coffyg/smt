package smt

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Mock provider for testing
type StuckTestProvider struct {
	name string
	mu   sync.Mutex
	taskCount int
	callCount int
}

func (p *StuckTestProvider) Name() string {
	return p.name
}

func (p *StuckTestProvider) Handle(task ITask, server string) error {
	p.mu.Lock()
	p.callCount++
	p.mu.Unlock()
	
	// Simulate variable execution time
	time.Sleep(time.Duration(5+time.Now().UnixNano()%20) * time.Millisecond)
	
	// Sometimes return an error to trigger retries
	if time.Now().UnixNano()%10 == 0 {
		return fmt.Errorf("simulated error")
	}
	
	return nil
}

// Mock task group implementation
type StuckMockTaskGroup struct {
	completedCount int
	taskCount      int
	mu             sync.Mutex
}

func (g *StuckMockTaskGroup) MarkComplete() error {
	return nil
}

func (g *StuckMockTaskGroup) GetTaskCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.taskCount
}

func (g *StuckMockTaskGroup) GetTaskCompletedCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.completedCount
}

func (g *StuckMockTaskGroup) UpdateTaskCompletedCount(count int) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.completedCount = count
	return nil
}

type StuckMockTask struct {
	ID          string
	Provider    IProvider
	Priority    int
	Retries     int
	MaxRetries  int
	CallbackName string
	TimeoutMs   int64
	Complete    bool
	Failed      bool
	completeCh  chan struct{}
	mu          sync.Mutex
	CreatedAt   time.Time
	taskGroup   *StuckMockTaskGroup
}

func NewStuckMockTask(id string, provider IProvider, callbackName string) *StuckMockTask {
	return &StuckMockTask{
		ID:          id,
		Provider:    provider,
		MaxRetries:  3,
		CallbackName: callbackName,
		completeCh:  make(chan struct{}),
		CreatedAt:   time.Now(),
		taskGroup:   &StuckMockTaskGroup{taskCount: 1},
	}
}

func (t *StuckMockTask) GetID() string {
	return t.ID
}

func (t *StuckMockTask) GetProvider() IProvider {
	return t.Provider
}

func (t *StuckMockTask) GetRetries() int {
	return t.Retries
}

func (t *StuckMockTask) GetMaxRetries() int {
	return t.MaxRetries
}

func (t *StuckMockTask) UpdateRetries(retries int) error {
	t.Retries = retries
	return nil
}

func (t *StuckMockTask) MarkAsSuccess(timeMs int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Complete = true
	t.TimeoutMs = timeMs
}

func (t *StuckMockTask) MarkAsFailed(timeMs int64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Failed = true
	t.TimeoutMs = timeMs
}

func (t *StuckMockTask) IsCompleted() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.Complete
}

func (t *StuckMockTask) IsFailed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.Failed
}

func (t *StuckMockTask) GetTimeout() time.Duration {
	return 0 // Use default timeout
}

func (t *StuckMockTask) UpdateLastError(errString string) error {
	return nil
}

func (t *StuckMockTask) GetCallbackName() string {
	return t.CallbackName
}

func (t *StuckMockTask) GetPriority() int {
	return t.Priority
}

func (t *StuckMockTask) GetCreatedAt() time.Time {
	return t.CreatedAt
}

func (t *StuckMockTask) GetTaskGroup() ITaskGroup {
	return t.taskGroup
}

func (t *StuckMockTask) OnStart() {
	// Nothing to do
}

func (t *StuckMockTask) OnComplete() {
	close(t.completeCh)
}

func (t *StuckMockTask) WaitForCompletion(timeout time.Duration) bool {
	select {
	case <-t.completeCh:
		return true
	case <-time.After(timeout):
		return false
	}
}

// TestTaskStuckReproduction tries to reproduce the issue with tasks getting stuck
func TestTaskStuckReproduction(t *testing.T) {
	// Create a logger
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()
	
	// Create providers
	provider1 := &StuckTestProvider{name: "provider1"}
	provider2 := &StuckTestProvider{name: "provider2"}
	provider3 := &StuckTestProvider{name: "provider3"}
	
	providers := []IProvider{provider1, provider2, provider3}
	
	// Create server configuration - use multiple servers per provider
	// This is key to reproducing the issue - complex server routing
	servers := map[string][]string{
		"provider1": {"server1-1", "server1-2", "server1-3", "server1-4"},
		"provider2": {"server2-1", "server2-2", "server2-3", "server2-4"},
		"provider3": {"server3-1", "server3-2", "server3-3", "server3-4"},
	}
	
	// Get timeout function
	getTimeout := func(callback, provider string) time.Duration {
		// Very short timeout to increase chance of timing issues
		return 30 * time.Millisecond
	}

	// Initialize task manager
	InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)
	defer TaskQueueManagerInstance.Shutdown()
	
	// Create a large number of tasks to simulate load
	const totalTasks = 300
	tasks := make([]*StuckMockTask, totalTasks)
	
	for i := 0; i < totalTasks; i++ {
		providerIdx := i % 3
		provider := providers[providerIdx]
		taskId := fmt.Sprintf("task-%d", i)
		callbackName := fmt.Sprintf("callback-%d", i%5)
		
		tasks[i] = NewStuckMockTask(taskId, provider, callbackName)
	}
	
	// Add tasks in batches with short delays between
	batchSize := 10
	for i := 0; i < totalTasks; i += batchSize {
		end := i + batchSize
		if end > totalTasks {
			end = totalTasks
		}
		
		batchTasks := make([]ITask, 0, batchSize)
		for j := i; j < end; j++ {
			batchTasks = append(batchTasks, tasks[j])
		}
		
		added, _ := TaskQueueManagerInstance.AddTasks(batchTasks)
		t.Logf("Added %d tasks", added)
		
		// Small delay between batches
		time.Sleep(5 * time.Millisecond)
	}
	
	// Wait for tasks to complete with timeout
	completionTimeout := 5 * time.Second
	
	// Wait for completion or timeout
	t.Log("Waiting for tasks to complete...")
	time.Sleep(completionTimeout)
	
	// Check task status
	completed := 0
	failed := 0
	stuck := 0
	
	for i, task := range tasks {
		if task.IsCompleted() {
			completed++
		} else if task.IsFailed() {
			failed++
		} else {
			stuck++
			t.Logf("Task %d (ID: %s) stuck - neither completed nor failed", i, task.ID)
		}
	}
	
	t.Logf("TASK STATS: Completed: %d, Failed: %d, Stuck: %d, Total: %d", 
		completed, failed, stuck, totalTasks)
	
	if stuck > 0 {
		t.Errorf("Found %d stuck tasks (neither completed nor failed)", stuck)
	}
}

func TestTaskStuckWithHighConcurrency(t *testing.T) {
	// Create a logger
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()
	
	// Create provider with many servers
	providerNames := []string{"p1", "p2", "p3", "p4", "p5"}
	providers := make([]IProvider, 0, len(providerNames))
	
	for _, name := range providerNames {
		providers = append(providers, &StuckTestProvider{name: name})
	}
	
	// Create complex server configuration with many servers per provider
	servers := make(map[string][]string)
	
	for _, name := range providerNames {
		serverList := make([]string, 0, 10)
		for j := 0; j < 10; j++ {
			serverList = append(serverList, fmt.Sprintf("%s-server-%d", name, j))
		}
		servers[name] = serverList
	}
	
	// Get timeout function with very short timeouts
	getTimeout := func(callback, provider string) time.Duration {
		return 20 * time.Millisecond
	}

	// Initialize task manager
	InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)
	defer TaskQueueManagerInstance.Shutdown()
	
	// Create many tasks to simulate high load
	const totalTasks = 1000
	tasks := make([]*StuckMockTask, totalTasks)
	
	for i := 0; i < totalTasks; i++ {
		providerIdx := i % len(providerNames)
		provider := providers[providerIdx]
		taskId := fmt.Sprintf("hc-task-%d", i)
		callbackName := fmt.Sprintf("hc-callback-%d", i%10)
		
		tasks[i] = NewStuckMockTask(taskId, provider, callbackName)
	}
	
	// Add tasks concurrently from multiple goroutines to simulate high concurrency
	t.Log("Adding tasks concurrently...")
	var wg sync.WaitGroup
	concurrentAdders := 20
	tasksPerAdder := totalTasks / concurrentAdders
	
	for i := 0; i < concurrentAdders; i++ {
		wg.Add(1)
		go func(startIdx int) {
			defer wg.Done()
			
			startPos := startIdx * tasksPerAdder
			endPos := startPos + tasksPerAdder
			if endPos > totalTasks {
				endPos = totalTasks
			}
			
			for j := startPos; j < endPos; j++ {
				TaskQueueManagerInstance.AddTask(tasks[j])
				
				// Small random delay
				if j%10 == 0 {
					time.Sleep(time.Duration(1+time.Now().UnixNano()%3) * time.Millisecond)
				}
			}
		}(i)
	}
	
	// Wait for tasks to be added
	wg.Wait()
	t.Log("All tasks added, waiting for completion...")
	
	// Wait for completion
	time.Sleep(8 * time.Second)
	
	// Check task status
	completed := 0
	failed := 0
	stuck := 0
	
	for i, task := range tasks {
		if task.IsCompleted() {
			completed++
		} else if task.IsFailed() {
			failed++
		} else {
			stuck++
			if stuck <= 10 { // Only log first 10 stuck tasks
				t.Logf("Task %d (ID: %s) stuck - neither completed nor failed", i, task.ID)
			}
		}
	}
	
	t.Logf("TASK STATS: Completed: %d, Failed: %d, Stuck: %d, Total: %d", 
		completed, failed, stuck, totalTasks)
	
	if stuck > 0 {
		t.Errorf("Found %d stuck tasks (neither completed nor failed)", stuck)
	}
}