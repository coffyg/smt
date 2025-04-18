package smt

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestTaskLeakUnderLoad tests that the server return mechanism works correctly
// under controlled load. The test creates a limited number of servers and ensures
// that tasks don't get stuck waiting for server resources.
func TestTaskLeakUnderLoad(t *testing.T) {
	// Skip test in short mode
	if testing.Short() {
		t.Skip("Skipping leak test in short mode")
	}

	// Create a logger with console output
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()
	
	// Create a single provider with multiple servers
	provider := &DelayedProvider{
		name:       "provider",
		delayRange: [2]int{1, 3}, // Very short delay range (1-3ms)
		failRate:   5,            // 5% failure rate
	}
	providers := []IProvider{provider}
	
	// Configure a simple server topology
	servers := make(map[string][]string)
	// Use just 2 servers to create contention
	servers["provider"] = []string{"server1", "server2"}
	
	// Use a reasonable timeout
	getTimeout := func(callback, provider string) time.Duration {
		return 50 * time.Millisecond
	}
	
	// Initialize the task manager
	t.Log("Initializing task manager...")
	InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)
	defer TaskQueueManagerInstance.Shutdown()
	
	// Create a small number of tasks (should be more than the servers)
	const totalTasks = 50
	runningTasks := int32(totalTasks)
	
	// Use a WaitGroup to track task completion
	var wg sync.WaitGroup
	wg.Add(totalTasks)
	
	// Counters for task results
	successCount := int32(0)
	errorCount := int32(0)
	timeoutCount := int32(0)
	
	// Create tasks and add them to the task manager
	t.Log("Adding tasks...")
	
	for i := 0; i < totalTasks; i++ {
		taskID := fmt.Sprintf("leak-test-task-%d", i)
		
		// Create task with our provider
		task := NewLeakTestTask(taskID, &wg, provider, &runningTasks, &successCount, &errorCount, &timeoutCount)
		
		// Add task to the manager
		TaskQueueManagerInstance.AddTask(task)
		
		// Add a small delay between tasks (1ms)
		time.Sleep(time.Millisecond)
	}
	
	// Wait for tasks to complete with a reasonable timeout
	t.Log("Waiting for tasks to complete...")
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()
	
	// Use a reasonable timeout
	timeoutPeriod := 10 * time.Second
	select {
	case <-waitCh:
		t.Log("All tasks completed successfully")
	case <-time.After(timeoutPeriod):
		// Check for stuck tasks
		tasksLeft := atomic.LoadInt32(&runningTasks)
		
		// Since we're using a controlled test with small delays,
		// all tasks should complete
		t.Errorf("TIMEOUT: %d tasks still running after %s", 
			tasksLeft, timeoutPeriod)
		
		// Print task statistics
		success := atomic.LoadInt32(&successCount)
		failures := atomic.LoadInt32(&errorCount)
		timeouts := atomic.LoadInt32(&timeoutCount)
		
		t.Logf("Completed: %d, Failed: %d, Timeouts: %d, Still running: %d, Total: %d",
			success, failures, timeouts, tasksLeft, totalTasks)
	}
	
	// Final stats
	success := atomic.LoadInt32(&successCount)
	failures := atomic.LoadInt32(&errorCount)
	timeouts := atomic.LoadInt32(&timeoutCount)
	tasksLeft := atomic.LoadInt32(&runningTasks)
	
	t.Logf("FINAL STATS: Completed: %d, Failed: %d, Timeouts: %d, Still running: %d, Total: %d",
		success, failures, timeouts, tasksLeft, totalTasks)
	
	// Ensure all tasks completed
	if tasksLeft > 0 {
		t.Errorf("Task leak detected: %d tasks did not complete", tasksLeft)
	}
}

// generateTaskIDWithData creates a task ID with embedded data
func generateTaskIDWithData(index int) string {
	return "leak-test-task-" + time.Now().Format("150405") + "-" + string(rune(65+(index%26)))
}

// DelayedProvider simulates a provider with variable response times
type DelayedProvider struct {
	name       string
	delayRange [2]int // Min/max milliseconds of delay
	failRate   int    // Percentage chance of failure (0-100)
}

func (p *DelayedProvider) Name() string {
	return p.name
}

func (p *DelayedProvider) Handle(task ITask, server string) error {
	// Introduce a very short delay (1-3ms)
	delayMs := p.delayRange[0]
	if p.delayRange[1] > p.delayRange[0] {
		delayMs += int(time.Now().UnixNano()%int64(p.delayRange[1]-p.delayRange[0]))
	}
	
	time.Sleep(time.Duration(delayMs) * time.Millisecond)
	
	// Simulate occasional failures
	if time.Now().UnixNano()%100 < int64(p.failRate) {
		return fmt.Errorf("simulated error for task %s", task.GetID())
	}
	
	return nil
}

// LeakTestTask implements ITask for leak testing
type LeakTestTask struct {
	id            string
	provider      IProvider
	wg            *sync.WaitGroup
	runningCount  *int32
	successCount  *int32
	errorCount    *int32
	timeoutCount  *int32
	maxRetries    int
	retries       int
	callbackName  string
	priority      int
	started       bool
	completed     bool
	failed        bool
	createdAt     time.Time
	mu            sync.Mutex
}

func NewLeakTestTask(id string, wg *sync.WaitGroup, provider IProvider, runningCount, successCount, errorCount, timeoutCount *int32) *LeakTestTask {
	return &LeakTestTask{
		id:            id,
		provider:      provider,
		wg:            wg,
		runningCount:  runningCount,
		successCount:  successCount,
		errorCount:    errorCount,
		timeoutCount:  timeoutCount,
		maxRetries:    2,
		callbackName:  "test-callback",
		priority:      0,
		createdAt:     time.Now(),
	}
}

func (t *LeakTestTask) GetID() string {
	return t.id
}

func (t *LeakTestTask) GetProvider() IProvider {
	return t.provider
}

func (t *LeakTestTask) GetRetries() int {
	return t.retries
}

func (t *LeakTestTask) GetMaxRetries() int {
	return t.maxRetries
}

func (t *LeakTestTask) UpdateRetries(retries int) error {
	t.retries = retries
	return nil
}

func (t *LeakTestTask) MarkAsSuccess(timeMs int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	if !t.completed && !t.failed {
		t.completed = true
		atomic.AddInt32(t.successCount, 1)
	}
}

func (t *LeakTestTask) MarkAsFailed(timeMs int64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	if !t.completed && !t.failed {
		t.failed = true
		
		// Check if error is timeout
		if err != nil && err.Error() == "smt.HandleWithTimeout: task timed out" {
			atomic.AddInt32(t.timeoutCount, 1)
		} else {
			atomic.AddInt32(t.errorCount, 1)
		}
	}
}

func (t *LeakTestTask) GetTimeout() time.Duration {
	return 75 * time.Millisecond
}

func (t *LeakTestTask) GetCallbackName() string {
	return t.callbackName
}

func (t *LeakTestTask) GetPriority() int {
	return t.priority
}

func (t *LeakTestTask) GetCreatedAt() time.Time {
	return t.createdAt
}

func (t *LeakTestTask) GetTaskGroup() ITaskGroup {
	return nil
}

func (t *LeakTestTask) UpdateLastError(errString string) error {
	return nil
}

func (t *LeakTestTask) OnStart() {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	if !t.started {
		t.started = true
	}
}

func (t *LeakTestTask) OnComplete() {
	t.mu.Lock()
	
	// Ensure we decrement running count and signal completion only once
	if t.started && (t.completed || t.failed) {
		atomic.AddInt32(t.runningCount, -1)
		
		// Extract waitgroup reference in case this task gets reused
		wg := t.wg
		t.mu.Unlock()
		
		wg.Done()
		return
	}
	
	t.mu.Unlock()
}