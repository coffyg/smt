package smt

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestServerResourceLeak specifically tests for leaking server resources in the two-level dispatch system
// by simulating situations where tasks might fail or timeout during handling
func TestServerResourceLeak(t *testing.T) {
	// Create a logger
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()
	
	// These settings are designed to create a high likelihood of resource leaks
	const (
		totalTasks = 100
		numServers = 3
		totalDuration = 10 * time.Second
		workerPoolSize = 2 // Very small worker pool to cause contention
	)
	
	// Create a provider that deliberately causes problems
	provider := &LeakyProvider{
		name: "leak_provider",
		panicRate: 5,  // 1 in X chance of panic
		timeoutRate: 7, // 1 in X chance of timing out
		errorRate: 3,   // 1 in X chance of error
	}
	providers := []IProvider{provider}
	
	// Create servers - use a very small number to make it easier to detect when all are stuck
	servers := make(map[string][]string)
	serverList := make([]string, numServers)
	for i := 0; i < numServers; i++ {
		serverList[i] = fmt.Sprintf("server-%d", i)
	}
	servers["leak_provider"] = serverList
	
	// Use a very short timeout to increase chance of timeouts
	getTimeout := func(callback, provider string) time.Duration {
		return 20 * time.Millisecond // Extremely short timeout
	}
	
	// Create task manager with two-level dispatch enabled (should trigger issue)
	tm := NewTaskManagerWithOptions(&providers, servers, &logger, getTimeout, &TaskManagerOptions{
		EnablePooling:  true,
		EnableTwoLevel: true, // This is key for reproducing the issue
		PoolConfig: &PoolConfig{
			PreWarmSize:  100,
			TrackStats:   false,
			PreWarmAsync: true,
		},
	})
	
	// Use a helper function to check if all servers get "stuck"
	isAllServersBusy := func() bool {
		if pd, ok := tm.providers["leak_provider"]; ok {
			// Try to read from the available servers channel with a timeout
			// If no servers are available within the timeout, they might be leaked
			select {
			case server := <-pd.availableServers:
				// Put it back
				pd.availableServers <- server
				return false
			case <-time.After(100 * time.Millisecond):
				// Consider all servers busy after timeout
				return true
			}
		}
		return false
	}
	
	// Start the task manager
	tm.Start()
	defer tm.Shutdown()
	
	// Track completion status
	var wg sync.WaitGroup
	var completedCount int32
	var failedCount int32
	
	// Create tasks that will be added continuously
	createTask := func(id int) ITask {
		taskID := fmt.Sprintf("task-%d", id)
		wg.Add(1)
		
		return &ServerLeakTestTask{
			id:          taskID,
			provider:    provider,
			callbackName: "callback",
			retries:     0,
			maxRetries:  2,
			wg:          &wg,
			completedPtr: &completedCount,
			failedPtr:   &failedCount,
		}
	}
	
	// Add tasks until we either run for the max duration or hit the task limit
	startTime := time.Now()
	var totalAddedTasks int32
	stuckDetected := false
	
	// Continuously add tasks and check for stuck servers
	t.Logf("Starting task submission test")
	for i := 0; i < totalTasks && time.Since(startTime) < totalDuration; i++ {
		// Add a task
		task := createTask(i)
		if tm.AddTask(task) {
			atomic.AddInt32(&totalAddedTasks, 1)
		}
		
		// Every few tasks, check if all servers are busy
		if i%5 == 0 && isAllServersBusy() {
			// Check if there are completed tasks - if there are, servers aren't truly stuck
			completed := atomic.LoadInt32(&completedCount)
			failed := atomic.LoadInt32(&failedCount)
			
			// Only consider it stuck if we've added some tasks but none are completing
			if completed+failed > 0 && isAllServersBusy() {
				// Still could be just busy, wait a bit and check again
				time.Sleep(500 * time.Millisecond)
				
				// If still all busy after waiting, we likely have stuck servers
				if isAllServersBusy() {
					stuckDetected = true
					t.Logf("All servers appear to be stuck! Added: %d, Completed: %d, Failed: %d", 
						atomic.LoadInt32(&totalAddedTasks), 
						atomic.LoadInt32(&completedCount), 
						atomic.LoadInt32(&failedCount))
					break
				}
			}
		}
		
		// Small pause between task additions
		time.Sleep(10 * time.Millisecond)
	}
	
	// Wait for completion with a timeout
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()
	
	// Wait for tasks to complete or timeout
	allCompleted := false
	select {
	case <-waitCh:
		allCompleted = true
		t.Logf("All tasks completed successfully")
	case <-time.After(5 * time.Second):
		t.Logf("Timeout waiting for task completion")
	}
	
	// Check final stats
	finalAdded := atomic.LoadInt32(&totalAddedTasks)
	finalCompleted := atomic.LoadInt32(&completedCount)
	finalFailed := atomic.LoadInt32(&failedCount)
	finalStuck := finalAdded - finalCompleted - finalFailed
	
	t.Logf("Final stats: Added: %d, Completed: %d, Failed: %d, Unaccounted: %d", 
		finalAdded, finalCompleted, finalFailed, finalStuck)
	
	// This should fail when server resources are being leaked
	if stuckDetected || (finalStuck > 0 && !allCompleted) {
		t.Errorf("Server resource leak detected: %d tasks unaccounted for", finalStuck)
	}
}

// ServerLeakTestTask is designed to test server resource leaks
type ServerLeakTestTask struct {
	id          string
	provider    IProvider
	callbackName string
	retries     int
	maxRetries  int
	
	wg          *sync.WaitGroup
	completedPtr *int32
	failedPtr   *int32
	
	mu          sync.Mutex
	completed   bool
	failed      bool
}

func (t *ServerLeakTestTask) GetID() string {
	return t.id
}

func (t *ServerLeakTestTask) GetProvider() IProvider {
	return t.provider
}

func (t *ServerLeakTestTask) GetRetries() int {
	return t.retries
}

func (t *ServerLeakTestTask) GetMaxRetries() int {
	return t.maxRetries
}

func (t *ServerLeakTestTask) UpdateRetries(retries int) error {
	t.retries = retries
	return nil
}

func (t *ServerLeakTestTask) MarkAsSuccess(timeMs int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	if !t.completed && !t.failed {
		t.completed = true
		atomic.AddInt32(t.completedPtr, 1)
	}
}

func (t *ServerLeakTestTask) MarkAsFailed(timeMs int64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	if !t.completed && !t.failed {
		t.failed = true
		atomic.AddInt32(t.failedPtr, 1)
	}
}

func (t *ServerLeakTestTask) GetTimeout() time.Duration {
	return 0 // Use provider default
}

func (t *ServerLeakTestTask) GetCallbackName() string {
	return t.callbackName
}

func (t *ServerLeakTestTask) GetPriority() int {
	return 0
}

func (t *ServerLeakTestTask) GetCreatedAt() time.Time {
	return time.Time{}
}

func (t *ServerLeakTestTask) GetTaskGroup() ITaskGroup {
	return nil
}

func (t *ServerLeakTestTask) UpdateLastError(errString string) error {
	return nil
}

func (t *ServerLeakTestTask) OnStart() {
	// Called when task starts
}

func (t *ServerLeakTestTask) OnComplete() {
	t.wg.Done()
}

// LeakyProvider deliberately causes issues with task execution
type LeakyProvider struct {
	name       string
	panicRate  int // 1 in X chance of panic
	timeoutRate int // 1 in X chance of timeout (sleep longer than timeout)
	errorRate  int // 1 in X chance of error
	
	mu         sync.Mutex
	counter    int
}

func (p *LeakyProvider) Name() string {
	return p.name
}

func (p *LeakyProvider) Handle(task ITask, server string) error {
	p.mu.Lock()
	p.counter++
	counter := p.counter
	p.mu.Unlock()
	
	// Handle based on counter value
	switch {
	case counter%p.panicRate == 0:
		// Simulate a panic
		panic(fmt.Sprintf("simulated panic from task %s on server %s", task.GetID(), server))
		
	case counter%p.timeoutRate == 0:
		// Simulate a timeout by sleeping longer than the timeout
		time.Sleep(50 * time.Millisecond)
		return nil
		
	case counter%p.errorRate == 0:
		// Return an error
		return fmt.Errorf("simulated error from task %s on server %s", task.GetID(), server)
		
	default:
		// Normal execution - very quick
		time.Sleep(5 * time.Millisecond)
		return nil
	}
}