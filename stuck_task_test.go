package smt

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestStuckTaskV010vsLatest compares behavior between v0.0.10 style and latest implementation
func TestStuckTaskV010vsLatest(t *testing.T) {
	// Create a logger
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()
	
	// Set up a simple test environment
	testEnvs := []struct {
		name             string
		enableTwoLevel   bool
		enablePooling    bool
		enableAdaptive   bool
		serverCount      int
		taskCount        int
		concurrentAdders int
		expectedStuck    bool
	}{
		{
			name:             "v0.0.10_style",
			enableTwoLevel:   false,
			enablePooling:    false,
			enableAdaptive:   false,
			serverCount:      2,
			taskCount:        500,
			concurrentAdders: 10,
			expectedStuck:    false,
		},
		{
			name:             "current_style",
			enableTwoLevel:   true,
			enablePooling:    true,
			enableAdaptive:   false,
			serverCount:      3,
			taskCount:        1000,
			concurrentAdders: 20, 
			expectedStuck:    true,
		},
	}
	
	for _, env := range testEnvs {
		t.Run(env.name, func(t *testing.T) {
			// Create test providers
			providers := []IProvider{
				&VariableDelayProvider{name: "provider1"},
				&VariableDelayProvider{name: "provider2"},
			}
			
			// Set up server config - use many servers to increase contention
			servers := make(map[string][]string)
			for _, p := range providers {
				serverList := make([]string, 0, env.serverCount)
				for i := 0; i < env.serverCount; i++ {
					serverList = append(serverList, fmt.Sprintf("%s-server-%d", p.Name(), i))
				}
				servers[p.Name()] = serverList
			}
			
			// Very short timeouts to increase chance of timing issues
			getTimeout := func(callback, provider string) time.Duration {
				// Use extremely short timeouts that will likely cause timeout errors
				return 5 * time.Millisecond
			}
			
			// Create custom task manager with specific settings
			options := &TaskManagerOptions{
				EnablePooling:        env.enablePooling,
				EnableTwoLevel:       env.enableTwoLevel,
				EnableAdaptiveTimeout: env.enableAdaptive,
				PoolConfig: &PoolConfig{
					PreWarmSize:  500,
					TrackStats:   false,
					PreWarmAsync: true,
				},
			}
			
			tm := NewTaskManagerWithOptions(&providers, servers, &logger, getTimeout, options)
			tm.Start()
			defer tm.Shutdown()
			
			// For tracking completion
			var wg sync.WaitGroup
			completedCount := int32(0)
			failedCount := int32(0)
			taskCount := env.taskCount
			wg.Add(taskCount)
			
			// Create tasks
			tasks := make([]*StuckTestTaskWithPaths, 0, taskCount)
			for i := 0; i < taskCount; i++ {
				providerIdx := i % len(providers)
				provider := providers[providerIdx]
				taskId := fmt.Sprintf("%s-task-%d", env.name, i)
				callbackName := fmt.Sprintf("callback-%d", i%5)
				
				task := NewStuckTestTaskWithPaths(taskId, provider, callbackName, &wg, &completedCount, &failedCount)
				tasks = append(tasks, task)
			}
			
			// Add tasks with high concurrency
			t.Logf("Adding %d tasks with %d concurrent adders...", taskCount, env.concurrentAdders)
			tasksPerAdder := taskCount / env.concurrentAdders
			var addWg sync.WaitGroup
			addWg.Add(env.concurrentAdders)
			
			// High concurrency adders to create contention
			startTime := time.Now()
			for i := 0; i < env.concurrentAdders; i++ {
				go func(idx int) {
					defer addWg.Done()
					
					startPos := idx * tasksPerAdder
					endPos := (idx + 1) * tasksPerAdder
					if idx == env.concurrentAdders-1 {
						endPos = taskCount
					}
					
					// Add tasks very quickly to create contention
					for j := startPos; j < endPos; j++ {
						tm.AddTask(tasks[j])
						
						// Small delay on some tasks to create varied timing
						if j%7 == 0 {
							time.Sleep(time.Millisecond)
						}
					}
				}(i)
			}
			
			// Wait for all tasks to be added
			addWg.Wait()
			t.Logf("All tasks added in %v", time.Since(startTime))
			
			// Track goroutines before waiting to detect potential leaks
			startingGoroutines := runtime.NumGoroutine()
			t.Logf("Current goroutines: %d", startingGoroutines)
			
			// Set a time limit for task completion
			timeoutCh := make(chan struct{})
			go func() {
				defer close(timeoutCh)
				
				// Set a time limit of 5 seconds
				time.Sleep(5 * time.Second)
			}()
			
			// Wait for completion or timeout
			completionCh := make(chan struct{})
			go func() {
				defer close(completionCh)
				wg.Wait()
			}()
			
			// Wait for either completion or timeout
			select {
			case <-completionCh:
				t.Logf("All tasks completed successfully")
			case <-timeoutCh:
				// Check for stuck tasks
				completed := atomic.LoadInt32(&completedCount)
				failed := atomic.LoadInt32(&failedCount)
				stuck := int32(taskCount) - completed - failed
				
				t.Logf("TASK STATS: Completed: %d, Failed: %d, Total: %d", 
					completed, failed, taskCount)
				t.Logf("Stuck tasks: %d", stuck)
				
				// Check error paths
				var inStart, inProcess, inComplete int
				for _, task := range tasks {
					if !task.IsCompleted() && !task.IsFailed() {
						if task.IsInStart() && !task.IsInProcess() && !task.IsInComplete() {
							inStart++
						} else if task.IsInProcess() && !task.IsInComplete() {
							inProcess++
						} else if task.IsInComplete() {
							inComplete++
						}
						
						t.Logf("Task %s stuck - Paths: start=%v, process=%v, complete=%v",
							task.ID, task.IsInStart(), task.IsInProcess(), task.IsInComplete())
					}
				}
				
				t.Logf("Stuck task paths: start=%d, process=%d, complete=%d", 
					inStart, inProcess, inComplete)
				
				// Verify if we expected tasks to be stuck in this env
				if stuck > 0 {
					if env.expectedStuck {
						t.Logf("Found %d stuck tasks as expected in %s configuration", stuck, env.name)
					} else {
						t.Errorf("Found %d stuck tasks in %s configuration, expected none", stuck, env.name)
					}
				} else if env.expectedStuck {
					t.Errorf("Expected stuck tasks in %s configuration but found none", env.name)
				}
			}
			
			// Check goroutine leaks
			time.Sleep(100 * time.Millisecond) // Give goroutines a chance to clean up
			endingGoroutines := runtime.NumGoroutine()
			t.Logf("Ending goroutines: %d (delta: %d)", 
				endingGoroutines, endingGoroutines-startingGoroutines)
			
			// Get final completion numbers
			completed := atomic.LoadInt32(&completedCount)
			failed := atomic.LoadInt32(&failedCount)
			stuck := int32(taskCount) - completed - failed
			
			t.Logf("FINAL TASK STATS: Completed: %d, Failed: %d, Stuck: %d, Total: %d", 
				completed, failed, stuck, taskCount)
		})
	}
}

// StuckTestTaskWithPaths is a task implementation that tracks execution paths
type StuckTestTaskWithPaths struct {
	ID            string
	Provider      IProvider
	CallbackName  string
	Priority      int
	Retries       int
	MaxRetries    int
	TimeoutMs     int64
	
	createdAt     time.Time
	wg            *sync.WaitGroup
	completedCount *int32
	failedCount   *int32
	
	// State flags
	mu           sync.Mutex
	inStartPath  bool
	inProcessPath bool
	inCompletePath bool
	completed    bool
	failed       bool
}

// NewStuckTestTaskWithPaths creates a new task for stuck task testing
func NewStuckTestTaskWithPaths(id string, provider IProvider, callbackName string, 
	wg *sync.WaitGroup, completedCount, failedCount *int32) *StuckTestTaskWithPaths {
	return &StuckTestTaskWithPaths{
		ID:            id,
		Provider:      provider,
		CallbackName:  callbackName,
		MaxRetries:    2,
		createdAt:     time.Now(),
		wg:            wg,
		completedCount: completedCount,
		failedCount:   failedCount,
	}
}

// Path tracking methods
func (t *StuckTestTaskWithPaths) IsInStart() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inStartPath
}

func (t *StuckTestTaskWithPaths) IsInProcess() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inProcessPath
}

func (t *StuckTestTaskWithPaths) IsInComplete() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inCompletePath
}

func (t *StuckTestTaskWithPaths) IsCompleted() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.completed
}

func (t *StuckTestTaskWithPaths) IsFailed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.failed
}

// ITask interface implementation
func (t *StuckTestTaskWithPaths) GetID() string {
	return t.ID
}

func (t *StuckTestTaskWithPaths) GetProvider() IProvider {
	return t.Provider
}

func (t *StuckTestTaskWithPaths) GetRetries() int {
	return t.Retries
}

func (t *StuckTestTaskWithPaths) GetMaxRetries() int {
	return t.MaxRetries
}

func (t *StuckTestTaskWithPaths) UpdateRetries(retries int) error {
	t.Retries = retries
	return nil
}

func (t *StuckTestTaskWithPaths) MarkAsSuccess(timeMs int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.completed && !t.failed {
		t.completed = true
		atomic.AddInt32(t.completedCount, 1)
	}
}

func (t *StuckTestTaskWithPaths) MarkAsFailed(timeMs int64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.completed && !t.failed {
		t.failed = true
		atomic.AddInt32(t.failedCount, 1)
	}
}

func (t *StuckTestTaskWithPaths) GetTimeout() time.Duration {
	return 20 * time.Millisecond
}

func (t *StuckTestTaskWithPaths) GetCallbackName() string {
	return t.CallbackName
}

func (t *StuckTestTaskWithPaths) GetPriority() int {
	return t.Priority
}

func (t *StuckTestTaskWithPaths) GetCreatedAt() time.Time {
	return t.createdAt
}

func (t *StuckTestTaskWithPaths) GetTaskGroup() ITaskGroup {
	return nil
}

func (t *StuckTestTaskWithPaths) UpdateLastError(errString string) error {
	return nil
}

// These two methods track execution path
func (t *StuckTestTaskWithPaths) OnStart() {
	t.mu.Lock()
	t.inStartPath = true
	t.mu.Unlock()
	
	// Delay slightly to make race conditions more likely
	if time.Now().UnixNano()%5 == 0 {
		time.Sleep(time.Millisecond)
	}
	
	t.mu.Lock()
	t.inProcessPath = true
	t.mu.Unlock()
}

func (t *StuckTestTaskWithPaths) OnComplete() {
	t.mu.Lock()
	t.inCompletePath = true
	t.mu.Unlock()
	
	// Signal completion
	t.wg.Done()
}

// VariableDelayProvider simulates a provider with variable response times and failures
type VariableDelayProvider struct {
	name     string
	mu       sync.Mutex
	callCount int
}

func (p *VariableDelayProvider) Name() string {
	return p.name
}

func (p *VariableDelayProvider) Handle(task ITask, server string) error {
	p.mu.Lock()
	p.callCount++
	p.mu.Unlock()
	
	// Randomly sleep to simulate work
	delay := time.Duration(5+time.Now().UnixNano()%15) * time.Millisecond
	time.Sleep(delay)
	
	// Create random failures
	if time.Now().UnixNano()%10 == 0 {
		return fmt.Errorf("simulated error [task=%s provider=%s server=%s]", 
			task.GetID(), p.name, server)
	}
	
	return nil
}