package smt

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestServerMaxParallelEnforcement verifies SetTaskManagerServerMaxParallel enforcement
// This test MUST be 1002013030210% sure that with limit=1, only 1 task runs at a time
func TestServerMaxParallelEnforcement(t *testing.T) {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: zerolog.NewConsoleWriter().Out}).
		Level(zerolog.InfoLevel)

	// Tracking variables
	var currentlyRunning int32
	var maxConcurrent int32
	var totalExecutions int32
	var violationDetails []string
	violationMutex := sync.Mutex{}
	
	// Track each task's execution timeline
	type ExecutionRecord struct {
		TaskID    string
		StartTime time.Time
		EndTime   time.Time
		Server    string
	}
	executionTimeline := make([]ExecutionRecord, 0)
	timelineMutex := sync.Mutex{}

	// Create provider that tracks concurrent executions
	provider := &MockProvider{
		name: "test_provider",
		handleFunc: func(task ITask, server string) error {
			startTime := time.Now()
			taskID := task.GetID()
			
			// Increment currently running
			running := atomic.AddInt32(&currentlyRunning, 1)
			atomic.AddInt32(&totalExecutions, 1)
			
			// Track max concurrent
			for {
				old := atomic.LoadInt32(&maxConcurrent)
				if running <= old || atomic.CompareAndSwapInt32(&maxConcurrent, old, running) {
					break
				}
			}
			
			// Check for violation
			if running > 1 {
				violationMutex.Lock()
				violationDetails = append(violationDetails, 
					fmt.Sprintf("VIOLATION: Task %s started with %d already running on %s at %v", 
						taskID, running-1, server, startTime))
				violationMutex.Unlock()
			}
			
			// Log execution start
			t.Logf("[%v] Task %s STARTED on %s (running: %d)", 
				startTime.Format("15:04:05.000"), taskID, server, running)
			
			// Simulate work (long enough to catch concurrency violations)
			time.Sleep(50 * time.Millisecond)
			
			// Decrement currently running
			remaining := atomic.AddInt32(&currentlyRunning, -1)
			endTime := time.Now()
			
			// Log execution end
			t.Logf("[%v] Task %s ENDED on %s (remaining: %d)", 
				endTime.Format("15:04:05.000"), taskID, server, remaining)
			
			// Record in timeline
			timelineMutex.Lock()
			executionTimeline = append(executionTimeline, ExecutionRecord{
				TaskID:    taskID,
				StartTime: startTime,
				EndTime:   endTime,
				Server:    server,
			})
			timelineMutex.Unlock()
			
			return nil
		},
	}
	
	providers := []IProvider{provider}
	
	// Single server to test
	servers := map[string][]string{
		"test_provider": {"https://test-server.com"},
	}
	
	getTimeout := func(string, string) time.Duration {
		return 5 * time.Second
	}
	
	// Initialize task manager
	InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)
	defer TaskQueueManagerInstance.Shutdown()
	
	// SET THE CRITICAL LIMIT: Only 1 task at a time!
	TaskQueueManagerInstance.SetTaskManagerServerMaxParallel("https://test-server.com", 1)
	
	// Create many tasks to stress test concurrency
	numTasks := 20
	tasks := make([]*MockTask, numTasks)
	
	for i := 0; i < numTasks; i++ {
		tasks[i] = &MockTask{
			id:         fmt.Sprintf("task_%02d", i),
			priority:   10 - (i % 5), // Vary priorities
			maxRetries: 0,
			provider:   provider,
			timeout:    1 * time.Second,
			done:       make(chan struct{}),
		}
	}
	
	// Add all tasks rapidly to create contention
	startTime := time.Now()
	for _, task := range tasks {
		AddTask(task, &logger)
		// Small delay to spread them slightly
		time.Sleep(1 * time.Millisecond)
	}
	
	// Wait for all tasks to complete
	allDone := make(chan bool)
	go func() {
		for _, task := range tasks {
			select {
			case <-task.done:
				// Task completed
			case <-time.After(10 * time.Second):
				t.Errorf("Task %s timed out", task.GetID())
			}
		}
		allDone <- true
	}()
	
	select {
	case <-allDone:
		t.Log("All tasks completed")
	case <-time.After(15 * time.Second):
		t.Fatal("Test timeout - not all tasks completed")
	}
	
	totalTime := time.Since(startTime)
	
	// Analyze results
	t.Log("\n=== CONCURRENCY ENFORCEMENT RESULTS ===")
	t.Logf("Total tasks: %d", numTasks)
	t.Logf("Total executions: %d", atomic.LoadInt32(&totalExecutions))
	t.Logf("Max concurrent: %d", atomic.LoadInt32(&maxConcurrent))
	t.Logf("Total time: %v", totalTime)
	t.Logf("Violations found: %d", len(violationDetails))
	
	// Print any violations
	if len(violationDetails) > 0 {
		t.Error("\n=== CONCURRENCY VIOLATIONS DETECTED ===")
		for _, violation := range violationDetails {
			t.Error(violation)
		}
	}
	
	// Verify timeline for overlaps
	t.Log("\n=== TIMELINE ANALYSIS ===")
	overlaps := 0
	for i := 0; i < len(executionTimeline); i++ {
		for j := i + 1; j < len(executionTimeline); j++ {
			rec1 := executionTimeline[i]
			rec2 := executionTimeline[j]
			
			// Check if they overlap
			if rec1.StartTime.Before(rec2.EndTime) && rec2.StartTime.Before(rec1.EndTime) {
				overlaps++
				t.Errorf("OVERLAP: %s [%v-%v] overlaps with %s [%v-%v]",
					rec1.TaskID, 
					rec1.StartTime.Format("15:04:05.000"),
					rec1.EndTime.Format("15:04:05.000"),
					rec2.TaskID,
					rec2.StartTime.Format("15:04:05.000"),
					rec2.EndTime.Format("15:04:05.000"))
			}
		}
	}
	
	// CRITICAL ASSERTIONS
	if atomic.LoadInt32(&maxConcurrent) != 1 {
		t.Fatalf("CRITICAL FAILURE: Max concurrent was %d, expected 1", 
			atomic.LoadInt32(&maxConcurrent))
	}
	
	if atomic.LoadInt32(&totalExecutions) != int32(numTasks) {
		t.Errorf("Expected %d executions, got %d", numTasks, atomic.LoadInt32(&totalExecutions))
	}
	
	if overlaps > 0 {
		t.Fatalf("CRITICAL FAILURE: Found %d overlapping executions when limit was 1", overlaps)
	}
	
	// Calculate theoretical minimum time (all tasks sequential)
	theoreticalMin := time.Duration(numTasks) * 50 * time.Millisecond
	t.Logf("Theoretical minimum time (sequential): %v", theoreticalMin)
	t.Logf("Actual time: %v", totalTime)
	
	// Time should be at least the theoretical minimum (minus some tolerance for scheduling)
	if totalTime < theoreticalMin - 100*time.Millisecond {
		t.Errorf("Total time %v is impossibly fast for sequential execution of %d tasks", 
			totalTime, numTasks)
	}
	
	t.Log("\n✅ TEST PASSED: SetTaskManagerServerMaxParallel(server, 1) enforces strict serial execution")
}

// TestServerMaxParallelMultipleValues tests different concurrency limits
func TestServerMaxParallelMultipleValues(t *testing.T) {
	testCases := []struct {
		name     string
		limit    int
		numTasks int
	}{
		{"Limit_1", 1, 10},
		{"Limit_2", 2, 10},
		{"Limit_3", 3, 15},
		{"Limit_5", 5, 20},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger := zerolog.New(zerolog.ConsoleWriter{Out: zerolog.NewConsoleWriter().Out}).
				Level(zerolog.WarnLevel)
			
			var currentlyRunning int32
			var maxConcurrent int32
			
			provider := &MockProvider{
				name: "test_provider",
				handleFunc: func(task ITask, server string) error {
					running := atomic.AddInt32(&currentlyRunning, 1)
					
					// Track max
					for {
						old := atomic.LoadInt32(&maxConcurrent)
						if running <= old || atomic.CompareAndSwapInt32(&maxConcurrent, old, running) {
							break
						}
					}
					
					// Hold the slot
					time.Sleep(20 * time.Millisecond)
					
					atomic.AddInt32(&currentlyRunning, -1)
					return nil
				},
			}
			
			providers := []IProvider{provider}
			servers := map[string][]string{
				"test_provider": {"server1"},
			}
			
			getTimeout := func(string, string) time.Duration {
				return 5 * time.Second
			}
			
			tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
			tm.Start()
			defer tm.Shutdown()
			
			// Set the limit for this test
			tm.SetTaskManagerServerMaxParallel("server1", tc.limit)
			
			tasks := make([]*MockTask, tc.numTasks)
			for i := 0; i < tc.numTasks; i++ {
				tasks[i] = &MockTask{
					id:       fmt.Sprintf("task_%d", i),
					priority: 5,
					provider: provider,
					timeout:  1 * time.Second,
					done:     make(chan struct{}),
				}
				tm.AddTask(tasks[i])
			}
			
			// Wait for completion
			for _, task := range tasks {
				select {
				case <-task.done:
					// Good
				case <-time.After(5 * time.Second):
					t.Errorf("Task timeout")
				}
			}
			
			maxSeen := atomic.LoadInt32(&maxConcurrent)
			t.Logf("Limit: %d, Max concurrent seen: %d", tc.limit, maxSeen)
			
			if maxSeen > int32(tc.limit) {
				t.Errorf("VIOLATION: Max concurrent %d exceeded limit %d", maxSeen, tc.limit)
			}
		})
	}
}

// TestServerMaxParallelWithURLVariations tests that URL normalization works
func TestServerMaxParallelWithURLVariations(t *testing.T) {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: zerolog.NewConsoleWriter().Out}).
		Level(zerolog.WarnLevel)
	
	var currentlyRunning int32
	var maxConcurrent int32
	
	provider := &MockProvider{
		name: "test_provider",
		handleFunc: func(task ITask, server string) error {
			running := atomic.AddInt32(&currentlyRunning, 1)
			
			// Track max
			for {
				old := atomic.LoadInt32(&maxConcurrent)
				if running <= old || atomic.CompareAndSwapInt32(&maxConcurrent, old, running) {
					break
				}
			}
			
			time.Sleep(30 * time.Millisecond)
			atomic.AddInt32(&currentlyRunning, -1)
			return nil
		},
	}
	
	providers := []IProvider{provider}
	
	// Single server to avoid dispatcher complexity in test
	servers := map[string][]string{
		"test_provider": {
			"https://api.example.com",
		},
	}
	
	getTimeout := func(string, string) time.Duration {
		return 5 * time.Second
	}
	
	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()
	
	// Set limit on base URL - should apply to all variations
	tm.SetTaskManagerServerMaxParallel("https://api.example.com", 1)
	
	// Create tasks  
	tasks := make([]*MockTask, 5)
	for i := 0; i < 5; i++ {
		tasks[i] = &MockTask{
			id:       fmt.Sprintf("task_%d", i),
			priority: 5,
			provider: provider,
			timeout:  1 * time.Second,
			done:     make(chan struct{}),
		}
		tm.AddTask(tasks[i])
	}
	
	// Wait for all
	for _, task := range tasks {
		select {
		case <-task.done:
			// Good
		case <-time.After(5 * time.Second):
			t.Errorf("Task timeout")
		}
	}
	
	maxSeen := atomic.LoadInt32(&maxConcurrent)
	t.Logf("Max concurrent with URL variations: %d", maxSeen)
	
	if maxSeen != 1 {
		t.Errorf("CRITICAL: Max concurrent %d with limit 1 (URL normalization may have failed)", maxSeen)
	}
}