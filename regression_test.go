package smt

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestCompareVersions_Regression tests for regressions between versions
// It should detect the issue with tasks getting stuck in the current version
// but working correctly in v0.0.10
func TestCompareVersions_Regression(t *testing.T) {
	// Simulate a real-world scenario with:
	// 1. Multiple tasks with varied completion times
	// 2. A mix of successful and failing tasks
	// 3. Tasks that hit timeouts
	// 4. Heavy concurrent load
	// This should reproduce the stuck-task issue in the current version
	
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).Level(zerolog.ErrorLevel)
	
	// Define providers with limited servers (critical to reproduce the issue)
	providerName := "provider1"
	provider := &MockProvider{name: providerName}
	providers := []IProvider{provider}
	
	// Use a VERY limited number of servers to increase contention
	servers := map[string][]string{
		providerName: {"server1", "server2"},
	}
	
	// Production-like timeout that's long enough to let tasks complete
	getTimeout := func(callbackName string, providerName string) time.Duration {
		return 10 * time.Second
	}
	
	// Task behavior deterministically based on ID
	var taskCompletionCount atomic.Int32
	var taskFailureCount atomic.Int32
	
	// Handler that ensures resource contention
	provider.handleFunc = func(task ITask, server string) error {
		id := task.GetID()
		taskNum := 0
		fmt.Sscanf(id, "task%d", &taskNum)
		
		// Create predictable patterns 
		switch {
		case taskNum%10 == 5:
			// These tasks will fail with error
			return errors.New("simulated processing error")
			
		default:
			// All other tasks succeed quickly
			time.Sleep(10 * time.Millisecond)
			taskCompletionCount.Add(1)
			return nil
		}
	}
	
	// Initialize task manager using InitTaskQueueManager (the production method)
	InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)
	
	// Add a large number of tasks in quick succession to create contention
	const totalTasks = 200
	completedSignals := make([]chan struct{}, totalTasks)
	
	for i := 0; i < totalTasks; i++ {
		doneCh := make(chan struct{})
		completedSignals[i] = doneCh
		
		task := &MockTask{
			id:         fmt.Sprintf("task%d", i),
			priority:   i % 5, // Mix of priorities
			maxRetries: 1,     // Limited retries to ensure we see issues
			provider:   provider,
			timeout:    time.Duration(0), // Use default from getTimeout
			done:       doneCh,
		}
		
		// We can't modify OnComplete directly, so we'll check after completion
		go func(t *MockTask) {
			<-t.done
			if t.failed {
				taskFailureCount.Add(1)
			}
		}(task)
		
		AddTask(task, &logger)
	}
	
	// Wait a limited time for tasks to complete
	timeout := time.After(3 * time.Second)
	
	// Track how many tasks have completed
	var completedOrFailed int32
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	lastCompleted := int32(0)
	stuckTime := time.Time{}
	
	for {
		select {
		case <-timeout:
			// Test timed out, check results
			goto CheckResults
			
		case <-ticker.C:
			// Check how many tasks have completed or failed
			currentTotal := taskCompletionCount.Load() + taskFailureCount.Load()
			completedOrFailed = currentTotal
			
			// If no progress for 0.5 seconds, tasks are likely stuck
			if currentTotal == lastCompleted && lastCompleted > 0 && lastCompleted < totalTasks {
				if stuckTime.IsZero() {
					stuckTime = time.Now()
				} else if time.Since(stuckTime) > 500*time.Millisecond {
					// No progress for 0.5 seconds, likely stuck
					goto CheckResults
				}
			} else {
				// Progress being made, reset stuck timer
				stuckTime = time.Time{}
				lastCompleted = currentTotal
			}
			
			// All tasks completed or failed?
			if completedOrFailed >= totalTasks {
				goto CheckResults
			}
		}
	}
	
CheckResults:
	// Log final stats
	t.Logf("TASK STATS: Completed: %d, Failed: %d, Total: %d", 
		taskCompletionCount.Load(), taskFailureCount.Load(), totalTasks)
	
	// Detect stuck tasks
	stuckTasks := totalTasks - completedOrFailed
	t.Logf("Stuck tasks: %d", stuckTasks)
	
	// Shutdown TaskManager
	TaskQueueManagerInstance.Shutdown()
	
	// The test should fail if tasks are stuck
	if stuckTasks > 0 {
		t.Errorf("Found %d tasks stuck (not completed or failed)", stuckTasks)
	}
}