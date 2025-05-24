package smt

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestConcurrencyRetryNotCounted verifies that concurrency rejections don't count as retries
func TestConcurrencyRetryNotCounted(t *testing.T) {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: zerolog.NewConsoleWriter().Out}).
		Level(zerolog.WarnLevel)

	var executionCount int32
	var rejectionCount int32
	
	provider := &MockProvider{
		name: "test_provider",
		handleFunc: func(task ITask, server string) error {
			atomic.AddInt32(&executionCount, 1)
			// Hold the slot for a while
			time.Sleep(100 * time.Millisecond)
			// Always succeed
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

	InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)
	defer TaskQueueManagerInstance.Shutdown()

	// Set very strict limit - only 1 concurrent task
	TaskQueueManagerInstance.SetTaskManagerServerMaxParallel("server1", 1)

	// Create a task with only 2 retries allowed
	task1 := &MockTask{
		id:         "blocking_task",
		priority:   5,
		maxRetries: 2,
		provider:   provider,
		timeout:    1 * time.Second,
		done:       make(chan struct{}),
	}
	
	// Add first task to block the server
	AddTask(task1, &logger)
	
	// Wait a bit to ensure it's processing
	time.Sleep(10 * time.Millisecond)
	
	// Now add a second task that will be rejected multiple times
	task2 := &MockTask{
		id:         "rejected_task",
		priority:   5,
		maxRetries: 2, // Only 2 retries allowed
		provider:   provider,
		timeout:    1 * time.Second,
		done:       make(chan struct{}),
		retries:    0,
	}
	
	// Track rejections
	go func() {
		for i := 0; i < 20; i++ {
			time.Sleep(10 * time.Millisecond)
			if task2.GetRetries() > 0 {
				atomic.AddInt32(&rejectionCount, 1)
			}
		}
	}()
	
	AddTask(task2, &logger)
	
	// Wait for both tasks to complete
	select {
	case <-task1.done:
		t.Log("Task 1 completed")
	case <-time.After(2 * time.Second):
		t.Error("Task 1 timeout")
	}
	
	select {
	case <-task2.done:
		t.Log("Task 2 completed")
	case <-time.After(2 * time.Second):
		t.Error("Task 2 timeout")
	}
	
	// Verify results
	executions := atomic.LoadInt32(&executionCount)
	rejections := atomic.LoadInt32(&rejectionCount)
	
	t.Logf("Executions: %d", executions)
	t.Logf("Task2 retries after rejections: %d", task2.GetRetries())
	t.Logf("Rejection count: %d", rejections)
	
	// Both tasks should have executed
	if executions != 2 {
		t.Errorf("Expected 2 executions, got %d", executions)
	}
	
	// Task2 should not have incremented retries due to concurrency
	if task2.GetRetries() > 0 {
		t.Errorf("Task2 retries should be 0 (concurrency rejections shouldn't count), got %d", task2.GetRetries())
	}
	
	// Task2 should have succeeded
	if task2.failed {
		t.Error("Task2 should not have failed due to concurrency limits")
	}
}