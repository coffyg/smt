package smt

import (
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestTaskCompactionBasic tests the basic functionality of task compaction
func TestTaskCompactionBasic(t *testing.T) {
	// Create test logger
	logger := zerolog.Nop()
	
	// Create mock provider
	provider := &MockProvider{name: "test-provider"}
	providers := []IProvider{provider}
	servers := map[string][]string{
		"test-provider": {"server1", "server2"},
	}
	
	// Create a channel to artificially block the processing of tasks
	waitCh := make(chan struct{})
	
	// Override the Handle method to block indefinitely
	provider.handleFunc = func(task ITask, server string) error {
		// Block until test is done
		<-waitCh
		return nil
	}
	
	// Create task manager with compaction DISABLED (to avoid race conditions in test)
	tm := NewTaskManagerWithOptions(&providers, servers, &logger, func(string, string) time.Duration {
		return 5 * time.Second // Long timeout
	}, &TaskManagerOptions{
		EnableCompaction:    false, // Disable automatic compaction for test
		CompactionThreshold: 10,    // Low threshold for testing
	})
	
	// Start the task manager
	tm.Start()
	defer func() {
		// Unblock tasks before shutdown
		close(waitCh)
		tm.Shutdown()
	}()
	
	// Add a bunch of tasks that will remain in queue since provider is blocked
	taskCount := 50
	for i := 0; i < taskCount; i++ {
		taskID := fmt.Sprintf("task-%d", i)
		task := &MockTask{
			id:          taskID,
			createdAt:   time.Now(),
			maxRetries:  3,
			priority:    i % 5,
			provider:    provider,
			timeout:     10 * time.Second, // Long timeout
		}
		
		tm.AddTask(task)
	}
	
	// Wait a bit to ensure tasks are added to queue
	time.Sleep(100 * time.Millisecond)
	
	// Get provider data
	pd := tm.providers["test-provider"]
	
	// Safely check initial size with lock
	pd.taskQueueLock.Lock()
	initialSize := len(pd.taskQueue)
	pd.taskQueueLock.Unlock()
	
	t.Logf("Added %d tasks, queue size: %d", taskCount, initialSize)
	
	if initialSize < 20 {
		t.Fatalf("Expected at least 20 tasks in queue, got %d", initialSize)
	}
	
	// Mark many tasks as completed (by removing them from the taskInQueue map)
	markCount := 25
	for i := 0; i < markCount; i++ {
		taskID := fmt.Sprintf("task-%d", i)
		tm.delTaskInQueue(taskID)
	}
	
	t.Logf("Marked %d tasks as completed", markCount)
	
	// Wait to ensure task queue map is updated
	time.Sleep(100 * time.Millisecond)
	
	// Run compaction manually - this handles its own locking
	compacted := tm.compactProviderQueue("test-provider", pd)
	
	// Safely check final size with lock
	pd.taskQueueLock.Lock()
	finalSize := len(pd.taskQueue)
	pd.taskQueueLock.Unlock()
	
	// Verify some tasks were compacted
	t.Logf("Initial size: %d, Compacted: %d, Final size: %d", initialSize, compacted, finalSize)
	
	if compacted == 0 {
		t.Error("Expected some tasks to be compacted, but none were")
	}
	
	if finalSize >= initialSize {
		t.Errorf("Expected queue size to decrease from %d, but it's now %d", initialSize, finalSize)
	}
}

// TestCompactionUnderLoad tests the compaction worker under simulated load
func TestCompactionUnderLoad(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}
	
	// Create test logger
	logger := zerolog.Nop()
	
	// Create mock provider
	provider := &MockProvider{name: "test-provider"}
	providers := []IProvider{provider}
	servers := map[string][]string{
		"test-provider": {"server1", "server2"},
	}
	
	// Create a channel to artificially block the processing of tasks
	waitCh := make(chan struct{})
	
	// Override the Handle method to block indefinitely
	provider.handleFunc = func(task ITask, server string) error {
		// Block until test is done
		<-waitCh
		return nil
	}
	
	// Create task manager with compaction DISABLED to avoid race conditions
	tm := NewTaskManagerWithOptions(&providers, servers, &logger, func(string, string) time.Duration {
		return 5 * time.Second // Long timeout
	}, &TaskManagerOptions{
		EnableCompaction:    false, // Disable automatic compaction
		CompactionThreshold: 20,    // Low threshold for testing
	})
	
	// Start the task manager
	tm.Start()
	defer func() {
		// Unblock tasks before shutdown
		close(waitCh)
		tm.Shutdown()
	}()
	
	// Add tasks in batches to simulate load
	totalTasks := 200
	for i := 0; i < totalTasks; i++ {
		taskID := fmt.Sprintf("task-%d", i)
		task := &MockTask{
			id:         taskID,
			createdAt:  time.Now(),
			maxRetries: 3,
			priority:   i % 5,
			provider:   provider,
			timeout:    10 * time.Second, // Long timeout
		}
		
		tm.AddTask(task)
		
		// Mark 80% of tasks as completed quickly
		if i > 0 && i%5 != 0 {
			tm.delTaskInQueue(taskID)
		}
		
		// Small delay to simulate natural processing
		if i%10 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	
	// Wait for tasks to be added to queue
	time.Sleep(200 * time.Millisecond)
	
	// Get provider data
	pd := tm.providers["test-provider"]
	
	// Safely check queue size with lock before compaction
	pd.taskQueueLock.Lock()
	beforeSize := len(pd.taskQueue)
	pd.taskQueueLock.Unlock()
	
	t.Logf("Added %d tasks, queue size before compaction: %d", totalTasks, beforeSize)
	
	if beforeSize < 50 {
		t.Fatalf("Expected at least 50 tasks in queue, got %d", beforeSize)
	}
	
	// Run manual compaction
	compacted := tm.compactProviderQueue("test-provider", pd)
	
	// Safely check queue size with lock after compaction
	pd.taskQueueLock.Lock()
	finalSize := len(pd.taskQueue)
	pd.taskQueueLock.Unlock()
	
	// We expect about 20% of tasks to remain (the ones we didn't mark as completed)
	expectedMax := totalTasks / 5 + totalTasks / 5 / 5 // Add 20% margin
	
	t.Logf("Before size: %d, Compacted: %d, Final size: %d (expected < %d)", 
		beforeSize, compacted, finalSize, expectedMax)
	
	if compacted == 0 {
		t.Error("Expected some tasks to be compacted, but none were")
	}
	
	if finalSize > expectedMax {
		t.Errorf("Expected queue size to be less than %d after compaction, but it's %d", expectedMax, finalSize)
	}
}