package smt

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// TestDelTaskQueued tests removing a task from the queue before it starts
func TestDelTaskQueued(t *testing.T) {
	logger := zerolog.Nop()
	
	// Create test provider and server
	provider := &MockProvider{name: "test"}
	providers := []IProvider{provider}
	servers := map[string][]string{"test": {"server1"}}
	
	tm := NewTaskManagerSimple(&providers, servers, &logger, func(string, string) time.Duration {
		return 30 * time.Second
	})
	tm.Start()
	defer tm.Shutdown()

	// Create a task but don't let it execute (no goroutines processing yet)
	task := &MockTask{
		id:       uuid.New().String(),
		provider: provider,
		priority: 1,
		createdAt: time.Now(),
	}

	// Add task to queue
	if !tm.AddTask(task) {
		t.Fatal("Failed to add task")
	}

	// Verify task is in queue
	if !tm.isTaskInQueue(task.GetID()) {
		t.Fatal("Task should be in queue")
	}

	// Create interrupt function to verify it's NOT called for queued tasks
	var interruptCalled bool
	interruptFn := func(server string) {
		interruptCalled = true
	}

	// Delete the queued task
	result := tm.DelTask(task.GetID(), interruptFn)

	// Verify result
	if result != "removed_from_queue" {
		t.Errorf("Expected 'removed_from_queue', got %s", result)
	}

	// Verify task is no longer in queue
	if tm.isTaskInQueue(task.GetID()) {
		t.Error("Task should have been removed from queue")
	}

	// Verify interrupt function was NOT called for queued task
	if interruptCalled {
		t.Error("Interrupt function should not be called for queued tasks")
	}

	// Verify task count decreased
	pd := tm.providers["test"]
	if atomic.LoadInt32(&pd.taskCount) != 0 {
		t.Errorf("Expected task count 0, got %d", atomic.LoadInt32(&pd.taskCount))
	}
}

// TestDelTaskRunning tests interrupting a running task
func TestDelTaskRunning(t *testing.T) {
	logger := zerolog.Nop()
	
	// Create test provider that blocks execution
	provider := &MockProvider{
		name: "test",
		handleFunc: func(task ITask, server string) error {
			// Block for a while to simulate running task
			time.Sleep(200 * time.Millisecond)
			return nil
		},
	}
	providers := []IProvider{provider}
	servers := map[string][]string{"test": {"test-server-123"}}
	
	tm := NewTaskManagerSimple(&providers, servers, &logger, func(string, string) time.Duration {
		return 30 * time.Second
	})
	tm.Start()
	defer tm.Shutdown()

	// Create task
	task := &MockTask{
		id:       uuid.New().String(),
		provider: provider,
		priority: 1,
		createdAt: time.Now(),
	}

	// Add task and wait for it to start running
	if !tm.AddTask(task) {
		t.Fatal("Failed to add task")
	}

	// Wait for task to be picked up and start running
	time.Sleep(100 * time.Millisecond)

	// Verify task is now running (not in queue, but in runningTasks)
	tm.runningTasksMu.RLock()
	_, isRunning := tm.runningTasks[task.GetID()]
	tm.runningTasksMu.RUnlock()
	
	if !isRunning {
		t.Fatal("Task should be running")
	}

	// Setup interrupt function
	var interruptCalled bool
	var serverReceived string
	var wg sync.WaitGroup
	wg.Add(1)
	
	interruptFn := func(server string) {
		defer wg.Done()
		interruptCalled = true
		serverReceived = server
	}

	// Delete the running task
	result := tm.DelTask(task.GetID(), interruptFn)

	// Verify result
	if result != "interrupted_running" {
		t.Errorf("Expected 'interrupted_running', got %s", result)
	}

	// Wait for interrupt function to be called
	wg.Wait()

	// Verify interrupt function was called with correct server
	if !interruptCalled {
		t.Error("Interrupt function should have been called")
	}
	
	if serverReceived != "test-server-123" {
		t.Errorf("Expected server 'test-server-123', got '%s'", serverReceived)
	}

	// Wait a bit for task to finish/cleanup
	time.Sleep(50 * time.Millisecond)
}

// TestDelTaskNotFound tests deleting a non-existent task
func TestDelTaskNotFound(t *testing.T) {
	logger := zerolog.Nop()
	
	provider := &MockProvider{name: "test"}
	providers := []IProvider{provider}
	servers := map[string][]string{"test": {"server1"}}
	
	tm := NewTaskManagerSimple(&providers, servers, &logger, func(string, string) time.Duration {
		return 30 * time.Second
	})
	tm.Start()
	defer tm.Shutdown()

	// Try to delete non-existent task
	interruptFn := func(server string) {
		t.Error("Interrupt function should not be called for non-existent task")
	}

	result := tm.DelTask("non-existent-id", interruptFn)

	if result != "not_found" {
		t.Errorf("Expected 'not_found', got %s", result)
	}
}

// TestDelTaskGlobal tests the global DelTask function
func TestDelTaskGlobal(t *testing.T) {
	logger := zerolog.Nop()
	
	provider := &MockProvider{name: "test"}
	providers := []IProvider{provider}
	servers := map[string][]string{"test": {"server1"}}
	
	// Initialize global task manager
	InitTaskQueueManager(&logger, &providers, []ITask{}, servers, func(string, string) time.Duration {
		return 30 * time.Second
	})
	defer TaskQueueManagerInstance.Shutdown()

	// Create and add task
	task := &MockTask{
		id:       uuid.New().String(),
		provider: provider,
		priority: 1,
		createdAt: time.Now(),
	}

	AddTask(task, &logger)

	// Delete using global function
	interruptFn := func(server string) {
		// This should not be called for queued task
		t.Error("Interrupt function should not be called for queued task")
	}

	result := DelTask(task.GetID(), interruptFn, &logger)

	if result != "removed_from_queue" {
		t.Errorf("Expected 'removed_from_queue', got %s", result)
	}
}

