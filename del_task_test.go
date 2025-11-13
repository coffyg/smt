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

	// Create interrupt function to verify it's called for queued tasks with empty server
	var interruptCalled bool
	var taskReceived ITask
	var serverReceived string
	interruptFn := func(task ITask, server string) error {
		interruptCalled = true
		taskReceived = task
		serverReceived = server
		return nil
	}

	// Delete the queued task
	result := tm.DelTask(task.GetID(), interruptFn)

	// Verify result
	if result != DelTaskRemovedFromQueue {
		t.Errorf("Expected DelTaskRemovedFromQueue, got %s", result)
	}

	// Verify task is no longer in queue
	if tm.isTaskInQueue(task.GetID()) {
		t.Error("Task should have been removed from queue")
	}

	// Verify interrupt function was called for queued task with empty server
	if !interruptCalled {
		t.Error("Interrupt function should be called for queued tasks")
	}

	if serverReceived != "" {
		t.Errorf("Expected empty server for queued task, got '%s'", serverReceived)
	}

	if taskReceived == nil {
		t.Error("Task should have been passed to interrupt function")
	} else if taskReceived.GetID() != task.GetID() {
		t.Errorf("Expected task ID '%s', got '%s'", task.GetID(), taskReceived.GetID())
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
	
	// Synchronization to ensure Handle() has actually been called
	handleStarted := make(chan string, 1) // Will receive the server name when Handle starts
	handleContinue := make(chan struct{})  // Will signal Handle to continue
	
	// Create test provider that blocks execution and signals when Handle starts
	provider := &MockProvider{
		name: "test",
		handleFunc: func(task ITask, server string) error {
			// Signal that Handle has started with the actual server
			handleStarted <- server
			// Block until test tells us to continue
			<-handleContinue
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

	// Add task
	if !tm.AddTask(task) {
		t.Fatal("Failed to add task")
	}

	// Wait for Handle() to actually start and capture the server name
	var actualServer string
	select {
	case actualServer = <-handleStarted:
		// Good! Handle has started executing
	case <-time.After(5 * time.Second):
		t.Fatal("Handle never started within timeout")
	}

	// Now we KNOW Handle() is executing on actualServer
	// Verify task is in running state
	tm.runningTasksMu.RLock()
	runningInfo, isRunning := tm.runningTasks[task.GetID()]
	tm.runningTasksMu.RUnlock()
	
	if !isRunning {
		t.Fatal("Task should be running")
	}
	
	if runningInfo.server != actualServer {
		t.Fatalf("Expected running task server '%s', got '%s'", actualServer, runningInfo.server)
	}

	// Setup interrupt function
	var interruptCalled bool
	var taskReceived ITask
	var serverReceived string
	var wg sync.WaitGroup
	wg.Add(1)
	
	interruptFn := func(task ITask, server string) error {
		defer wg.Done()
		interruptCalled = true
		taskReceived = task
		serverReceived = server
		return nil
	}

	// Delete the running task (Handle is definitely executing)
	result := tm.DelTask(task.GetID(), interruptFn)

	// Verify result
	if result != DelTaskInterruptedRunning {
		t.Errorf("Expected DelTaskInterruptedRunning, got %s", result)
	}

	// Wait for interrupt function to be called
	wg.Wait()

	// Verify interrupt function was called with correct task and server
	if !interruptCalled {
		t.Error("Interrupt function should have been called")
	}
	
	// This is the critical test - server must match the actual server from Handle
	if serverReceived != actualServer {
		t.Errorf("Expected server '%s' (from Handle), got '%s'", actualServer, serverReceived)
	}
	
	if serverReceived != "test-server-123" {
		t.Errorf("Expected specific server 'test-server-123', got '%s'", serverReceived)
	}
	
	if taskReceived == nil {
		t.Error("Task should have been passed to interrupt function")
	} else if taskReceived.GetID() != task.GetID() {
		t.Errorf("Expected task ID '%s', got '%s'", task.GetID(), taskReceived.GetID())
	}

	// Let Handle finish
	close(handleContinue)
	
	// Wait a bit for cleanup
	time.Sleep(50 * time.Millisecond)
}

// TestDelTaskLongRunning tests interrupting a definitely running long task
func TestDelTaskLongRunning(t *testing.T) {
	logger := zerolog.Nop()
	
	// Use channels to coordinate the test precisely
	handleStarted := make(chan string, 1)  // Handle sends server name when it starts
	interruptReceived := make(chan struct{}, 1) // Test signals when interrupt is done
	handleShouldExit := make(chan struct{})  // Signals Handle to exit
	
	provider := &MockProvider{
		name: "longtest",
		handleFunc: func(task ITask, server string) error {
			// Immediately signal we've started with the server name
			handleStarted <- server
			
			// Simulate long-running work that can be interrupted
			for {
				select {
				case <-handleShouldExit:
					return nil
				case <-time.After(10 * time.Millisecond):
					// Continue working...
				}
			}
		},
	}
	
	providers := []IProvider{provider}
	servers := map[string][]string{"longtest": {"long-server-456"}}
	
	tm := NewTaskManagerSimple(&providers, servers, &logger, func(string, string) time.Duration {
		return 30 * time.Second
	})
	tm.Start()
	defer tm.Shutdown()

	task := &MockTask{
		id:       uuid.New().String(),
		provider: provider,
		priority: 1,
		createdAt: time.Now(),
	}

	// Add the long-running task
	if !tm.AddTask(task) {
		t.Fatal("Failed to add long task")
	}

	// Wait for Handle to start and get the actual server
	var runningServer string
	select {
	case runningServer = <-handleStarted:
		t.Logf("Handle started on server: %s", runningServer)
	case <-time.After(3 * time.Second):
		t.Fatal("Long task Handle never started")
	}

	// At this point we're 100% sure Handle() is executing
	// Let it run a bit more to be absolutely certain
	time.Sleep(50 * time.Millisecond)

	// Set up interrupt function that verifies the server
	var interruptTask ITask
	var interruptServer string
	interruptFn := func(task ITask, server string) error {
		interruptTask = task
		interruptServer = server
		interruptReceived <- struct{}{}
		return nil
	}

	// Interrupt the definitely-running task
	result := tm.DelTask(task.GetID(), interruptFn)
	
	// Wait for interrupt to be processed
	select {
	case <-interruptReceived:
		// Good!
	case <-time.After(2 * time.Second):
		t.Fatal("Interrupt function never called")
	}

	// Verify interrupt was called correctly
	if result != DelTaskInterruptedRunning {
		t.Errorf("Expected DelTaskInterruptedRunning, got %s", result)
	}

	if interruptServer != runningServer {
		t.Errorf("Interrupt server '%s' != Handle server '%s'", interruptServer, runningServer)
	}

	if interruptServer != "long-server-456" {
		t.Errorf("Expected 'long-server-456', got '%s'", interruptServer)
	}

	if interruptTask == nil || interruptTask.GetID() != task.GetID() {
		t.Error("Interrupt function got wrong task")
	}

	// Clean shutdown
	close(handleShouldExit)
	time.Sleep(20 * time.Millisecond)
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
	interruptFn := func(task ITask, server string) error {
		t.Error("Interrupt function should not be called for non-existent task")
		return nil
	}

	result := tm.DelTask("non-existent-id", interruptFn)

	if result != DelTaskNotFound {
		t.Errorf("Expected DelTaskNotFound, got %s", result)
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
	tm := GetTaskQueueManagerInstance()
	defer tm.Shutdown()

	// Create and add task
	task := &MockTask{
		id:       uuid.New().String(),
		provider: provider,
		priority: 1,
		createdAt: time.Now(),
	}

	AddTask(task, &logger)

	// Delete using global function
	var interruptCalled bool
	var taskReceived ITask
	var serverReceived string
	interruptFn := func(task ITask, server string) error {
		interruptCalled = true
		taskReceived = task
		serverReceived = server
		return nil
	}

	result := DelTask(task.GetID(), interruptFn, &logger)

	if result != DelTaskRemovedFromQueue {
		t.Errorf("Expected DelTaskRemovedFromQueue, got %s", result)
	}

	// Verify interrupt function was called for queued task with empty server
	if !interruptCalled {
		t.Error("Interrupt function should be called for queued tasks")
	}

	if serverReceived != "" {
		t.Errorf("Expected empty server for queued task, got '%s'", serverReceived)
	}

	if taskReceived == nil {
		t.Error("Task should have been passed to interrupt function")
	} else if taskReceived.GetID() != task.GetID() {
		t.Errorf("Expected task ID '%s', got '%s'", task.GetID(), taskReceived.GetID())
	}
}

