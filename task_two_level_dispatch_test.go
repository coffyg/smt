package smt

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Test that tasks are properly processed and retried in two-level dispatch system
func TestTwoLevelTaskRetry(t *testing.T) {
	// Setup logger
	logger := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.DebugLevel)

	// Setup mock provider
	providerName := "test-provider"
	provider := &MockProvider{name: providerName}
	
	// Track call count to simulate retries
	var callCount int
	var callMutex sync.Mutex
	
	// Create a handler that fails for the first attempt but succeeds on retry
	provider.handleFunc = func(task ITask, server string) error {
		callMutex.Lock()
		callCount++
		currentCount := callCount
		callMutex.Unlock()
		
		if currentCount == 1 {
			// First call, simulate timeout
			return errors.New("simulated timeout error")
		}
		// Second call, succeed
		return nil
	}
	
	providers := []IProvider{provider}
	servers := map[string][]string{
		providerName: {"server1"},
	}
	
	getTimeout := func(callback, provider string) time.Duration {
		return 50 * time.Millisecond
	}
	
	// Create task manager with two-level dispatch enabled
	tm := NewTaskManagerWithOptions(&providers, servers, &logger, getTimeout, &TaskManagerOptions{
		EnableTwoLevel: true,
		EnablePooling: true,
	})
	
	// Start the task manager
	tm.Start()
	defer tm.Shutdown()
	
	// Task completion channel
	taskDone := make(chan struct{})
	
	// Create a task with retry
	task := &MockTask{
		id:         "test-retry-task",
		provider:   provider,
		maxRetries: 3, // Allow retries
		done:       taskDone,
	}
	
	// Add the task
	if !tm.AddTask(task) {
		t.Fatal("Failed to add task")
	}
	
	// Wait for task completion
	select {
	case <-taskDone:
		// Task should succeed on the retry
	case <-time.After(time.Second * 5):
		t.Fatal("Timeout waiting for task to complete")
	}
	
	// Verify the task succeeded
	if !task.success {
		t.Error("Task should have succeeded on retry")
	}
	
	// Verify the call count
	callMutex.Lock()
	if callCount != 2 {
		t.Errorf("Expected 2 calls (1 failure + 1 retry), got %d", callCount)
	}
	callMutex.Unlock()
	
	// Verify retry count
	if task.GetRetries() != 1 {
		t.Errorf("Expected retry count of 1, got %d", task.GetRetries())
	}
}

// Test max retries behavior in two-level dispatch
func TestTwoLevelMaxRetries(t *testing.T) {
	// Setup logger
	logger := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.DebugLevel)
	
	// Setup mock provider
	providerName := "test-provider"
	provider := &MockProvider{name: providerName}
	
	// Create a handler that always fails
	provider.handleFunc = func(task ITask, server string) error {
		return errors.New("simulated persistent error")
	}
	
	providers := []IProvider{provider}
	servers := map[string][]string{
		providerName: {"server1"},
	}
	
	getTimeout := func(callback, provider string) time.Duration {
		return 50 * time.Millisecond
	}
	
	// Create task manager with two-level dispatch and memory pooling enabled
	tm := NewTaskManagerWithOptions(&providers, servers, &logger, getTimeout, &TaskManagerOptions{
		EnableTwoLevel: true,
		EnablePooling: true,
	})
	
	// Start the task manager
	tm.Start()
	defer tm.Shutdown()
	
	// Task completion channel
	taskDone := make(chan struct{})
	
	// Create a task with limited retries
	task := &MockTask{
		id:         "test-max-retries-task",
		provider:   provider,
		maxRetries: 2, // Allow only 2 retries
		done:       taskDone,
	}
	
	// Add the task
	if !tm.AddTask(task) {
		t.Fatal("Failed to add task")
	}
	
	// Wait for task completion
	select {
	case <-taskDone:
		// Task should fail after max retries
	case <-time.After(time.Second * 5):
		t.Fatal("Timeout waiting for task to complete")
	}
	
	// Verify the task failed
	if !task.failed {
		t.Error("Task should have failed after max retries")
	}
	
	// Verify retry count
	if task.GetRetries() != 2 {
		t.Errorf("Expected retry count of 2, got %d", task.GetRetries())
	}
}

// Test comprehensive setup with TaskManagerSimple constructor
func TestTwoLevelWithTaskManagerSimple(t *testing.T) {
	// Setup logger
	logger := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.DebugLevel)

	// Setup mock provider
	providerName := "test-provider"
	provider := &MockProvider{name: providerName}
	
	// Track call count to simulate retries
	var callCount int
	var callMutex sync.Mutex
	
	// Create a handler that fails for the first attempt but succeeds on retry
	provider.handleFunc = func(task ITask, server string) error {
		callMutex.Lock()
		callCount++
		currentCount := callCount
		callMutex.Unlock()
		
		if currentCount == 1 {
			// First call, simulate timeout
			return errors.New("simulated timeout error")
		}
		// Second call, succeed
		return nil
	}
	
	providers := []IProvider{provider}
	servers := map[string][]string{
		providerName: {"server1"},
	}
	
	getTimeout := func(callback, provider string) time.Duration {
		return 50 * time.Millisecond
	}
	
	// Create task manager with the standard constructor (should enable all optimizations)
	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	
	// Start the task manager
	tm.Start()
	defer tm.Shutdown()
	
	// Verify that two-level dispatch is enabled
	if !tm.enableTwoLevel || tm.twoLevelDispatch == nil {
		t.Error("Two-level dispatch should be enabled by default")
	}
	
	// Task completion channel
	taskDone := make(chan struct{})
	
	// Create a task with retry
	task := &MockTask{
		id:         "test-retry-task-simple",
		provider:   provider,
		maxRetries: 3, // Allow retries
		done:       taskDone,
	}
	
	// Add the task
	if !tm.AddTask(task) {
		t.Fatal("Failed to add task")
	}
	
	// Wait for task completion
	select {
	case <-taskDone:
		// Task should succeed on the retry
	case <-time.After(time.Second * 5):
		t.Fatal("Timeout waiting for task to complete")
	}
	
	// Verify the task succeeded
	if !task.success {
		t.Error("Task should have succeeded on retry")
	}
	
	// Verify retry count
	if task.GetRetries() != 1 {
		t.Errorf("Expected retry count of 1, got %d", task.GetRetries())
	}
}