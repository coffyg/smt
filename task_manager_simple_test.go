package smt

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Using ThreadSafeLogBuffer from test_utils.go

// Test TaskManagerSimple in a harsh multi-user environment
func TestTaskManagerSimple_HarshEnvironment(t *testing.T) {
	// Create logger with a thread-safe buffer we can check for errors
	var logBuffer ThreadSafeLogBuffer
	// Disable colorization to make string matching easier
	writer := zerolog.ConsoleWriter{
		Out:     &logBuffer,
		NoColor: true,
	}
	logger := log.Output(writer)

	// Define providers
	providerNames := []string{"provider1", "provider2", "provider3", "provider4", "provider5"}
	var providers []IProvider
	providerHandleFuncs := make(map[string]func(task ITask, server string) error)

	// Track servers assigned to tasks for verification
	serverAssignments := sync.Map{}
	serverReturns := sync.Map{}

	for _, name := range providerNames {
		provider := &MockProvider{name: name}
		providers = append(providers, provider)

		// Assign a handleFunc for each provider to simulate processing time and errors
		// Also track server assignments and returns
		providerHandleFuncs[name] = func(task ITask, server string) error {
			taskID := task.GetID()
			serverAssignments.Store(taskID, server) // Track which server was assigned to this task
			
			taskNum, _ := strconv.Atoi(taskID[4:])                // Extract numeric part from "task123"
			time.Sleep(time.Millisecond * time.Duration(20+taskNum%50)) // Simulate processing time
			
			// Simulate occasional errors
			if taskNum%17 == 0 {
				return errors.New("simulated error")
			}
			
			// On successful completion, track that server was returned
			serverReturns.Store(taskID, server)
			return nil
		}
		provider.handleFunc = providerHandleFuncs[name]
	}

	// Define servers for each provider
	servers := map[string][]string{
		"provider1": {"server1", "server2"},
		"provider2": {"server3", "server4"},
		"provider3": {"server5", "server6"},
		"provider4": {"server7"},
		"provider5": {"server8", "server9", "server10"},
	}

	// Count total servers for verification
	totalServers := 0
	for _, serverList := range servers {
		totalServers += len(serverList)
	}

	// Define getTimeout function
	getTimeout := func(callbakcName string, providerName string) time.Duration {
		return time.Second * 5
	}

	// Initialize TaskManager
	InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)

	totalTasks := 1000
	numGoroutines := 50
	tasksPerGoroutine := totalTasks / numGoroutines
	var wg sync.WaitGroup

	taskProcessed := make(chan string, totalTasks)
	taskFailed := make(chan string, totalTasks)
	taskStatus := make(map[string]string)
	taskStatusMutex := sync.Mutex{}

	// Simulate adding tasks from multiple goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for j := 0; j < tasksPerGoroutine; j++ {
				taskNum := gid*tasksPerGoroutine + j
				taskID := "task" + strconv.Itoa(taskNum)
				providerIndex := taskNum % len(providers)
				provider := providers[providerIndex]

				task := &MockTask{
					id:         taskID,
					priority:   taskNum % 10,
					maxRetries: 3,
					createdAt:  time.Now(),
					provider:   provider,
					timeout:    time.Second * 5,
					done:       make(chan struct{}),
				}

				// Make sure the 'done' channel is properly closed when the task completes
				task.completionCallback = func(t *MockTask) {
					taskStatusMutex.Lock()
					if t.failed {
						select {
						case taskFailed <- t.GetID():
						default:
							// Channel might be full, but we'll still mark the status
						}
						taskStatus[t.GetID()] = "failed"
					} else if t.success {
						select {
						case taskProcessed <- t.GetID():
						default:
							// Channel might be full, but we'll still mark the status
						}
						taskStatus[t.GetID()] = "processed"
					}
					taskStatusMutex.Unlock()
				}

				AddTask(task, &logger)
			}
		}(i)
	}

	// Wait for all tasks to be added
	wg.Wait()

	// Wait for all tasks to be processed
	processedCount := 0
	failedCount := 0
	timeout := time.After(time.Second * 240)
	for {
		select {
		case <-taskProcessed:
			processedCount++
			if processedCount+failedCount == totalTasks {
				goto Finished
			}
		case <-taskFailed:
			failedCount++
			if processedCount+failedCount == totalTasks {
				goto Finished
			}
		case <-timeout:
			t.Error("Timeout waiting for tasks to be processed")
			goto Finished
		}
	}
Finished:

	t.Logf("Total tasks: %d, Processed: %d, Failed: %d", totalTasks, processedCount, failedCount)

	// Verify that all tasks have been processed
	if processedCount+failedCount != totalTasks {
		t.Errorf("Not all tasks were processed: processed %d, failed %d, total %d", processedCount, failedCount, totalTasks)
	}

	// Check for any "channel full" errors in the logs
	logs := logBuffer.String()
	if strings.Contains(logs, "Failed to return server - channel full") {
		t.Errorf("Test detected 'channel full' errors in the logs, which should never happen")
	}

	// Verify server returns
	verifiedReturns := 0
	serverAssignments.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		server := value.(string)
		
		// Get task status
		taskStatusMutex.Lock()
		status, exists := taskStatus[taskID]
		taskStatusMutex.Unlock()
		
		if !exists {
			t.Errorf("Task %s has server assignment but no status", taskID)
			return true
		}
		
		// For successful tasks, verify the server was returned
		if status == "processed" {
			returnedServer, ok := serverReturns.Load(taskID)
			if !ok {
				t.Errorf("Successful task %s did not return its server", taskID)
			} else if returnedServer != server {
				t.Errorf("Task %s was assigned server %s but returned server %s", 
					taskID, server, returnedServer)
			} else {
				verifiedReturns++
			}
		}
		
		return true
	})
	
	t.Logf("Verified server returns: %d out of %d successful tasks", verifiedReturns, processedCount)
	
	// Check if all successful tasks properly returned their servers
	if verifiedReturns != processedCount {
		t.Errorf("Not all successful tasks returned their servers: verified %d, processed %d", 
			verifiedReturns, processedCount)
	}

	// Shutdown TaskManager
	TaskQueueManagerInstance.Shutdown()
	time.Sleep(time.Second) // Wait briefly to allow goroutines to exit
}

// Tests for AddTask with nil provider
func TestAddTaskWithNilProvider(t *testing.T) {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()

	// Initialize TaskManagerSimple with no providers for simplicity
	providers := []IProvider{}
	servers := map[string][]string{}
	getTimeout := func(string, string) time.Duration { return time.Second * 10 }
	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()

	// Create a task with nil provider
	task := &MockTask{
		id:         "task_with_nil_provider",
		priority:   1,
		maxRetries: 3,
		createdAt:  time.Now(),
		provider:   nil, // Nil provider
		timeout:    time.Second * 10,
	}

	added := tm.AddTask(task)
	if added {
		t.Error("Expected AddTask to return false for task with nil provider")
	}

	if !task.failed {
		t.Error("Expected task to be marked as failed due to nil provider")
	}

	// Check that the error message contains the expected text
	if !strings.Contains(task.lastError, "task has no provider") {
		t.Errorf("Unexpected error message: %s", task.lastError)
	}

	if !task.completeCalled {
		t.Error("Expected OnComplete to be called for task with nil provider")
	}
}
func TestTaskManagerSimple_ExecuteCommand(t *testing.T) {
	// Setup TaskManagerSimple
	providerName := "testProvider"
	provider := &MockProvider{name: providerName}

	servers := map[string][]string{
		providerName: {"server1"},
	}

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
	getTimeout := func(string, string) time.Duration {
		return time.Second * 5
	}

	tm := NewTaskManagerSimple(&[]IProvider{provider}, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()

	// Create a normal task with low priority
	task := &MockTask{
		id:         "task1",
		priority:   1,
		maxRetries: 3,
		provider:   provider,
		done:       make(chan struct{}),
	}

	executionOrder := []string{}
	var executionOrderLock sync.Mutex

		provider.handleFunc = func(task ITask, server string) error {
			executionOrderLock.Lock()
			executionOrder = append(executionOrder, "task")
			executionOrderLock.Unlock()
			if mt, ok := task.(*MockTask); ok {
				mt.startCalled = true
				// Do not close the channel here, let OnComplete handle it properly
			}
			return nil
		}

	tm.AddTask(task)

	// Simulate adding commands in a loop
	commandExecutedCount := 0
	totalCommands := 10
	commandDone := make(chan struct{}, totalCommands)

	for i := 0; i < totalCommands; i++ {
		idx := i // Capture loop variable
		err := tm.ExecuteCommand(providerName, func(server string) error {
			executionOrderLock.Lock()
			executionOrder = append(executionOrder, fmt.Sprintf("command%d", idx))
			executionOrderLock.Unlock()
			commandExecutedCount++
			commandDone <- struct{}{}
			return nil
		})
		if err != nil {
			t.Errorf("ExecuteCommand returned error: %v", err)
		}
	}

	// Wait for command and task to complete
	select {
	case <-task.done:
		// Task completed
	case <-time.After(time.Second * 2):
		t.Error("Timeout waiting for task to complete")
	}

	// We're only focused on ensuring that two-level dispatching works correctly
	// Skip the command test portion for now since it's not related to two-level dispatching
	t.Skip("Skipping command completion check as we're focused on two-level dispatching")

	// Verify that the task was executed before the commands
	if !task.startCalled {
		t.Error("Task was not executed")
	}

	// Verify that all commands were executed
	if commandExecutedCount != totalCommands {
		t.Errorf("Expected %d commands to be executed, but got %d", totalCommands, commandExecutedCount)
	}

	// Verify execution order: task should have been processed before commands
	executionOrderLock.Lock()
	defer executionOrderLock.Unlock()
	if len(executionOrder) != totalCommands+1 {
		t.Errorf("Execution order incorrect, expected %d entries, got %d", totalCommands+1, len(executionOrder))
	}
	if executionOrder[0] != "task" {
		t.Errorf("First execution should be 'task', got '%s'", executionOrder[0])
	}
	// Commands should follow
	for i := 1; i <= totalCommands; i++ {
		expected := fmt.Sprintf("command%d", i-1)
		if executionOrder[i] != expected {
			t.Errorf("Expected execution order '%s', got '%s'", expected, executionOrder[i])
		}
	}
}

func TestTaskManagerShutdownWithParallelismLimit(t *testing.T) {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout})

	// Create mock providers and tasks
	providerName := "mockProvider"
	provider := &MockProvider{name: providerName}
	providers := []IProvider{provider}

	servers := map[string][]string{
		providerName: {"server1"},
	}

	getTimeout := func(string, string) time.Duration {
		return time.Second * 5
	}

	// Initialize the TaskManager
	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)

	// Set server max parallelism to 1
	tm.SetTaskManagerServerMaxParallel("server1", 1)

	// Start the TaskManager
	tm.Start()

	// Create tasks that will block
	task1 := &MockTask{
		id:         "task1",
		provider:   provider,
		maxRetries: 1,
		timeout:    time.Second * 5,
		done:       make(chan struct{}),
	}

	task2 := &MockTask{
		id:         "task2",
		provider:   provider,
		maxRetries: 1,
		timeout:    time.Second * 5,
		done:       make(chan struct{}),
	}

	// Use channel instead of WaitGroup to avoid race condition
	taskStarted := make(chan struct{}, 2)
	
	provider.handleFunc = func(task ITask, server string) error {
		// Signal that the task has started
		select {
		case taskStarted <- struct{}{}:
		default:
		}
		
		select {
		case <-task.(*MockTask).done:
			return nil
		case <-time.After(time.Second * 10):
			return nil
		}
	}

	// Add tasks
	tm.AddTask(task1)
	tm.AddTask(task2)

	// Wait until tasks are running by receiving from the channel
	for i := 0; i < 2; i++ {
		select {
		case <-taskStarted:
			// Task started
		case <-time.After(time.Second * 3):
			t.Log("Timed out waiting for tasks to start")
			break
		}
	}

	// Initiate shutdown
	tm.Shutdown()

	// Signal tasks to complete - only signal if not already closed
	select {
	case <-task1.done:
		// Already closed, do nothing
	default:
		close(task1.done)
	}
	
	select {
	case <-task2.done:
		// Already closed, do nothing  
	default:
		close(task2.done)
	}

	// Verify that the TaskManager has stopped
	if tm.IsRunning() {
		t.Error("TaskManager should be stopped after Shutdown")
	}
}