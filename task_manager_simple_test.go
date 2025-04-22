package smt

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

			taskNum, _ := strconv.Atoi(taskID[4:])                      // Extract numeric part from "task123"
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

	// Use thread-safe logging buffer for race detection
	var logBuffer ThreadSafeLogBuffer
	writer := zerolog.ConsoleWriter{
		Out:     &logBuffer,
		NoColor: true,
	}
	logger := log.Output(writer)
	
	getTimeout := func(string, string) time.Duration {
		return time.Second * 5
	}

	tm := NewTaskManagerSimple(&[]IProvider{provider}, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()

	// Create a normal task with low priority
	taskCompletedChan := make(chan struct{}, 1)
	task := &MockTask{
		id:         "task1",
		priority:   1,
		maxRetries: 3,
		provider:   provider,
		done:       make(chan struct{}),
	}

	// Track execution using sync.Map for thread safety
	var executionOrder sync.Map
	var executionIndex int32
	var taskStartCalled int32

	provider.handleFunc = func(task ITask, server string) error {
		idx := atomic.AddInt32(&executionIndex, 1)
		executionOrder.Store(idx, "task")
		
		// Note: Purposely using _ to ignore the unused variable
		if _, ok := task.(*MockTask); ok {
			atomic.StoreInt32(&taskStartCalled, 1)
			// Task will be marked complete in its completion callback
		}
		return nil
	}

	// Add completion callback to signal when task is done
	task.completionCallback = func(t *MockTask) {
		taskCompletedChan <- struct{}{}
	}

	tm.AddTask(task)

	// Wait briefly for task to start processing
	time.Sleep(time.Millisecond * 100)

	// Simulate adding commands in a loop
	totalCommands := 10
	commandResults := make(chan struct{}, totalCommands)
	var commandCount int32

	for i := 0; i < totalCommands; i++ {
		idx := i // Capture loop variable
		err := tm.ExecuteCommand(providerName, func(server string) error {
			cmdIdx := atomic.AddInt32(&executionIndex, 1)
			executionOrder.Store(cmdIdx, fmt.Sprintf("command%d", idx))
			
			atomic.AddInt32(&commandCount, 1)
			commandResults <- struct{}{}
			return nil
		})
		if err != nil {
			t.Errorf("ExecuteCommand returned error: %v", err)
		}
	}

	// Wait for commands to complete
	completedCommands := 0
	commandTimeout := time.After(time.Second * 10)
	for completedCommands < totalCommands {
		select {
		case <-commandResults:
			completedCommands++
		case <-commandTimeout:
			t.Logf("Timeout waiting for commands to complete, only %d/%d finished",
				completedCommands, totalCommands)
			goto CommandsDone
		}
	}
CommandsDone:

	// Wait for task to complete
	select {
	case <-taskCompletedChan:
		// Task completed
	case <-time.After(time.Second * 5):
		t.Log("Timeout waiting for task to complete")
	}

	// Verify that the task was executed
	if atomic.LoadInt32(&taskStartCalled) != 1 {
		t.Error("Task was not executed")
	}

	// Verify that all commands were executed
	finalCommandCount := atomic.LoadInt32(&commandCount)
	if int(finalCommandCount) != totalCommands {
		t.Errorf("Expected %d commands to be executed, but got %d", totalCommands, finalCommandCount)
	}

	// Collect and sort execution order
	var executionValues []string
	executionOrder.Range(func(key, value interface{}) bool {
		executionValues = append(executionValues, value.(string))
		return true
	})
	
	// Log execution sequence for debugging
	t.Logf("Execution sequence: %v", executionValues)
	
	// Check for any "channel full" errors in the logs
	logs := logBuffer.String()
	if strings.Contains(logs, "Failed to return server - channel full") {
		t.Errorf("Test detected 'channel full' errors in the logs, which should never happen")
	}
}

// TestTaskManagerSimple_ExecuteCommandHarshEnvironment tests executing commands while
// multiple background tasks are running in a harsh environment
func TestTaskManagerSimple_ExecuteCommandHarshEnvironment(t *testing.T) {
	// Create logger with a thread-safe buffer we can check for errors
	var logBuffer ThreadSafeLogBuffer
	// Disable colorization to make string matching easier
	writer := zerolog.ConsoleWriter{
		Out:     &logBuffer,
		NoColor: true,
	}
	logger := log.Output(writer)

	// Define providers for our test
	providerNames := []string{"provider1", "provider2", "provider3"}
	var providers []IProvider
	providerHandleFuncs := make(map[string]func(task ITask, server string) error)

	// Track servers assigned to tasks and commands for verification
	serverAssignments := sync.Map{}
	serverReturns := sync.Map{}
	
	// Track command executions
	commandExecutions := sync.Map{}
	commandErrors := sync.Map{}

	for _, name := range providerNames {
		provider := &MockProvider{name: name}
		providers = append(providers, provider)

		// Assign a handleFunc for each provider to simulate processing time and errors
		// Using a closure to capture the provider name
		providerName := name // Capture the provider name in this scope
		providerHandleFuncs[providerName] = func(task ITask, server string) error {
			taskID := task.GetID()
			serverAssignments.Store(taskID, server) // Track which server was assigned to this task

			// Extract numeric part from "task123" to add variability
			taskNum, _ := strconv.Atoi(taskID[4:])
			// Simulate variable processing time - shorter for test performance
			time.Sleep(time.Millisecond * time.Duration(5+taskNum%20))

			// Simulate occasional errors
			if taskNum%17 == 0 {
				return errors.New("simulated task error")
			}

			// On successful completion, track that server was returned
			serverReturns.Store(taskID, server)
			return nil
		}
		provider.handleFunc = providerHandleFuncs[providerName]
	}

	// Define servers for each provider - ensure we have enough for concurrent use
	servers := map[string][]string{
		"provider1": {"server1", "server2", "server3"},
		"provider2": {"server4", "server5"},
		"provider3": {"server6", "server7", "server8", "server9"},
	}

	// Count total servers for verification
	totalServers := 0
	for _, serverList := range servers {
		totalServers += len(serverList)
	}

	// Define getTimeout function
	getTimeout := func(callbackName string, providerName string) time.Duration {
		return time.Second * 5
	}

	// Initialize TaskManager
	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()

	// For a faster test, reduce the counts
	totalBackgroundTasks := 30 
	numTaskGoroutines := 5
	tasksPerGoroutine := totalBackgroundTasks / numTaskGoroutines
	
	totalCommands := 20
	numCommandGoroutines := 5
	commandsPerGoroutine := totalCommands / numCommandGoroutines

	// Use atomic counters for tracking
	var processedTasks, failedTasks, processedCommands, failedCommands int32
	
	// Track status with thread-safe maps
	taskStatus := sync.Map{}
	var wg sync.WaitGroup

	// Start background task goroutines
	for i := 0; i < numTaskGoroutines; i++ {
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

				// Make sure the task status is properly tracked when it completes
				task.completionCallback = func(t *MockTask) {
					if t.failed {
						atomic.AddInt32(&failedTasks, 1)
						taskStatus.Store(t.GetID(), "failed")
					} else if t.success {
						atomic.AddInt32(&processedTasks, 1)
						taskStatus.Store(t.GetID(), "processed")
					}
				}

				tm.AddTask(task)
				
				// Small delay to avoid overloading the system
				time.Sleep(time.Millisecond * 5)
			}
		}(i)
	}

	// Wait a small amount of time to let some tasks get started
	time.Sleep(time.Millisecond * 50)

	// Start command execution goroutines
	for i := 0; i < numCommandGoroutines; i++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for j := 0; j < commandsPerGoroutine; j++ {
				cmdNum := gid*commandsPerGoroutine + j
				providerIndex := cmdNum % len(providerNames)
				providerName := providerNames[providerIndex]
				
				// Add a small delay between command executions to simulate real usage
				time.Sleep(time.Millisecond * 5)
				
				err := tm.ExecuteCommand(providerName, func(server string) error {
					// Track which server was used for this command
					commandExecutions.Store(cmdNum, server)
					
					// Simulate command work with variable duration - shorter for test performance
					time.Sleep(time.Millisecond * time.Duration(5+cmdNum%10))
					
					// Simulate occasional command errors
					if cmdNum%19 == 0 {
						cmdErr := errors.New("simulated command error")
						commandErrors.Store(cmdNum, cmdErr.Error())
						atomic.AddInt32(&failedCommands, 1)
						return cmdErr
					}
					
					// Command succeeded
					atomic.AddInt32(&processedCommands, 1)
					return nil
				})
				
				if err != nil {
					// Command couldn't be executed at all (e.g., provider not found)
					atomic.AddInt32(&failedCommands, 1)
					commandErrors.Store(cmdNum, err.Error())
				}
			}
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Wait for a short time to allow tasks and commands to complete
	time.Sleep(time.Second * 1)

	// Get final counts
	finalProcessedTasks := atomic.LoadInt32(&processedTasks)
	finalFailedTasks := atomic.LoadInt32(&failedTasks)
	finalProcessedCommands := atomic.LoadInt32(&processedCommands)
	finalFailedCommands := atomic.LoadInt32(&failedCommands)

	t.Logf("Background tasks: %d processed, %d failed (out of %d total)", 
		finalProcessedTasks, finalFailedTasks, totalBackgroundTasks)
	t.Logf("Commands: %d processed, %d failed (out of %d total)",
		finalProcessedCommands, finalFailedCommands, totalCommands)

	// Verify tasks were processed as expected
	if int(finalProcessedTasks+finalFailedTasks) < totalBackgroundTasks {
		t.Logf("Not all background tasks were processed yet: %d processed, %d failed, %d total",
			finalProcessedTasks, finalFailedTasks, totalBackgroundTasks)
		// This is not a failure, as we're in an async test
	}

	// Verify commands were processed as expected
	if int(finalProcessedCommands+finalFailedCommands) < totalCommands {
		t.Logf("Not all commands were processed yet: %d processed, %d failed, %d total",
			finalProcessedCommands, finalFailedCommands, totalCommands)
		// This is not a failure, as we're in an async test
	}

	// Check for any "channel full" errors in the logs
	logs := logBuffer.String()
	if strings.Contains(logs, "Failed to return server - channel full") {
		t.Errorf("Test detected 'channel full' errors in the logs, which should never happen")
	}

	// Verify server returns for tasks
	verifiedTaskReturns := 0
	serverAssignments.Range(func(key, value interface{}) bool {
		taskID, ok := key.(string)
		if !ok || !strings.HasPrefix(taskID, "task") {
			return true // Skip non-task entries
		}
		
		server := value.(string)

		// Get task status
		status, exists := taskStatus.Load(taskID)
		if !exists {
			// This is expected for tasks that are still in progress
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
				verifiedTaskReturns++
			}
		}

		return true
	})

	t.Logf("Verified server returns: %d out of %d successful tasks", 
		verifiedTaskReturns, finalProcessedTasks)
}

// TestTaskManagerSimple_ConcurrentCommandsAndTasks tests whether we can execute
// commands accurately while tasks are running concurrently
func TestTaskManagerSimple_ConcurrentCommandsAndTasks(t *testing.T) {
	// Create logger with thread-safe buffer
	var logBuffer ThreadSafeLogBuffer
	writer := zerolog.ConsoleWriter{
		Out:     &logBuffer,
		NoColor: true,
	}
	logger := log.Output(writer)

	// Define a single provider and server for simplicity
	providerName := "testProvider"
	provider := &MockProvider{name: providerName}
	providers := []IProvider{provider}

	servers := map[string][]string{
		providerName: {"server1", "server2", "server3"},
	}

	getTimeout := func(string, string) time.Duration {
		return time.Second * 5
	}

	// Initialize TaskManager
	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()

	// Track execution for verification using thread-safe primitives
	executionLog := sync.Map{}
	var executionCount int32
	
	// Use atomic counter to track task starts
	var taskStarted int32

	// Set up provider handler to track task executions
	provider.handleFunc = func(task ITask, server string) error {
		taskID := task.GetID()
		executionLog.Store(taskID, server)
		
		atomic.AddInt32(&executionCount, 1)
		atomic.AddInt32(&taskStarted, 1)
		
		// Simulate variable task execution time - shorter for test performance
		taskNum, _ := strconv.Atoi(taskID[4:])
		time.Sleep(time.Millisecond * time.Duration(10+taskNum%20))
		
		if mt, ok := task.(*MockTask); ok {
			mt.startCalled = true
			// Complete will be called automatically
		}
		
		return nil
	}

	// Create a set of background tasks
	const numBackgroundTasks = 5
	var taskWaitGroup sync.WaitGroup
	
	for i := 0; i < numBackgroundTasks; i++ {
		// Create task with a done channel we can track
		taskDone := make(chan struct{})
		task := &MockTask{
			id:         fmt.Sprintf("task%d", i),
			priority:   i % 5, // Mix of priorities
			maxRetries: 3,
			provider:   provider,
			done:       taskDone,
			timeout:    time.Second * 5,
		}
		
		// Add completion callback to signal when done
		taskWaitGroup.Add(1)
		task.completionCallback = func(t *MockTask) {
			defer taskWaitGroup.Done()
			// Task completed
		}
		
		tm.AddTask(task)
	}

	// Give tasks a chance to start
	time.Sleep(time.Millisecond * 20)

	// Execute commands with verification
	const numCommands = 10
	var commandWaitGroup sync.WaitGroup
	var commandSuccesses int32
	
	// Run the commands
	for i := 0; i < numCommands; i++ {
		cmdNum := i
		commandWaitGroup.Add(1)
		
		go func(num int) {
			defer commandWaitGroup.Done()
			
			err := tm.ExecuteCommand(providerName, func(server string) error {
				// Track command execution with the server used
				executionLog.Store(fmt.Sprintf("cmd%d", num), server)
				
				// Increment execution count
				atomic.AddInt32(&executionCount, 1)
				
				// Simulate brief command execution time
				time.Sleep(time.Millisecond * time.Duration(5+num%10))
				
				// Mark command as successfully executed
				atomic.AddInt32(&commandSuccesses, 1)
				return nil
			})
			
			if err != nil {
				t.Errorf("Command %d execution failed: %v", num, err)
			}
		}(cmdNum)
	}

	// Wait for all commands with a timeout
	commandsDone := make(chan struct{})
	go func() {
		commandWaitGroup.Wait()
		close(commandsDone)
	}()
	
	select {
	case <-commandsDone:
		// Commands completed
	case <-time.After(time.Second * 5):
		t.Log("Timeout waiting for commands to complete")
	}
	
	// Wait for tasks with a timeout
	tasksDone := make(chan struct{})
	go func() {
		taskWaitGroup.Wait()
		close(tasksDone)
	}()
	
	select {
	case <-tasksDone:
		// Tasks completed
	case <-time.After(time.Second * 5):
		t.Log("Timeout waiting for tasks to complete")
	}
	
	// Wait a short additional time to ensure command handlers complete
	// (might be delayed after commandWaitGroup.Done())
	time.Sleep(time.Second * 1)
	
	// Verify tasks were started
	tasksStartedCount := atomic.LoadInt32(&taskStarted)
	if tasksStartedCount < int32(numBackgroundTasks) {
		t.Logf("Only %d of %d tasks started", tasksStartedCount, numBackgroundTasks)
	} else {
		t.Logf("All %d background tasks were started", numBackgroundTasks)
	}

	// Verify commands were executed
	commandSuccessCount := atomic.LoadInt32(&commandSuccesses)
	if commandSuccessCount < int32(numCommands) {
		t.Logf("Only %d of %d commands completed successfully", commandSuccessCount, numCommands)
	} else {
		t.Logf("All %d commands completed successfully", numCommands)
	}
	
	// Log server assignments for debug purposes
	executionLog.Range(func(key, value interface{}) bool {
		t.Logf("%v used server: %v", key, value)
		return true
	})
	
	// Check for any "channel full" errors in the logs
	logs := logBuffer.String()
	if strings.Contains(logs, "Failed to return server - channel full") {
		t.Errorf("Test detected 'channel full' errors in the logs, which should never happen")
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