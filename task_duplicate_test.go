package smt

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// TestConcurrentTaskAddingDuplication tests for a race condition where tasks
// are added to the queue multiple times.
func TestConcurrentTaskAddingDuplication(t *testing.T) {
	// Setup logger
	logger := log.Output(zerolog.ConsoleWriter{Out: zerolog.NewConsoleWriter().Out})

	// Create server list with the production structure (10 servers with 10 sub-servers each)
	mainServers := []string{
		"https://phoebe.soulkyn.com",
		"https://felix.soulkyn.com",
		"https://joey.soulkyn.com",
		"https://leekie.soulkyn.com",
		"https://sk-dev-4.soulkyn.com",
		"https://l40neb.soulkyn.com",
		"https://devai2.soulkyn.com",
		"https://vidneb.soulkyn.com",
		"https://dev-5.soulkyn.com",
		"https://claude.soulkyn.com",
	}

	// Create all server URLs with multiple paths per server
	var serverURLs []string
	for _, server := range mainServers {
		for i := 1; i <= 10; i++ {
			serverURLs = append(serverURLs, fmt.Sprintf("%s/server%d/call", server, i))
		}
	}

	// Define providers that will process tasks with different speeds
	providerNames := []string{"provider1", "provider2", "provider3", "provider4", "provider5"}
	var providers []IProvider
	providerHandleFuncs := make(map[string]func(task ITask, server string) error)

	// Create a map to track task execution counts - this will help identify duplications
	taskExecutionCount := sync.Map{}
	var totalExecutions int32
	var duplicatesDetected int32

	// Create a shared RNG and protect it with a mutex to avoid race conditions
	var rngMutex sync.Mutex
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Helper function to safely call rng.Intn(...)
	safeIntn := func(n int) int {
		rngMutex.Lock()
		val := rng.Intn(n)
		rngMutex.Unlock()
		return val
	}

	// Build providers and define a handle function simulating variable processing times
	for _, name := range providerNames {
		provider := &MockProvider{name: name}
		providers = append(providers, provider)

		providerHandleFuncs[name] = func(task ITask, server string) error {
			taskID := task.GetID()

			// Count executions per task to detect duplicates
			var counter int32
			val, _ := taskExecutionCount.LoadOrStore(taskID, &counter)
			counterPtr := val.(*int32)
			newCount := atomic.AddInt32(counterPtr, 1)
			atomic.AddInt32(&totalExecutions, 1)

			// If we've executed this task more than once, it's a duplicate
			if newCount > 1 {
				t.Logf("DUPLICATE DETECTED: Task %s was executed %d times", taskID, newCount)
				atomic.AddInt32(&duplicatesDetected, 1)
			}

			// Simulate realistic processing time (1-8 seconds)
			processingTime := time.Duration(1000+safeIntn(7000)) * time.Millisecond
			time.Sleep(processingTime)

			return nil
		}
		provider.handleFunc = providerHandleFuncs[name]
	}

	// Define servers for each provider (distributing evenly)
	servers := make(map[string][]string)
	serverCount := len(serverURLs)
	serversPerProvider := serverCount / len(providerNames)

	for i, name := range providerNames {
		startIdx := i * serversPerProvider
		endIdx := (i + 1) * serversPerProvider
		if i == len(providerNames)-1 {
			// Last provider gets any leftover servers
			endIdx = serverCount
		}
		servers[name] = serverURLs[startIdx:endIdx]
	}

	// Define timeout function
	getTimeout := func(callbackName string, providerName string) time.Duration {
		return 30 * time.Second
	}

	// Initialize TaskManager
	InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)

	// Set parallelism limits for servers (1-2 per server URL)
	for _, url := range serverURLs {
		limit := 1
		if safeIntn(2) == 1 {
			limit = 2
		}
		TaskQueueManagerInstance.SetTaskManagerServerMaxParallel(url, limit)
	}

	// Configuration for the load test
	totalTasks := 200
	numAdderGoroutines := 20
	numModifierGoroutines := 8

	var wg sync.WaitGroup

	// Map to track tasks
	tasksCreated := make(map[string]*MockTask)
	tasksMutex := sync.Mutex{}

	// Channel to signal task completion
	taskCompleted := make(chan string, totalTasks*2) // capacity for extra hotspot tasks

	// Create unique tasks with variable priorities
	for i := 0; i < totalTasks; i++ {
		taskID := fmt.Sprintf("task_%d", i)
		providerIndex := i % len(providerNames)
		priority := i % 10

		// Create a counter for this task's executions
		var counter int32
		taskExecutionCount.Store(taskID, &counter)

		task := &MockTask{
			id:         taskID,
			priority:   priority,
			maxRetries: 2,
			createdAt:  time.Now(),
			provider:   providers[providerIndex],
			timeout:    30 * time.Second,
			done:       make(chan struct{}),
		}

		tasksMutex.Lock()
		tasksCreated[taskID] = task
		tasksMutex.Unlock()

		go func(t *MockTask) {
			<-t.done
			taskCompleted <- t.GetID()
		}(task)
	}

	// Launch goroutines that add tasks
	for i := 0; i < numAdderGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			startIdx := (goroutineID * totalTasks) / numAdderGoroutines
			endIdx := ((goroutineID + 1) * totalTasks) / numAdderGoroutines

			for taskIdx := startIdx; taskIdx < endIdx; taskIdx++ {
				taskID := fmt.Sprintf("task_%d", taskIdx)

				tasksMutex.Lock()
				task := tasksCreated[taskID]
				tasksMutex.Unlock()

				// Add the task
				AddTask(task, &logger)

				// Add randomness
				if taskIdx%3 == 0 {
					time.Sleep(time.Millisecond * time.Duration(safeIntn(10)))
				}
			}
		}(i)
	}

	// Launch goroutines that do "disruptive" operations (hotspot tasks)
	for i := 0; i < numModifierGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			// Create "hotspot" tasks
			for j := 0; j < 5; j++ {
				taskID := fmt.Sprintf("hotspot_task_%d_%d", goroutineID, j)
				providerIndex := (goroutineID + j) % len(providerNames)
				priority := j % 10

				var counter int32
				taskExecutionCount.Store(taskID, &counter)

				task := &MockTask{
					id:         taskID,
					priority:   priority,
					maxRetries: 2,
					createdAt:  time.Now(),
					provider:   providers[providerIndex],
					timeout:    30 * time.Second,
					done:       make(chan struct{}),
				}

				go func(t *MockTask) {
					<-t.done
					taskCompleted <- t.GetID()
				}(task)

				// Add the task multiple times
				for k := 0; k < 8; k++ {
					AddTask(task, &logger)
					// Random delay
					time.Sleep(time.Duration(safeIntn(20)) * time.Millisecond)
				}
			}
		}(i)
	}

	// Wait for all adder goroutines
	wg.Wait()

	// Wait for tasks to complete or timeout
	completedTasks := 0
	expectedCompletions := totalTasks + (numModifierGoroutines * 5)
	timeout := time.After(3 * time.Minute)

WaitLoop:
	for completedTasks < expectedCompletions {
		select {
		case <-taskCompleted:
			completedTasks++
			if completedTasks%20 == 0 {
				t.Logf("Progress: %d/%d tasks completed", completedTasks, expectedCompletions)
			}
		case <-timeout:
			t.Logf("Timeout waiting for tasks to complete: %d/%d done", completedTasks, expectedCompletions)
			break WaitLoop
		}
	}

	// Check duplicates
	var duplicateCount int32
	var duplicateTasks []string

	taskExecutionCount.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		count := atomic.LoadInt32(value.(*int32))
		if count > 1 {
			atomic.AddInt32(&duplicateCount, 1)
			duplicateTasks = append(duplicateTasks, fmt.Sprintf("%s (%d times)", taskID, count))
		}
		return true
	})

	// Report stats
	t.Logf("Test completed: %d/%d tasks finished", completedTasks, expectedCompletions)
	t.Logf("Total executions: %d (should match # tasks if no duplicates)", atomic.LoadInt32(&totalExecutions))
	t.Logf("Duplicate tasks detected: %d", duplicateCount)

	if len(duplicateTasks) > 0 {
		t.Errorf("Found %d tasks that were executed multiple times: %v", duplicateCount, duplicateTasks)
	}

	// Shutdown TaskManager
	TaskQueueManagerInstance.Shutdown()
}
