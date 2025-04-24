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
	var totalExecutions int32 = 0
	var duplicatesDetected int32 = 0

	// Random number generator with seed
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for _, name := range providerNames {
		provider := &MockProvider{name: name}
		providers = append(providers, provider)

		// Define a handle func that simulates varying processing times (1-8 seconds)
		// and tracks executions to detect duplicates
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
			processingTime := time.Duration(1000+rng.Intn(7000)) * time.Millisecond
			time.Sleep(processingTime)
			
			return nil
		}
		provider.handleFunc = providerHandleFuncs[name]
	}

	// Define servers for each provider (distributing evenly across providers)
	servers := make(map[string][]string)
	serverCount := len(serverURLs)
	serversPerProvider := serverCount / len(providers)
	
	for i, name := range providerNames {
		startIdx := i * serversPerProvider
		endIdx := (i + 1) * serversPerProvider
		if i == len(providers)-1 {
			// Last provider gets any remaining servers
			endIdx = serverCount
		}
		servers[name] = serverURLs[startIdx:endIdx]
	}

	// Define timeout function
	getTimeout := func(callbackName string, providerName string) time.Duration {
		return time.Second * 30 // Longer timeout to match longer task durations
	}

	// Initialize TaskManager with the server configuration
	InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)

	// Set parallelism limits for servers (1-2 per server URL)
	for _, url := range serverURLs {
		// Randomly assign either 1 or 2 as max parallel
		limit := 1
		if rng.Intn(2) == 1 {
			limit = 2
		}
		TaskQueueManagerInstance.SetTaskManagerServerMaxParallel(url, limit)
	}

	// Configuration for the load test
	totalTasks := 200 // Fewer tasks due to longer processing times
	numAdderGoroutines := 20 // Multiple goroutines adding tasks
	numModifierGoroutines := 8 // Goroutines that modify task queue state

	// Create a WaitGroup to wait for all operations to complete
	var wg sync.WaitGroup
	
	// Map to track which tasks we've created
	tasksCreated := make(map[string]*MockTask)
	tasksMutex := sync.Mutex{}
	
	// Channel to signal task completion
	taskCompleted := make(chan string, totalTasks*2) // Extra capacity for hotspot tasks
	
	// Create unique tasks with variable priorities
	for i := 0; i < totalTasks; i++ {
		taskID := fmt.Sprintf("task_%d", i)
		providerIndex := i % len(providers)
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
			timeout:    time.Second * 30,
			done:       make(chan struct{}),
		}
		
		tasksMutex.Lock()
		tasksCreated[taskID] = task
		tasksMutex.Unlock()
		
		// Create a goroutine to handle task completion
		go func(t *MockTask) {
			<-t.done
			taskCompleted <- t.GetID()
		}(task)
	}
	
	// Launch goroutines that add tasks to the queue at a rapid pace
	for i := 0; i < numAdderGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			
			// Each goroutine adds a subset of tasks
			startIdx := (goroutineID * totalTasks) / numAdderGoroutines
			endIdx := ((goroutineID + 1) * totalTasks) / numAdderGoroutines
			
			for taskIdx := startIdx; taskIdx < endIdx; taskIdx++ {
				taskID := fmt.Sprintf("task_%d", taskIdx)
				
				tasksMutex.Lock()
				task := tasksCreated[taskID]
				tasksMutex.Unlock()
				
				// Add the task to the queue
				AddTask(task, &logger)
				
				// Add some randomness to timing to increase chances of race conditions
				if taskIdx%3 == 0 {
					time.Sleep(time.Millisecond * time.Duration(rng.Intn(10)))
				}
			}
		}(i)
	}
	
	// Launch goroutines that perform "disruptive" operations while tasks are being added
	for i := 0; i < numModifierGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			
			// Create "hotspot" tasks that we'll repeatedly add and potentially cause duplicates
			for j := 0; j < 5; j++ { // Fewer hotspot tasks due to longer execution times
				taskID := fmt.Sprintf("hotspot_task_%d_%d", goroutineID, j)
				providerIndex := (goroutineID + j) % len(providers)
				priority := j % 10
				
				// Create a counter for this task's executions
				var counter int32
				taskExecutionCount.Store(taskID, &counter)
				
				task := &MockTask{
					id:         taskID,
					priority:   priority,
					maxRetries: 2,
					createdAt:  time.Now(),
					provider:   providers[providerIndex],
					timeout:    time.Second * 30,
					done:       make(chan struct{}),
				}
				
				// Monitor this task's completion
				go func(t *MockTask) {
					<-t.done
					taskCompleted <- t.GetID()
				}(task)
				
				// Add the task multiple times in quick succession
				for k := 0; k < 8; k++ {
					// Try to add the task multiple times (potentially causing duplicates)
					AddTask(task, &logger)
					
					// Add random delay between attempts
					delay := time.Duration(rng.Intn(20)) * time.Millisecond
					time.Sleep(delay)
				}
			}
		}(i)
	}
	
	// Wait for all adder goroutines to finish adding tasks
	wg.Wait()
	
	// Wait for tasks to complete or timeout
	completedTasks := 0
	expectedCompletions := totalTasks + (numModifierGoroutines * 5) // Regular tasks + hotspot tasks
	timeout := time.After(time.Second * 180) // 3 minutes timeout
	
	for completedTasks < expectedCompletions {
		select {
		case <-taskCompleted:
			completedTasks++
			if completedTasks%20 == 0 {
				t.Logf("Progress: %d/%d tasks completed", completedTasks, expectedCompletions)
			}
		case <-timeout:
			t.Logf("Timeout waiting for tasks to complete: %d/%d done", completedTasks, expectedCompletions)
			goto Finished
		}
	}
	
Finished:
	// Check for duplicates and report statistics
	var duplicateCount int32 = 0
	var duplicateTasks []string
	
	taskExecutionCount.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		count := atomic.LoadInt32(value.(*int32))
		if count > 1 {
			duplicateCount++
			duplicateTasks = append(duplicateTasks, fmt.Sprintf("%s (%d times)", taskID, count))
		}
		return true
	})
	
	// Report statistics
	t.Logf("Test completed: %d/%d tasks finished", completedTasks, expectedCompletions)
	t.Logf("Total executions: %d (should equal number of tasks if no duplicates)", totalExecutions)
	t.Logf("Duplicate tasks detected: %d", duplicateCount)
	
	if len(duplicateTasks) > 0 {
		t.Errorf("Found %d tasks that were executed multiple times: %v", duplicateCount, duplicateTasks)
	}
	
	// Shutdown TaskManager
	TaskQueueManagerInstance.Shutdown()
}