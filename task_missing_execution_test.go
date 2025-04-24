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

// ExtendedMockTask extends MockTask to track execution status
type ExtendedMockTask struct {
	MockTask
	statusMutex      sync.Mutex
	succeeded        bool
	failed           bool
	completed        bool
	taskCompletionCh chan<- string
	tasksSucceeded   *int32
	tasksFailed      *int32
	tasksCompleted   *int32
}

func (t *ExtendedMockTask) MarkAsSuccess(time int64) {
	t.statusMutex.Lock()
	t.succeeded = true
	t.statusMutex.Unlock()
	atomic.AddInt32(t.tasksSucceeded, 1)
	t.MockTask.MarkAsSuccess(time)
}

func (t *ExtendedMockTask) MarkAsFailed(time int64, err error) {
	t.statusMutex.Lock()
	t.failed = true
	t.statusMutex.Unlock()
	atomic.AddInt32(t.tasksFailed, 1)
	t.MockTask.MarkAsFailed(time, err)
}

func (t *ExtendedMockTask) OnComplete() {
	t.statusMutex.Lock()
	t.completed = true
	t.statusMutex.Unlock()
	atomic.AddInt32(t.tasksCompleted, 1)

	// Signal completion
	t.taskCompletionCh <- t.GetID()

	t.MockTask.OnComplete()
}

func (t *ExtendedMockTask) GetStatus() (bool, bool, bool) {
	t.statusMutex.Lock()
	defer t.statusMutex.Unlock()
	return t.succeeded, t.failed, t.completed
}

// TestTasksNeverCompletedHighStress tests for a bug where tasks are added but never completed
// under high stress conditions with continuous task addition
func TestTasksNeverCompletedHighStress(t *testing.T) {
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

	// Random number generator with seed
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Counters to track task execution status
	var tasksAdded int32
	var tasksSucceeded int32
	var tasksFailed int32
	var tasksCompleted int32

	// Map to track all tasks
	taskMap := sync.Map{}

	// Create a simulated server resource crunch by occasionally sleeping in the handler
	serverResourceCrunch := make(chan struct{}, 10) // Limiting to 10 concurrent "heavy" tasks

	for _, name := range providerNames {
		provider := &MockProvider{name: name}
		providers = append(providers, provider)

		// Define handler function that simulates varying processing times
		// and occasional resource crunches
		providerHandleFuncs[name] = func(task ITask, server string) error {
			taskID := task.GetID()

			// Determine if this is a "heavy" task that will cause resource contention
			isHeavyTask := rng.Intn(10) < 3 // 30% of tasks are "heavy"

			if isHeavyTask {
				select {
				case serverResourceCrunch <- struct{}{}:
					// Got a resource slot, will release at the end
					defer func() {
						<-serverResourceCrunch
					}()
				case <-time.After(time.Millisecond * 100):
					// If can't get a resource slot quickly, proceed without it
					// This increases the chance of race conditions
				}
			}

			// Simulate realistic processing time (1-8 seconds)
			// With some tasks taking longer to increase the chances of race conditions
			var processingTime time.Duration
			if rng.Intn(20) == 0 { // 5% of tasks take much longer
				processingTime = time.Duration(800+rng.Intn(700)) * time.Millisecond
			} else {
				processingTime = time.Duration(100+rng.Intn(700)) * time.Millisecond
			}

			// Simulate CPU-intensive work occasionally
			if rng.Intn(10) < 2 { // 20% of tasks
				start := time.Now()
				for time.Since(start) < time.Millisecond*50 {
					// Busy loop to consume CPU and potentially cause scheduling issues
					for i := 0; i < 1000000; i++ {
						_ = i * i
					}
				}
			}

			time.Sleep(processingTime)

			// Simulate occasional network issues by adding jitter to task completion
			if rng.Intn(10) < 3 { // 30% of tasks experience "network jitter"
				jitter := time.Duration(rng.Intn(200)) * time.Millisecond
				time.Sleep(jitter)
			}

			// Occasionally fail tasks (about 10% of the time)
			if rng.Intn(10) == 0 {
				// Return error to simulate failure
				return fmt.Errorf("simulated error for task %s", taskID)
			}

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

	// Set parallelism limits for servers with more variety to increase contention
	for i, url := range serverURLs {
		// Pattern of limits: [1,2,1,3,1,2,1,4...] to create more varied contention
		var limit int
		switch i % 4 {
		case 0:
			limit = 1
		case 1:
			limit = 2
		case 2:
			limit = 1
		case 3:
			limit = 3
		}
		TaskQueueManagerInstance.SetTaskManagerServerMaxParallel(url, limit)
	}

	// Configuration for the load test
	initialTasks := 500
	maxTasks := 1500
	continuousAdding := true

	// Channel to signal task completion
	taskCompletionCh := make(chan string, maxTasks)

	// Create task generator function that will be used both initially and continuously
	createTask := func(id int, shouldTrack bool) *ExtendedMockTask {
		taskID := fmt.Sprintf("task_%d", id)
		providerIndex := id % len(providers)
		priority := id % 10

		// Create extended task with tracking capabilities
		task := &ExtendedMockTask{
			MockTask: MockTask{
				id:         taskID,
				priority:   priority,
				maxRetries: 2,
				createdAt:  time.Now(),
				provider:   providers[providerIndex],
				timeout:    time.Second * 30,
				done:       make(chan struct{}),
			},
			tasksSucceeded:   &tasksSucceeded,
			tasksFailed:      &tasksFailed,
			tasksCompleted:   &tasksCompleted,
			taskCompletionCh: taskCompletionCh,
		}

		// Only track tasks if requested (we'll track all initial tasks but not all continuous tasks)
		if shouldTrack {
			taskMap.Store(taskID, task)
			atomic.AddInt32(&tasksAdded, 1)
		}

		return task
	}

	// Create and add initial tasks
	for i := 0; i < initialTasks; i++ {
		task := createTask(i, true)

		// Add the task in a goroutine to simulate concurrent adding
		go func(t *ExtendedMockTask) {
			// Add random delay to increase chances of race conditions
			time.Sleep(time.Millisecond * time.Duration(rng.Intn(50)))
			AddTask(t, &logger)
		}(task)
	}

	// Start goroutine to continuously add tasks while the test is running
	var taskIDCounter int32 = int32(initialTasks)
	var continuousTasksAdded int32

	go func() {
		for continuousAdding {
			// Create burst of tasks (5-20 at a time)
			burstSize := 5 + rng.Intn(16)

			// Launch tasks in parallel to increase contention
			var wg sync.WaitGroup
			for i := 0; i < burstSize; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					// Generate unique task ID
					id := atomic.AddInt32(&taskIDCounter, 1)

					// 1 in 6 continuously added tasks are tracked and counted in our verification
					tracked := rng.Intn(6) == 0

					task := createTask(int(id), tracked)
					if tracked {
						atomic.AddInt32(&continuousTasksAdded, 1)
					}

					// Add task with randomized timing
					if rng.Intn(3) == 0 {
						// Quick add
						AddTask(task, &logger)
					} else {
						// Add with delay to simulate network jitter or client delay
						time.Sleep(time.Duration(rng.Intn(30)) * time.Millisecond)
						AddTask(task, &logger)
					}
				}()
			}

			// Wait for this burst to complete
			wg.Wait()

			// Random pause between bursts (10-100ms)
			time.Sleep(time.Duration(10+rng.Intn(90)) * time.Millisecond)
		}
	}()

	// Periodically execute commands between server load changes
	go func() {
		for continuousAdding {
			// Sleep for a random duration
			time.Sleep(time.Duration(500+rng.Intn(1000)) * time.Millisecond)

			// Pick a random provider
			providerIndex := rng.Intn(len(providers))
			providerName := providerNames[providerIndex]

			// Execute a command to add more server/task interaction complexity
			err := TaskQueueManagerInstance.ExecuteCommand(providerName, func(server string) error {
				// Simulate some command processing time
				time.Sleep(time.Duration(50+rng.Intn(200)) * time.Millisecond)
				return nil
			})

			if err != nil {
				t.Logf("Command execution failed: %v", err)
			}
		}
	}()

	// Periodically change server parallelism limits to simulate system reconfigurations
	go func() {
		for continuousAdding {
			// Sleep for a random duration between changes
			time.Sleep(time.Duration(2000+rng.Intn(3000)) * time.Millisecond)

			// Pick a random server
			serverIndex := rng.Intn(len(serverURLs))
			server := serverURLs[serverIndex]

			// Randomly change its parallelism limit
			newLimit := 1 + rng.Intn(3) // 1, 2, or 3
			TaskQueueManagerInstance.SetTaskManagerServerMaxParallel(server, newLimit)

			// Log this change occasionally
			if rng.Intn(5) == 0 {
				t.Logf("Changed parallelism limit for %s to %d", server, newLimit)
			}
		}
	}()

	// Wait for all initial tasks plus tracked continuous tasks to complete or timeout
	completedTasks := 0
	expectedMinimumCompletions := initialTasks
	missingTaskTimeout := time.After(time.Minute * 2) // 2 minutes should be enough
	progressTicker := time.NewTicker(time.Second * 5)
	defer progressTicker.Stop()

	// Force the test to end if this deadline is reached
	testDeadline := time.After(time.Minute * 3)

CheckingLoop:
	for {
		select {
		case <-taskCompletionCh:
			completedTasks++

			// Keep adding the tracked continuous tasks to our expected count
			expectedMinimumCompletions = initialTasks + int(atomic.LoadInt32(&continuousTasksAdded))

			// If we've completed all tracked tasks, we can finish
			if completedTasks >= expectedMinimumCompletions {
				// Wait a bit longer to make sure we catch any stragglers
				time.Sleep(time.Second * 2)
				break CheckingLoop
			}

		case <-progressTicker.C:
			// Log progress and counts
			expectedMinimumCompletions = initialTasks + int(atomic.LoadInt32(&continuousTasksAdded))
			t.Logf("Progress: %d/%d tasks completed (Added: %d, Continuous: %d)",
				completedTasks, expectedMinimumCompletions,
				atomic.LoadInt32(&tasksAdded),
				atomic.LoadInt32(&continuousTasksAdded))

		case <-missingTaskTimeout:
			t.Logf("Timeout waiting for tasks to complete: %d/%d done",
				completedTasks, expectedMinimumCompletions)
			break CheckingLoop

		case <-testDeadline:
			t.Logf("Test deadline reached. Stopping test.")
			break CheckingLoop
		}
	}

	// Stop continuous task addition
	continuousAdding = false

	// Wait a bit longer to make sure all counters are updated
	time.Sleep(time.Second * 2)

	// Log summary statistics
	t.Logf("Tasks Added:     %d", atomic.LoadInt32(&tasksAdded))
	t.Logf("Tasks Succeeded: %d", atomic.LoadInt32(&tasksSucceeded))
	t.Logf("Tasks Failed:    %d", atomic.LoadInt32(&tasksFailed))
	t.Logf("Tasks Completed: %d", atomic.LoadInt32(&tasksCompleted))

	// List missing tasks
	var incompleteTasks []string
	taskMap.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		task := value.(*ExtendedMockTask)

		succeeded, failed, completed := task.GetStatus()

		if !completed {
			details := fmt.Sprintf("%s [succeeded: %t, failed: %t, completed: %t]",
				taskID, succeeded, failed, completed)
			incompleteTasks = append(incompleteTasks, details)
		}

		return true
	})

	// Perform final validation
	totalSuccessFail := atomic.LoadInt32(&tasksSucceeded) + atomic.LoadInt32(&tasksFailed)

	// Check if tasks marked as succeeded/failed were also completed
	if atomic.LoadInt32(&tasksCompleted) < totalSuccessFail {
		t.Errorf("Some tasks were marked as succeeded/failed but not completed. Success+Failed: %d, Completed: %d",
			totalSuccessFail, atomic.LoadInt32(&tasksCompleted))
	}

	// Check if any tasks were added but never completed
	if atomic.LoadInt32(&tasksCompleted) < atomic.LoadInt32(&tasksAdded) {
		t.Errorf("%d tasks were added but never completed",
			atomic.LoadInt32(&tasksAdded)-atomic.LoadInt32(&tasksCompleted))
	}

	// If we found any incomplete tasks, log them and fail the test
	if len(incompleteTasks) > 0 {
		t.Errorf("Found %d tasks that were added but never completed: %v",
			len(incompleteTasks), incompleteTasks)
	}

	// Shutdown TaskManager
	TaskQueueManagerInstance.Shutdown()
}
