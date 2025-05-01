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

	// only signal the channel if it was set
	if t.taskCompletionCh != nil {
		t.taskCompletionCh <- t.id
	}

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
	logger := log.Output(zerolog.ConsoleWriter{Out: zerolog.NewConsoleWriter().Out})

	// Create server list with the production structure (10 servers, each with 10 sub-servers)
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
	}

	// Create all server URLs with multiple paths per server
	var serverURLs []string
	for _, server := range mainServers {
		for i := 1; i <= 10; i++ {
			serverURLs = append(serverURLs, fmt.Sprintf("%s/server%d/call", server, i))
		}
	}

	// Define providers
	providerNames := []string{"provider1", "provider2", "provider3", "provider4", "provider5"}
	var providers []IProvider
	providerHandleFuncs := make(map[string]func(task ITask, server string) error)

	mainSeed := time.Now().UnixNano()

	// Track execution status
	var tasksAdded int32
	var tasksSucceeded int32
	var tasksFailed int32
	var tasksCompleted int32

	// Map to track all tasks
	taskMap := sync.Map{}

	// Simulated server resource crunch
	serverResourceCrunch := make(chan struct{}, 10)

	for _, name := range providerNames {
		provider := &MockProvider{name: name}
		providers = append(providers, provider)

		// Define handler with random delays and possible failures
		providerHandleFuncs[name] = func(task ITask, server string) error {
			localRng := rand.New(rand.NewSource(mainSeed + int64(len(task.GetID()))))
			taskID := task.GetID()

			isHeavyTask := localRng.Intn(10) < 3 // 30%
			if isHeavyTask {
				select {
				case serverResourceCrunch <- struct{}{}:
					defer func() { <-serverResourceCrunch }()
				case <-time.After(100 * time.Millisecond):
				}
			}

			var processingTime time.Duration
			if localRng.Intn(20) == 0 {
				processingTime = time.Duration(800+localRng.Intn(700)) * time.Millisecond
			} else {
				processingTime = time.Duration(100+localRng.Intn(700)) * time.Millisecond
			}

			// Possibly simulate CPU load
			if localRng.Intn(10) < 2 {
				start := time.Now()
				for time.Since(start) < 50*time.Millisecond {
					for i := 0; i < 1000000; i++ {
						_ = i * i
					}
				}
			}

			time.Sleep(processingTime)

			// Network jitter
			if localRng.Intn(10) < 3 {
				jitter := time.Duration(localRng.Intn(200)) * time.Millisecond
				time.Sleep(jitter)
			}

			// 10% fail
			if localRng.Intn(10) == 0 {
				return fmt.Errorf("simulated error for task %s", taskID)
			}
			return nil
		}
		provider.handleFunc = providerHandleFuncs[name]
	}

	// Distribute servers among providers
	servers := make(map[string][]string)
	serverCount := len(serverURLs)
	serversPerProvider := serverCount / len(providers)

	for i, name := range providerNames {
		startIdx := i * serversPerProvider
		endIdx := (i + 1) * serversPerProvider
		if i == len(providers)-1 {
			endIdx = serverCount
		}
		servers[name] = serverURLs[startIdx:endIdx]
	}

	// Timeout
	getTimeout := func(cbName string, providerName string) time.Duration {
		return 30 * time.Second
	}

	// Init TaskManager
	InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)

	// Set parallel limits
	for i, url := range serverURLs {
		var limit int
		switch i % 4 {
		case 0:
			limit = 1
		case 1:
			limit = 2
		case 2:
			limit = 1
		default:
			limit = 3
		}
		TaskQueueManagerInstance.SetTaskManagerServerMaxParallel(url, limit)
	}

	initialTasks := 500
	taskCompletionCh := make(chan string, 2000)

	// We'll use both 'continuousAdding' and a new 'testRunning'
	var continuousAdding int32 = 1
	var testRunning int32 = 1

	// Ensure we clear testRunning to 0 before returning
	defer atomic.StoreInt32(&testRunning, 0)

	createTask := func(id int, shouldTrack bool) *ExtendedMockTask {
		taskID := fmt.Sprintf("task_%d", id)
		providerIndex := id % len(providers)
		priority := id % 10

		task := &ExtendedMockTask{
			MockTask: MockTask{
				id:         taskID,
				priority:   priority,
				maxRetries: 2,
				createdAt:  time.Now(),
				provider:   providers[providerIndex],
				timeout:    30 * time.Second,
				done:       make(chan struct{}),
			},
			tasksSucceeded:   &tasksSucceeded,
			tasksFailed:      &tasksFailed,
			tasksCompleted:   &tasksCompleted,
			taskCompletionCh: taskCompletionCh,
		}
		if shouldTrack {
			taskMap.Store(taskID, task)
			atomic.AddInt32(&tasksAdded, 1)
		}
		return task
	}

	// Add initial tasks
	for i := 0; i < initialTasks; i++ {
		task := createTask(i, true)
		go func(t *ExtendedMockTask, taskNum int) {
			goroutineRng := rand.New(rand.NewSource(mainSeed + int64(taskNum)))
			time.Sleep(time.Duration(goroutineRng.Intn(50)) * time.Millisecond)
			AddTask(t, &logger)
		}(task, i)
	}

	// Goroutine to continuously add tasks
	var taskIDCounter int32 = int32(initialTasks)
	var continuousTasksAdded int32

	var addingWg sync.WaitGroup
	addingWg.Add(1)
	go func() {
		defer addingWg.Done()
		burstRng := rand.New(rand.NewSource(mainSeed + 1000))
		for atomic.LoadInt32(&continuousAdding) == 1 && atomic.LoadInt32(&testRunning) == 1 {
			burstSize := 5 + burstRng.Intn(16)
			var wg sync.WaitGroup
			for i := 0; i < burstSize; i++ {
				wg.Add(1)
				go func(burstIndex int) {
					defer wg.Done()
					goroutineRng := rand.New(rand.NewSource(mainSeed + 2000 + int64(burstIndex)))
					id := atomic.AddInt32(&taskIDCounter, 1)
					tracked := goroutineRng.Intn(6) == 0
					task := createTask(int(id), tracked)
					if tracked {
						atomic.AddInt32(&continuousTasksAdded, 1)
					}
					if goroutineRng.Intn(3) == 0 {
						AddTask(task, &logger)
					} else {
						time.Sleep(time.Duration(goroutineRng.Intn(30)) * time.Millisecond)
						AddTask(task, &logger)
					}
				}(i)
			}
			wg.Wait()
			time.Sleep(time.Duration(10+burstRng.Intn(90)) * time.Millisecond)
		}
	}()

	// Goroutine that periodically changes server parallelism
	var parallelismWg sync.WaitGroup
	parallelismWg.Add(1)
	go func() {
		defer parallelismWg.Done()
		parallelismRng := rand.New(rand.NewSource(mainSeed + 4000))
		for atomic.LoadInt32(&continuousAdding) == 1 && atomic.LoadInt32(&testRunning) == 1 {
			time.Sleep(time.Duration(2000+parallelismRng.Intn(3000)) * time.Millisecond)

			// Might have ended, re-check
			if atomic.LoadInt32(&testRunning) == 0 {
				return
			}
			serverIndex := parallelismRng.Intn(len(serverURLs))
			server := serverURLs[serverIndex]
			newLimit := 1 + parallelismRng.Intn(3)
			TaskQueueManagerInstance.SetTaskManagerServerMaxParallel(server, newLimit)

			if parallelismRng.Intn(5) == 0 {
				// Safe to log only if test hasn't ended
				t.Logf("Changed parallelism limit for %s to %d", server, newLimit)
			}
		}
	}()

	// Wait for tasks
	completedTasks := 0
	expectedMinimumCompletions := initialTasks
	progressTicker := time.NewTicker(5 * time.Second)
	defer progressTicker.Stop()

	missingTaskTimeout := time.After(2 * time.Minute)
	testDeadline := time.After(3 * time.Minute)

CheckLoop:
	for {
		select {
		case <-testDeadline:
			t.Log("Test deadline reached. Stopping test.")
			break CheckLoop

		case <-missingTaskTimeout:
			t.Logf("Timeout waiting for tasks to complete: %d/%d done",
				completedTasks, expectedMinimumCompletions)
			break CheckLoop

		case <-progressTicker.C:
			expectedMinimumCompletions = initialTasks + int(atomic.LoadInt32(&continuousTasksAdded))
			t.Logf("Progress: %d/%d tasks completed (Added: %d, Continuous: %d)",
				completedTasks, expectedMinimumCompletions,
				atomic.LoadInt32(&tasksAdded),
				atomic.LoadInt32(&continuousTasksAdded))

		case <-taskCompletionCh:
			completedTasks++
			expectedMinimumCompletions = initialTasks + int(atomic.LoadInt32(&continuousTasksAdded))
			if completedTasks >= expectedMinimumCompletions {
				time.Sleep(2 * time.Second)
				break CheckLoop
			}
		}
	}

	// Stop continuous tasks
	atomic.StoreInt32(&continuousAdding, 0)

	// Wait for those goroutines to exit
	addingWg.Wait()
	parallelismWg.Wait()

	// Wait a bit for counters to finalize
	time.Sleep(6 * time.Second)

	t.Logf("Tasks Added:     %d", atomic.LoadInt32(&tasksAdded))
	t.Logf("Tasks Succeeded: %d", atomic.LoadInt32(&tasksSucceeded))
	t.Logf("Tasks Failed:    %d", atomic.LoadInt32(&tasksFailed))
	t.Logf("Tasks Completed: %d", atomic.LoadInt32(&tasksCompleted))

	// Identify missing tasks
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

	totalSuccessFail := atomic.LoadInt32(&tasksSucceeded) + atomic.LoadInt32(&tasksFailed)
	if atomic.LoadInt32(&tasksCompleted) < totalSuccessFail {
		t.Errorf("Some tasks were succeeded/failed but not completed. Success+Failed: %d, Completed: %d",
			totalSuccessFail, atomic.LoadInt32(&tasksCompleted))
	}

	if atomic.LoadInt32(&tasksCompleted) < atomic.LoadInt32(&tasksAdded) {
		t.Errorf("%d tasks were added but never completed",
			atomic.LoadInt32(&tasksAdded)-atomic.LoadInt32(&tasksCompleted))
	}

	if len(incompleteTasks) > 0 {
		t.Errorf("Found %d tasks that never completed: %v", len(incompleteTasks), incompleteTasks)
	}

	// Finally shut down TaskManager
	TaskQueueManagerInstance.Shutdown()
}
