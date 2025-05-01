package smt

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// TestNoConcurrentDuplicates ensures that if multiple goroutines call AddTask
// with the SAME ITask instance concurrently, it actually runs only once.
func TestNoConcurrentDuplicates(t *testing.T) {
	// 1. Setup logger
	logger := log.Output(zerolog.ConsoleWriter{Out: zerolog.NewConsoleWriter().Out})

	// 2. Create a single provider that increments a counter on Handle()
	var executionCount int32

	provider := &MockProvider{
		name: "testProvider",
		handleFunc: func(task ITask, server string) error {
			// Increment the execution counter
			atomic.AddInt32(&executionCount, 1)

			// Simulate some processing time
			time.Sleep(1 * time.Second)
			return nil
		},
	}

	// 3. Prepare the manager with a single provider and a single server
	var providers []IProvider = []IProvider{provider}
	servers := map[string][]string{
		"testProvider": {"https://test.server1/call"},
	}

	// Timeout function
	getTimeout := func(callbackName string, providerName string) time.Duration {
		return 10 * time.Second
	}

	// 4. Initialize the TaskManager with no preloaded tasks
	InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)

	// (Optional) set concurrency limit for that server
	TaskQueueManagerInstance.SetTaskManagerServerMaxParallel("https://test.server1/call", 5)

	// 5. Prepare a single task that we will attempt to add from multiple goroutines
	mockTask := &MockTask{
		id:         "unique_task_123",
		priority:   5,
		maxRetries: 1,
		createdAt:  time.Now(),
		provider:   provider,
		timeout:    5 * time.Second,
		done:       make(chan struct{}),
	}

	// We will close mockTask.done once the task is truly complete in OnComplete
	// (Assuming your MockTask calls `close(t.done)` in `OnComplete()`.)

	// 6. Run concurrent AddTask calls
	numGoroutines := 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			// Each goroutine tries to add the same task
			AddTask(mockTask, &logger)
		}(i)
	}

	wg.Wait()

	// 7. Wait for the task to finish
	select {
	case <-mockTask.done:
		// The task is done
	case <-time.After(30 * time.Second):
		t.Fatalf("Test timed out waiting for the task to complete")
	}

	// 8. Check how many times it actually executed
	finalCount := atomic.LoadInt32(&executionCount)
	t.Logf("Task was executed %d time(s)", finalCount)

	if finalCount != 1 {
		t.Errorf("Expected exactly 1 execution, but got %d. Duplicates exist!", finalCount)
	}

	// 9. Clean up
	TaskQueueManagerInstance.Shutdown()
}
