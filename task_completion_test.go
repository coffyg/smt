package smt

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestTaskCompletionUnderLoad tests that tasks complete properly under load
func TestTaskCompletionUnderLoad(t *testing.T) {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: zerolog.NewConsoleWriter().Out}).
		Level(zerolog.ErrorLevel)

	// Create providers
	var providers []IProvider
	servers := make(map[string][]string)
	const numProviders = 5
	const serversPerProvider = 10

	var processed int32
	for i := 0; i < numProviders; i++ {
		providerName := fmt.Sprintf("provider%d", i)
		provider := &MockProvider{
			name: providerName,
			handleFunc: func(task ITask, server string) error {
				atomic.AddInt32(&processed, 1)
				// Simulate work
				time.Sleep(5 * time.Millisecond)
				// 10% failure rate based on task ID
				if task.GetID()[len(task.GetID())-1] == '3' {
					return fmt.Errorf("simulated error")
				}
				return nil
			},
		}
		providers = append(providers, provider)

		// Create servers
		serverList := make([]string, serversPerProvider)
		for j := 0; j < serversPerProvider; j++ {
			serverList[j] = fmt.Sprintf("server_%s_%d", providerName, j)
		}
		servers[providerName] = serverList
	}

	getTimeout := func(string, string) time.Duration {
		return 5 * time.Second
	}

	// Initialize task manager
	InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)
	defer TaskQueueManagerInstance.Shutdown()

	// Set concurrency limits on some servers
	for i := 0; i < numProviders; i++ {
		for j := 0; j < 3; j++ {
			server := fmt.Sprintf("server_provider%d_%d", i, j)
			TaskQueueManagerInstance.SetTaskManagerServerMaxParallel(server, 2)
		}
	}

	// Submit tasks
	const numTasks = 1000
	var wg sync.WaitGroup
	var submitted int32

	for i := 0; i < numTasks; i++ {
		task := &MockTask{
			id:         fmt.Sprintf("task_%d", i),
			priority:   i % 10,
			maxRetries: 2,
			provider:   providers[i%numProviders],
			timeout:    1 * time.Second,
			done:       make(chan struct{}),
		}

		wg.Add(1)
		go func(t *MockTask) {
			defer wg.Done()
			AddTask(t, &logger)
			atomic.AddInt32(&submitted, 1)
		}(task)
	}

	wg.Wait()
	
	// Wait for processing
	deadline := time.After(30 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastProcessed int32
	stableCount := 0
	
	for {
		select {
		case <-deadline:
			t.Log("Deadline reached")
			goto Done
		case <-ticker.C:
			current := atomic.LoadInt32(&processed)
			if current == lastProcessed {
				stableCount++
				if stableCount > 3 {
					t.Log("Processing stable for 3 seconds")
					goto Done
				}
			} else {
				stableCount = 0
				lastProcessed = current
			}
			t.Logf("Processed: %d/%d", current, numTasks)
		}
	}

Done:
	finalProcessed := atomic.LoadInt32(&processed)
	t.Logf("Final: Submitted=%d, Processed=%d", submitted, finalProcessed)
	
	// We expect at least 90% to be processed (some may fail due to retries)
	expectedMin := int32(float64(numTasks) * 0.9)
	if finalProcessed < expectedMin {
		t.Errorf("Too few tasks processed: %d, expected at least %d", finalProcessed, expectedMin)
	}
}

// TestConcurrencyLimitHandling tests the retry mechanism with concurrency limits
func TestConcurrencyLimitHandling(t *testing.T) {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: zerolog.NewConsoleWriter().Out}).
		Level(zerolog.ErrorLevel)

	var processed int32
	provider := &MockProvider{
		name: "limited_provider",
		handleFunc: func(task ITask, server string) error {
			atomic.AddInt32(&processed, 1)
			time.Sleep(50 * time.Millisecond)
			return nil
		},
	}
	providers := []IProvider{provider}
	
	servers := map[string][]string{
		"limited_provider": {"server1", "server2"},
	}

	getTimeout := func(string, string) time.Duration {
		return 1 * time.Second
	}

	InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)
	defer TaskQueueManagerInstance.Shutdown()

	// Set very strict concurrency limit
	TaskQueueManagerInstance.SetTaskManagerServerMaxParallel("server1", 1)
	TaskQueueManagerInstance.SetTaskManagerServerMaxParallel("server2", 1)

	// Submit tasks
	const numTasks = 20
	var wg sync.WaitGroup

	for i := 0; i < numTasks; i++ {
		task := &MockTask{
			id:         fmt.Sprintf("limited_task_%d", i),
			priority:   5,
			maxRetries: 3,
			provider:   provider,
			timeout:    500 * time.Millisecond,
			done:       make(chan struct{}),
		}
		
		wg.Add(1)
		go func() {
			defer wg.Done()
			AddTask(task, &logger)
		}()
	}

	wg.Wait()
	
	// Wait for processing
	time.Sleep(2 * time.Second)
	
	finalProcessed := atomic.LoadInt32(&processed)
	t.Logf("Concurrency limit test: Tasks=%d, Processed=%d", numTasks, finalProcessed)
	
	// With 2 servers, 1 concurrent each, 50ms per task, in 2 seconds we can process ~40 tasks
	// But we only have 20 tasks, so all should be processed
	if finalProcessed < numTasks {
		t.Errorf("Not all tasks processed: %d/%d", finalProcessed, numTasks)
	}
}