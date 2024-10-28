package smt

import (
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Test TaskManagerSimple in a harsh multi-user environment
func TestTaskManagerSimple_HarshEnvironment(t *testing.T) {
	// Setup logger
	logger := log.Output(zerolog.ConsoleWriter{Out: zerolog.NewConsoleWriter().Out})

	// Define providers
	providerNames := []string{"provider1", "provider2", "provider3", "provider4", "provider5"}
	var providers []IProvider
	providerHandleFuncs := make(map[string]func(task ITask, server string) error)

	for _, name := range providerNames {
		provider := &MockProvider{name: name}
		providers = append(providers, provider)

		// Assign a handleFunc for each provider to simulate processing time and errors
		providerHandleFuncs[name] = func(task ITask, server string) error {
			taskNum, _ := strconv.Atoi(task.GetID()[4:])                // Extract numeric part from "task123"
			time.Sleep(time.Millisecond * time.Duration(20+taskNum%50)) // Simulate processing time
			// Simulate occasional errors
			if taskNum%17 == 0 {
				return errors.New("simulated error")
			}
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

	// Define getTimeout function
	getTimeout := func(providerName string) time.Duration {
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

				AddTask(task, &logger)

				// Wait for task to complete
				go func(task *MockTask) {
					<-task.done
					taskStatusMutex.Lock()
					defer taskStatusMutex.Unlock()
					if task.failed {
						taskFailed <- task.GetID()
						taskStatus[task.GetID()] = "failed"
					} else if task.success {
						taskProcessed <- task.GetID()
						taskStatus[task.GetID()] = "processed"
					}
				}(task)
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

	// Shutdown TaskManager
	TaskQueueManagerInstance.Shutdown()
}

// Tests for AddTask with nil provider
func TestAddTaskWithNilProvider(t *testing.T) {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()

	// Initialize TaskManagerSimple with no providers for simplicity
	providers := []IProvider{}
	servers := map[string][]string{}
	getTimeout := func(string) time.Duration { return time.Second * 10 }
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

	if task.lastError != "task 'task_with_nil_provider' has no provider" {
		t.Errorf("Unexpected error message: %s", task.lastError)
	}

	if !task.completeCalled {
		t.Error("Expected OnComplete to be called for task with nil provider")
	}
}
