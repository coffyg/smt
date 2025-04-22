package smt

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Using ThreadSafeLogBuffer from test_utils.go

// MockProviderForVerification implements IProvider interface
type MockProviderForVerification struct {
	providerName    string
	assignments     *sync.Map  // Maps taskID to server
	returns         *sync.Map  // Maps taskID to server
}

func (p *MockProviderForVerification) Name() string {
	return p.providerName
}

func (p *MockProviderForVerification) Handle(task ITask, server string) error {
	// Track which server was assigned to this task
	taskID := task.GetID()
	p.assignments.Store(taskID, server)

	// Simulate work
	time.Sleep(time.Millisecond * 10)

	// Record that this server was properly returned
	p.returns.Store(taskID, server)
	return nil
}

// MockTaskForVerification implements ITask interface
type MockTaskForVerification struct {
	id            string
	provider      IProvider
	done          chan struct{}
	successCount  *atomic.Int32
}

func (t *MockTaskForVerification) GetID() string              { return t.id }
func (t *MockTaskForVerification) GetProvider() IProvider     { return t.provider }
func (t *MockTaskForVerification) GetPriority() int           { return 1 }
func (t *MockTaskForVerification) GetMaxRetries() int         { return 3 }
func (t *MockTaskForVerification) GetRetries() int            { return 0 }
func (t *MockTaskForVerification) GetCreatedAt() time.Time    { return time.Now().Add(-time.Minute) }
func (t *MockTaskForVerification) GetTaskGroup() ITaskGroup   { return nil }
func (t *MockTaskForVerification) GetTimeout() time.Duration  { return time.Second }
func (t *MockTaskForVerification) GetCallbackName() string    { return "verify" }
func (t *MockTaskForVerification) UpdateRetries(r int) error  { return nil }
func (t *MockTaskForVerification) UpdateLastError(s string) error { return nil }

func (t *MockTaskForVerification) MarkAsSuccess(timeMs int64) {
	if t.successCount != nil {
		t.successCount.Add(1)
	}
}

func (t *MockTaskForVerification) MarkAsFailed(timeMs int64, err error) {
	// No-op for this test
}

func (t *MockTaskForVerification) OnComplete() {
	select {
	case t.done <- struct{}{}:
	default:
		// Channel might be full
	}
}

func (t *MockTaskForVerification) OnStart() {
	// No-op for this test
}

// TestServerVerification tests that servers are properly returned
// when tasks are completed successfully, even under high concurrency.
// This test is specifically designed to be race-safe for running with -race.
func TestServerVerification(t *testing.T) {
	// Create thread-safe tracking of server assignments and returns
	var serverAssignments sync.Map
	var serverReturns sync.Map
	var successCount atomic.Int32

	// Create mock providers with tracking maps
	provider1 := &MockProviderForVerification{
		providerName: "verify_provider1",
		assignments:  &serverAssignments,
		returns:      &serverReturns,
	}
	
	provider2 := &MockProviderForVerification{
		providerName: "verify_provider2",
		assignments:  &serverAssignments,
		returns:      &serverReturns,
	}

	// Setup logger with thread-safe output buffer for race detection
	var logBuffer ThreadSafeLogBuffer
	writer := zerolog.ConsoleWriter{
		Out:     &logBuffer,
		NoColor: true,
	}
	logger := zerolog.New(writer).Level(zerolog.WarnLevel)

	// Create provider list
	providers := []IProvider{provider1, provider2}

	// Define servers for each provider
	servers := map[string][]string{
		"verify_provider1": {"server1", "server2"},
		"verify_provider2": {"server3", "server4"},
	}

	// Define timeout function
	timeoutFn := func(callbackName string, providerName string) time.Duration {
		return time.Second * 1
	}

	// Initialize task manager
	tm := NewTaskManagerWithOptions(&providers, servers, &logger, timeoutFn, &TaskManagerOptions{
		EnablePooling:  true,
		EnableTwoLevel: true,
	})

	// Start the task manager
	tm.Start()
	defer tm.Shutdown()

	// Create and submit tasks
	totalTasks := 100 // A reasonable but nontrivial number of tasks
	
	var wg sync.WaitGroup
	wg.Add(totalTasks)
	
	// Create and submit tasks
	for i := 0; i < totalTasks; i++ {
		// Determine provider based on task index
		var provider IProvider
		if i%2 == 0 {
			provider = provider1
		} else {
			provider = provider2
		}
		
		task := &MockTaskForVerification{
			id:           "verify_task_" + string(rune('A'+i%26)) + string(rune('0'+i%10)),
			provider:     provider,
			done:         make(chan struct{}, 1),
			successCount: &successCount,
		}
		
		// Submit task
		tm.AddTask(task)
		
		// Wait for task completion in a separate goroutine
		go func(t *MockTaskForVerification) {
			defer wg.Done()
			select {
			case <-t.done:
				// Task completed
			case <-time.After(time.Second * 10):
				// Timeout as a safety
				t.MarkAsFailed(0, nil)
			}
		}(task)
	}
	
	// Wait for all tasks to complete
	wg.Wait()
	
	// Verify results
	successes := int(successCount.Load())
	t.Logf("Completed %d tasks successfully out of %d", successes, totalTasks)
	
	// Check that all successful tasks returned their servers properly
	verified := 0
	serverAssignments.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		assignedServer := value.(string)
		
		// Check if this server was properly returned
		returnedServer, ok := serverReturns.Load(taskID)
		if !ok {
			t.Errorf("Task %s did not return its server", taskID)
		} else if returnedServer != assignedServer {
			t.Errorf("Task %s was assigned server %s but returned server %s", 
				taskID, assignedServer, returnedServer)
		} else {
			verified++
		}
		
		return true
	})
	
	t.Logf("Verified server returns: %d out of %d successes", verified, successes)
	
	// Success criteria: all tasks completed and all servers were returned
	if verified != successes {
		t.Errorf("Not all servers were properly returned: %d verified vs %d successes", 
			verified, successes)
	}
	
	// Verify that channel capacity is double the server count
	for providerName, providerData := range tm.providers {
		servers := providerData.servers
		capacity := cap(providerData.availableServers)
		
		if len(servers)*2 != capacity {
			t.Errorf("Server channel capacity for %s is not double the server count: %d servers, capacity %d",
				providerName, len(servers), capacity)
		}
	}
}