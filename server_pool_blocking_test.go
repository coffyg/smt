package smt

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestServerPoolBlocking reproduces the cascade lag bug where returnServerToPool() blocks
// when the availableServers channel fills up, causing gradual system slowdown.
func TestServerPoolBlocking(t *testing.T) {
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	// Setup: Create provider with only 1 server (so channel capacity is 2)
	providerName := "lagProvider"
	provider := &MockProvider{name: providerName}
	providers := []IProvider{provider}

	servers := map[string][]string{
		providerName: {"server1"}, // Only 1 server = channel capacity 2
	}

	getTimeout := func(string, string) time.Duration {
		return time.Second * 30 // Long timeout to see blocking behavior
	}

	// Initialize TaskManager
	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()
	
	// Set higher concurrency to test pool blocking behavior
	// (default is 1, but this test needs concurrent tasks to trigger blocking)
	tm.SetTaskManagerServerMaxParallel("server1", 5)

	// Test strategy:
	// 1. Create tasks that take 10+ seconds (holding servers busy)
	// 2. Create enough concurrent tasks to overflow the server return channel
	// 3. Monitor how long tasks take to complete as the system gets congested

	var blockedReturnCount int32
	var completedTasks int32
	var taskStartTimes sync.Map
	taskCompletionTimes := make([]time.Duration, 0, 50)
	var completionMutex sync.Mutex

	// Mock provider that simulates slow AI processing
	provider.handleFunc = func(task ITask, server string) error {
		taskID := task.GetID()
		startTime := time.Now()
		taskStartTimes.Store(taskID, startTime)

		// Simulate long-running AI task (1-3 seconds) - reduced for test speed
		processingTime := time.Duration(1+len(taskID)%3) * time.Second
		time.Sleep(processingTime)

		// Track completion time
		elapsed := time.Since(startTime)
		completionMutex.Lock()
		taskCompletionTimes = append(taskCompletionTimes, elapsed)
		completionMutex.Unlock()

		atomic.AddInt32(&completedTasks, 1)
		return nil
	}

	// Phase 1: Create baseline tasks to establish normal performance
	t.Log("Phase 1: Creating baseline tasks...")
	baselineTasks := 3
	for i := 0; i < baselineTasks; i++ {
		task := &MockTask{
			id:         fmt.Sprintf("baseline_%d", i),
			priority:   1,
			maxRetries: 1,
			provider:   provider,
			timeout:    time.Second * 30,
			done:       make(chan struct{}),
		}
		tm.AddTask(task)
	}

	// Wait for baseline tasks to start
	time.Sleep(time.Second * 2)

	// Phase 2: Flood with many tasks to trigger server pool overflow
	t.Log("Phase 2: Flooding with tasks to trigger server pool blocking...")
	floodTasks := 10 // Reduced for faster test completion
	var floodTaskChannels []chan struct{}

	for i := 0; i < floodTasks; i++ {
		done := make(chan struct{})
		floodTaskChannels = append(floodTaskChannels, done)

		task := &MockTask{
			id:         fmt.Sprintf("flood_%d", i),
			priority:   1,
			maxRetries: 1,
			provider:   provider,
			timeout:    time.Second * 30,
			done:       done,
		}
		tm.AddTask(task)

		// Add small delay to stagger task creation
		time.Sleep(time.Millisecond * 50)
	}

	// Phase 3: Monitor system behavior during congestion
	t.Log("Phase 3: Monitoring system behavior...")
	
	// Wait for some tasks to complete (this is where blocking should occur)
	timeoutCh := time.After(time.Minute * 2)
	completionTarget := int32(baselineTasks + 5) // Wait for baseline + some flood tasks

	for {
		completed := atomic.LoadInt32(&completedTasks)
		blocked := atomic.LoadInt32(&blockedReturnCount)
		
		if completed >= completionTarget {
			t.Logf("Reached completion target. Completed: %d, Blocked returns: %d", completed, blocked)
			break
		}

		select {
		case <-timeoutCh:
			t.Logf("Test timeout. Completed: %d, Blocked returns: %d", completed, blocked)
			goto Analysis
		default:
			time.Sleep(time.Second)
		}
	}

Analysis:
	// Analyze results
	completionMutex.Lock()
	defer completionMutex.Unlock()

	if len(taskCompletionTimes) == 0 {
		t.Fatal("No tasks completed - possible deadlock!")
	}

	// Calculate average completion times
	var early, late time.Duration
	splitPoint := len(taskCompletionTimes) / 2

	for i, duration := range taskCompletionTimes {
		if i < splitPoint {
			early += duration
		} else {
			late += duration
		}
	}

	if splitPoint > 0 {
		early /= time.Duration(splitPoint)
		late /= time.Duration(len(taskCompletionTimes) - splitPoint)
	}

	t.Logf("Task completion analysis:")
	t.Logf("  Total tasks completed: %d", len(taskCompletionTimes))
	t.Logf("  Early tasks avg time: %v", early)
	t.Logf("  Later tasks avg time: %v", late)
	t.Logf("  Slowdown factor: %.2fx", float64(late)/float64(early))

	// Check for cascade lag symptoms
	slowdownFactor := float64(late) / float64(early)
	if slowdownFactor > 2.0 {
		t.Logf("⚠️  DETECTED CASCADE LAG: Later tasks took %.2fx longer than early tasks", slowdownFactor)
		t.Logf("This suggests server pool blocking is occurring!")
	} else {
		t.Logf("✅ No significant cascade lag detected (%.2fx slowdown)", slowdownFactor)
	}

	// Log system state for debugging
	completed := atomic.LoadInt32(&completedTasks)
	t.Logf("Final state: %d tasks completed", completed)

	if completed < int32(baselineTasks) {
		t.Error("System appears to be deadlocked - baseline tasks didn't complete")
	}
}

// TestServerPoolBlockingWithMonitoring adds instrumentation to detect blocking
func TestServerPoolBlockingWithMonitoring(t *testing.T) {
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	providerName := "monitoredProvider"
	provider := &MockProvider{name: providerName}
	providers := []IProvider{provider}

	servers := map[string][]string{
		providerName: {"server1"}, // Minimal servers for maximum blocking chance
	}

	getTimeout := func(string, string) time.Duration {
		return time.Second * 30
	}

	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()
	
	// Set higher concurrency for testing
	tm.SetTaskManagerServerMaxParallel("server1", 5)

	// Monitor the provider's server channel status
	pd := tm.providers[providerName]
	
	// Goroutine to monitor channel fullness
	channelMonitorStop := make(chan struct{})
	channelMonitorDone := make(chan struct{})
	var maxChannelUsage int
	go func() {
		defer close(channelMonitorDone)
		ticker := time.NewTicker(time.Millisecond * 100)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				usage := len(pd.availableServers)
				if usage > maxChannelUsage {
					maxChannelUsage = usage
				}
				if usage >= cap(pd.availableServers) {
					t.Logf("🚨 SERVER CHANNEL FULL! Usage: %d/%d", usage, cap(pd.availableServers))
				}
			case <-channelMonitorStop:
				return
			}
		}
	}()

	var taskCompletions int32
	provider.handleFunc = func(task ITask, server string) error {
		// Simulate work that could cause server return blocking
		time.Sleep(time.Second * 3)
		atomic.AddInt32(&taskCompletions, 1)
		return nil
	}

	// Create enough tasks to potentially fill the server channel
	numTasks := 10
	for i := 0; i < numTasks; i++ {
		task := &MockTask{
			id:         fmt.Sprintf("monitored_%d", i),
			priority:   1,
			maxRetries: 1,
			provider:   provider,
			timeout:    time.Second * 30,
		}
		tm.AddTask(task)
	}

	// Wait for completion or timeout
	timeout := time.After(time.Minute)
	for {
		completed := atomic.LoadInt32(&taskCompletions)
		if completed >= int32(numTasks) {
			break
		}

		select {
		case <-timeout:
			t.Logf("Timeout reached. Completed: %d/%d tasks", completed, numTasks)
			goto Cleanup
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}

Cleanup:
	close(channelMonitorStop)
	<-channelMonitorDone

	t.Logf("Test results:")
	t.Logf("  Max server channel usage: %d/%d", maxChannelUsage, cap(pd.availableServers))
	t.Logf("  Tasks completed: %d/%d", atomic.LoadInt32(&taskCompletions), numTasks)

	if maxChannelUsage >= cap(pd.availableServers) {
		t.Logf("✅ Successfully reproduced server channel saturation!")
		t.Logf("   This confirms the blocking scenario can occur in production.")
	} else {
		t.Logf("ℹ️  Server channel didn't reach full capacity in this test run.")
	}
}

// TestExtremeLagCascade simulates extreme lag conditions that might trigger cascade failures
func TestExtremeLagCascade(t *testing.T) {
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	providerName := "extremeLagProvider"
	provider := &MockProvider{name: providerName}
	providers := []IProvider{provider}

	// Multiple servers but still limited to force contention
	servers := map[string][]string{
		providerName: {"server1", "server2"}, // 2 servers = channel capacity 4
	}

	getTimeout := func(string, string) time.Duration {
		return time.Minute * 2 // Very long timeout to see extreme behavior
	}

	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()
	
	// Set higher concurrency for testing
	tm.SetTaskManagerServerMaxParallel("server1", 10)
	tm.SetTaskManagerServerMaxParallel("server2", 10)

	var taskStarted int32
	var taskCompleted int32
	var totalProcessingTime int64
	var extremeLagDetected int32

	// Simulate increasingly laggy provider that gets worse over time
	provider.handleFunc = func(task ITask, server string) error {
		started := atomic.AddInt32(&taskStarted, 1)
		
		// Escalating lag: later tasks take exponentially longer
		// First few tasks: 1-2 seconds
		// Middle tasks: 5-10 seconds  
		// Later tasks: 15-30 seconds (extreme lag)
		var processingTime time.Duration
		if started <= 5 {
			processingTime = time.Second * time.Duration(1+started%2)
		} else if started <= 15 {
			processingTime = time.Second * time.Duration(5+(started-5)%5)
		} else {
			// Extreme lag phase
			processingTime = time.Second * time.Duration(15+(started-15)*2)
			atomic.StoreInt32(&extremeLagDetected, 1)
		}

		t.Logf("Task %s starting (task #%d) - will take %v", task.GetID(), started, processingTime)
		
		startTime := time.Now()
		time.Sleep(processingTime)
		elapsed := time.Since(startTime)
		
		atomic.AddInt64(&totalProcessingTime, elapsed.Milliseconds())
		completed := atomic.AddInt32(&taskCompleted, 1)
		
		t.Logf("Task %s completed (task #%d) - actual time: %v", task.GetID(), completed, elapsed)
		return nil
	}

	// Create many tasks to trigger the escalating lag
	numTasks := 25
	taskChannels := make([]chan struct{}, numTasks)
	
	for i := 0; i < numTasks; i++ {
		done := make(chan struct{})
		taskChannels[i] = done
		
		task := &MockTask{
			id:         fmt.Sprintf("extreme_%d", i),
			priority:   1,
			maxRetries: 1,
			provider:   provider,
			timeout:    time.Minute * 2,
			done:       done,
		}
		tm.AddTask(task)
		
		// Small delay between task submissions
		time.Sleep(time.Millisecond * 20)
	}

	// Monitor the cascade effect with timeout
	monitorTimeout := time.After(time.Minute * 3)
	checkInterval := time.NewTicker(time.Second * 5)
	defer checkInterval.Stop()

	for {
		completed := atomic.LoadInt32(&taskCompleted)
		started := atomic.LoadInt32(&taskStarted)
		
		select {
		case <-monitorTimeout:
			t.Logf("Monitor timeout reached. Started: %d, Completed: %d", started, completed)
			goto Analysis
		case <-checkInterval.C:
			t.Logf("Progress: Started %d, Completed %d tasks", started, completed)
			
			// If we've completed enough tasks to see the pattern, analyze
			if completed >= 15 {
				goto Analysis
			}
		}
	}

Analysis:
	started := atomic.LoadInt32(&taskStarted)
	completed := atomic.LoadInt32(&taskCompleted)
	extremeLag := atomic.LoadInt32(&extremeLagDetected)
	
	t.Logf("Extreme lag test results:")
	t.Logf("  Tasks started: %d", started)
	t.Logf("  Tasks completed: %d", completed)
	t.Logf("  Extreme lag phase reached: %v", extremeLag == 1)
	
	if completed > 0 {
		avgTime := time.Duration(atomic.LoadInt64(&totalProcessingTime)/int64(completed)) * time.Millisecond
		t.Logf("  Average processing time: %v", avgTime)
	}

	// Check for cascade failure symptoms
	if started > completed+3 {
		t.Logf("⚠️  POTENTIAL CASCADE FAILURE: %d tasks started but only %d completed", started, completed)
		t.Logf("   This suggests tasks are getting stuck or blocked!")
	}
	
	if extremeLag == 1 {
		t.Logf("🐌 Extreme lag conditions successfully simulated")
		if started > completed+5 {
			t.Logf("🚨 SEVERE CASCADE DETECTED: System appears to be choking on extreme lag")
		}
	}

	// Don't fail the test - just report what we observed
	t.Logf("Test completed successfully (reporting cascade behavior)")
}

// TestCommandTaskInterference tests if slow commands block task processing
func TestCommandTaskInterference(t *testing.T) {
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	providerName := "interferenceProvider"
	provider := &MockProvider{name: providerName}
	providers := []IProvider{provider}

	servers := map[string][]string{
		providerName: {"server1"},
	}

	getTimeout := func(string, string) time.Duration {
		return time.Second * 30
	}

	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()
	
	// Set higher concurrency for testing
	tm.SetTaskManagerServerMaxParallel("server1", 10)

	var taskProcessed int32
	var commandProcessed int32

	provider.handleFunc = func(task ITask, server string) error {
		time.Sleep(time.Second * 2)
		atomic.AddInt32(&taskProcessed, 1)
		return nil
	}

	// Add normal tasks first
	for i := 0; i < 5; i++ {
		task := &MockTask{
			id:         fmt.Sprintf("task_%d", i),
			priority:   1,
			maxRetries: 1,
			provider:   provider,
			timeout:    time.Second * 30,
		}
		tm.AddTask(task)
	}

	// Then add slow commands that could block task processing
	for i := 0; i < 3; i++ {
		tm.ExecuteCommand(providerName, func(server string) error {
			t.Logf("Command %d starting - taking 10 seconds", i)
			time.Sleep(time.Second * 10) // Slow commands
			atomic.AddInt32(&commandProcessed, 1)
			t.Logf("Command %d completed", i)
			return nil
		})
	}

	// Wait and monitor interference
	time.Sleep(time.Second * 45)

	tasks := atomic.LoadInt32(&taskProcessed)
	commands := atomic.LoadInt32(&commandProcessed)

	t.Logf("Command interference test results:")
	t.Logf("  Tasks processed: %d/5", tasks)
	t.Logf("  Commands processed: %d/3", commands)

	if tasks < 5 && commands > 0 {
		t.Logf("⚠️  COMMAND INTERFERENCE: Commands may be blocking task processing")
	} else {
		t.Logf("✅ No command interference detected")
	}
}

// TestRetryStorm tests exponential load from failing API retries
func TestRetryStorm(t *testing.T) {
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	providerName := "retryProvider"
	provider := &MockProvider{name: providerName}
	providers := []IProvider{provider}

	servers := map[string][]string{
		providerName: {"server1", "server2"},
	}

	getTimeout := func(string, string) time.Duration {
		return time.Second * 10
	}

	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()
	
	// Set higher concurrency for testing
	tm.SetTaskManagerServerMaxParallel("server1", 10)
	tm.SetTaskManagerServerMaxParallel("server2", 10)

	var attemptCount int32
	var successCount int32

	// Simulate failing API that triggers retries
	provider.handleFunc = func(task ITask, server string) error {
		attempts := atomic.AddInt32(&attemptCount, 1)
		t.Logf("Attempt #%d for task %s", attempts, task.GetID())
		
		// Fail most attempts to trigger retries
		if attempts%4 != 0 {
			return fmt.Errorf("simulated API failure #%d", attempts)
		}
		
		atomic.AddInt32(&successCount, 1)
		return nil
	}

	// Create tasks that will retry on failure
	numTasks := 10
	for i := 0; i < numTasks; i++ {
		task := &MockTask{
			id:         fmt.Sprintf("retry_%d", i),
			priority:   1,
			maxRetries: 5, // Allow multiple retries
			provider:   provider,
			timeout:    time.Second * 10,
		}
		tm.AddTask(task)
	}

	// Monitor retry storm
	time.Sleep(time.Second * 30)

	attempts := atomic.LoadInt32(&attemptCount)
	successes := atomic.LoadInt32(&successCount)

	t.Logf("Retry storm test results:")
	t.Logf("  Total attempts: %d", attempts)
	t.Logf("  Successful tasks: %d", successes)
	t.Logf("  Retry multiplier: %.1fx", float64(attempts)/float64(numTasks))

	if attempts > int32(numTasks*3) {
		t.Logf("⚠️  RETRY STORM DETECTED: %d attempts for %d tasks", attempts, numTasks)
	} else {
		t.Logf("✅ Retry behavior appears normal")
	}
}

// TestContextLeak tests for goroutine/context leaks under load
func TestContextLeak(t *testing.T) {
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	providerName := "contextProvider"
	provider := &MockProvider{name: providerName}
	providers := []IProvider{provider}

	servers := map[string][]string{
		providerName: {"server1"},
	}

	getTimeout := func(string, string) time.Duration {
		return time.Second * 5
	}

	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()
	
	// Set higher concurrency for testing
	tm.SetTaskManagerServerMaxParallel("server1", 10)

	var taskStarted int32
	var taskCompleted int32

	provider.handleFunc = func(task ITask, server string) error {
		atomic.AddInt32(&taskStarted, 1)
		// Simulate varying processing times to stress context handling
		time.Sleep(time.Millisecond * time.Duration(100+len(task.GetID())*50))
		atomic.AddInt32(&taskCompleted, 1)
		return nil
	}

	// Get initial goroutine count
	initialGoroutines := runtime.NumGoroutine()

	// Create many short-lived tasks to test context cleanup
	numTasks := 50
	for i := 0; i < numTasks; i++ {
		task := &MockTask{
			id:         fmt.Sprintf("ctx_%d", i),
			priority:   1,
			maxRetries: 1,
			provider:   provider,
			timeout:    time.Second * 5,
		}
		tm.AddTask(task)
		
		if i%10 == 0 {
			time.Sleep(time.Millisecond * 50) // Small batching
		}
	}

	// Wait for tasks to complete
	timeout := time.After(time.Second * 30)
	for {
		completed := atomic.LoadInt32(&taskCompleted)
		if completed >= int32(numTasks) {
			break
		}
		
		select {
		case <-timeout:
			t.Logf("Timeout waiting for tasks. Completed: %d/%d", completed, numTasks)
			break
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}

	// Allow some time for cleanup
	time.Sleep(time.Second * 2)

	finalGoroutines := runtime.NumGoroutine()
	started := atomic.LoadInt32(&taskStarted)
	completed := atomic.LoadInt32(&taskCompleted)
	goroutineIncrease := finalGoroutines - initialGoroutines

	t.Logf("Context leak test results:")
	t.Logf("  Tasks started: %d", started)
	t.Logf("  Tasks completed: %d", completed)
	t.Logf("  Initial goroutines: %d", initialGoroutines)
	t.Logf("  Final goroutines: %d", finalGoroutines)
	t.Logf("  Goroutine increase: %d", goroutineIncrease)

	if goroutineIncrease > 10 {
		t.Logf("⚠️  POTENTIAL CONTEXT LEAK: %d extra goroutines remain", goroutineIncrease)
	} else {
		t.Logf("✅ No significant goroutine leak detected")
	}
}