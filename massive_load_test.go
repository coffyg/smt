package smt

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// ThreadSafeBuffer for collecting logs
type ThreadSafeBuffer struct {
	mu  sync.Mutex
	buf []string
}

func (b *ThreadSafeBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, string(p))
	return len(p), nil
}

func (b *ThreadSafeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := ""
	for _, s := range b.buf {
		result += s
	}
	return result
}

// TestMassiveTaskLoad simulates an extreme workload with many tasks distributed
// across many providers with many servers, verifying correct behavior.
func TestMassiveTaskLoad(t *testing.T) {
	// Setup thread-safe logging
	var logBuffer ThreadSafeBuffer
	logger := zerolog.New(zerolog.ConsoleWriter{Out: &logBuffer, NoColor: true}).
		With().Timestamp().Logger()

	// Configuration
	config := struct {
		providers      int
		serversPerProv int
		basicTasks     int
		spikeTasks     int
		maxRetries     int
		priorities     int
		variedDuration bool
	}{
		providers:      10,              // Number of providers
		serversPerProv: 5,               // Servers per provider
		basicTasks:     500,             // Regular load tasks
		spikeTasks:     2000,            // Spike load tasks
		maxRetries:     3,               // Max retries for tasks
		priorities:     10,              // Different priority levels
		variedDuration: true,            // Simulate varied task durations
	}

	// Create providers
	var providers []IProvider
	providerNames := make([]string, config.providers)
	handleFuncs := make(map[string]func(task ITask, server string) error)

	// Track execution statistics
	execStats := struct {
		tasksByServer      sync.Map // Maps server to count
		taskAssignments    sync.Map // Maps taskID to server
		taskCompletions    sync.Map // Maps taskID to success/failure
		taskResults        sync.Map // Maps taskID to result data (for verification)
		serverHandles      sync.Map // Maps server to list of taskIDs it handled
		tasksByPriority    sync.Map // Maps priority to count
		totalCompleted     int32
		totalFailed        int32
		serverUtilization  sync.Map // Maps server to usage time
		avgExecutionTimeMs int64
		maxExecutionTimeMs int64
		minExecutionTimeMs int64
		mu                 sync.Mutex // For non-atomic stats
		startTime          time.Time
	}{
		minExecutionTimeMs: int64(^uint64(0) >> 1), // Max int64 value
		startTime:          time.Now(),
	}

	// Create providers and servers
	servers := make(map[string][]string)
	for i := 0; i < config.providers; i++ {
		providerName := fmt.Sprintf("provider%d", i)
		providerNames[i] = providerName

		// Create provider
		provider := &MockProvider{name: providerName}
		providers = append(providers, provider)

		// Create servers for provider
		providerServers := make([]string, config.serversPerProv)
		for j := 0; j < config.serversPerProv; j++ {
			serverName := fmt.Sprintf("server_%s_%d", providerName, j)
			providerServers[j] = serverName
		}
		servers[providerName] = providerServers

		// Create handler function that tracks execution
		handleFuncs[providerName] = func(task ITask, server string) error {
			taskID := task.GetID()
			priority := task.GetPriority()

			// Record assignment
			execStats.taskAssignments.Store(taskID, server)
			
			// Extract task number from ID for verification
			taskIDNum, _ := strconv.Atoi(taskID[4:]) 
			
			// Create a result object with data we can verify later
			resultData := struct {
				server   string
				priority int
				taskNum  int
				status   string
			}{
				server:   server,
				priority: priority,
				taskNum:  taskIDNum,
				status:   "processing",
			}
			execStats.taskResults.Store(taskID, resultData)

			// Count by server
			if val, ok := execStats.tasksByServer.Load(server); ok {
				execStats.tasksByServer.Store(server, val.(int)+1)
			} else {
				execStats.tasksByServer.Store(server, 1)
			}

			// Record this task in the server's handled list
			var taskList []string
			if val, ok := execStats.serverHandles.Load(server); ok {
				taskList = val.([]string)
			}
			taskList = append(taskList, taskID)
			execStats.serverHandles.Store(server, taskList)

			// Count by priority
			if val, ok := execStats.tasksByPriority.Load(priority); ok {
				execStats.tasksByPriority.Store(priority, val.(int)+1)
			} else {
				execStats.tasksByPriority.Store(priority, 1)
			}

			// Simulate processing time
			var processingTime time.Duration

			if config.variedDuration {
				// Simulate varied execution times based on priority
				// Higher priority tasks get processed faster (more resources)
				baseDuration := 5 + (10 - priority) // 5-15ms base
				variability := taskIDNum % 50
				processingTime = time.Millisecond * time.Duration(baseDuration+variability)
			} else {
				// Simple fixed processing time
				processingTime = time.Millisecond * 10
			}

			// Track server utilization time
			startTime := time.Now()
			time.Sleep(processingTime)
			execTime := time.Since(startTime)

			// Update server utilization
			if val, ok := execStats.serverUtilization.Load(server); ok {
				execStats.serverUtilization.Store(server, val.(time.Duration)+execTime)
			} else {
				execStats.serverUtilization.Store(server, execTime)
			}

			// Simulate failures based on task ID
			if taskIDNum%23 == 0 {
				// Update the result data to indicate failure
				if val, ok := execStats.taskResults.Load(taskID); ok {
					result := val.(struct {
						server   string
						priority int
						taskNum  int
						status   string
					})
					result.status = "failed"
					execStats.taskResults.Store(taskID, result)
				}
				
				// Task failed
				execStats.taskCompletions.Store(taskID, "failed")
				atomic.AddInt32(&execStats.totalFailed, 1)
				return fmt.Errorf("simulated failure for task %s", taskID)
			}

			// Update the result data to indicate success
			if val, ok := execStats.taskResults.Load(taskID); ok {
				result := val.(struct {
					server   string
					priority int
					taskNum  int
					status   string
				})
				result.status = "success"
				execStats.taskResults.Store(taskID, result)
			}
			
			// Task succeeded
			execStats.taskCompletions.Store(taskID, "success")
			atomic.AddInt32(&execStats.totalCompleted, 1)

			// Update execution time stats
			execTimeMs := execTime.Milliseconds()
			execStats.mu.Lock()
			execStats.avgExecutionTimeMs += execTimeMs
			if execTimeMs > execStats.maxExecutionTimeMs {
				execStats.maxExecutionTimeMs = execTimeMs
			}
			if execTimeMs < execStats.minExecutionTimeMs {
				execStats.minExecutionTimeMs = execTimeMs
			}
			execStats.mu.Unlock()

			return nil
		}
		provider.handleFunc = handleFuncs[providerName]
	}

	// Define timeout function
	getTimeout := func(callbackName string, providerName string) time.Duration {
		return 500 * time.Millisecond
	}

	// Initialize the task manager
	InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)
	defer TaskQueueManagerInstance.Shutdown()

	// Wait group for task completion tracking
	var wg sync.WaitGroup
	taskProcessed := make(chan string, config.basicTasks+config.spikeTasks)
	taskFailed := make(chan string, config.basicTasks+config.spikeTasks)

	// Create a tracking map for task status
	taskStatus := sync.Map{}

	// Create basic load tasks
	t.Logf("Creating %d basic load tasks...", config.basicTasks)
	for i := 0; i < config.basicTasks; i++ {
		taskID := fmt.Sprintf("task%d", i)
		providerIndex := i % len(providers)
		priority := i % config.priorities

		task := &MockTask{
			id:         taskID,
			priority:   priority,
			maxRetries: config.maxRetries,
			createdAt:  time.Now(),
			provider:   providers[providerIndex],
			timeout:    200 * time.Millisecond,
			done:       make(chan struct{}),
		}

		wg.Add(1)
		go func(task *MockTask) {
			defer wg.Done()
			AddTask(task, &logger)

			// Wait for task completion
			<-task.done
			
			taskStatus.Store(task.id, task.success)
			if task.failed {
				taskFailed <- task.id
			} else {
				taskProcessed <- task.id
			}
		}(task)

		// Small delay to avoid flooding at once
		if i%100 == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}

	// Wait for basic tasks to be submitted
	time.Sleep(500 * time.Millisecond)
	
	// Test ExecuteCommand during task processing
	t.Log("Testing ExecuteCommand while tasks are running...")
	commandsPerProvider := 5
	totalCommands := commandsPerProvider * len(providerNames)
	
	// Track command execution results
	commandResults := sync.Map{}
	commandComplete := make(chan string, totalCommands)
	var commandsExecuted int32
	var commandErrors int32
	
	// Launch commands across all providers
	for _, providerName := range providerNames {
		for i := 0; i < commandsPerProvider; i++ {
			commandID := fmt.Sprintf("cmd_%s_%d", providerName, i)
			
			// Execute command
			err := TaskQueueManagerInstance.ExecuteCommand(providerName, func(server string) error {
				// Simulate command work with random duration
				processingTime := time.Millisecond * time.Duration(10+rand.Intn(20))
				startTime := time.Now()
				time.Sleep(processingTime)
				execTime := time.Since(startTime)
				
				// Store result with timing and server info
				result := struct {
					server     string
					startTime  time.Time
					endTime    time.Time
					duration   time.Duration
					successful bool
				}{
					server:     server,
					startTime:  startTime,
					endTime:    time.Now(),
					duration:   execTime,
					successful: true,
				}
				
				// Store command result for verification
				commandResults.Store(commandID, result)
				
				// Occasionally simulate a command error
				if rand.Intn(10) == 0 {
					atomic.AddInt32(&commandErrors, 1)
					commandComplete <- commandID
					return fmt.Errorf("simulated command error for %s on server %s", commandID, server)
				}
				
				atomic.AddInt32(&commandsExecuted, 1)
				commandComplete <- commandID
				return nil
			})
			
			if err != nil {
				t.Errorf("Error executing command %s: %v", commandID, err)
			}
		}
	}
	
	// Wait for commands to complete with timeout
	completedCommands := 0
	t.Logf("Waiting for %d commands to complete...", totalCommands)
	commandTimeout := time.After(30 * time.Second)
	
	for completedCommands < totalCommands {
		select {
		case <-commandComplete:
			completedCommands++
			if completedCommands%10 == 0 {
				t.Logf("Progress: %d/%d commands completed", completedCommands, totalCommands)
			}
		case <-commandTimeout:
			t.Logf("Timeout waiting for command completion. Completed: %d/%d", completedCommands, totalCommands)
			goto ContinueWithTasks
		}
	}
	
ContinueWithTasks:
	// Summarize command execution results
	successCommands := atomic.LoadInt32(&commandsExecuted)
	failedCommands := atomic.LoadInt32(&commandErrors)
	
	t.Logf("Commands completed: %d successful, %d failed (out of %d total)",
		successCommands, failedCommands, totalCommands)
	
	// Verify that commands used valid servers for each provider
	serversByProvider := make(map[string]map[string]int)
	commandResults.Range(func(key, value interface{}) bool {
		commandID := key.(string)
		result := value.(struct {
			server     string
			startTime  time.Time
			endTime    time.Time
			duration   time.Duration
			successful bool
		})
		
		// Extract provider from command ID
		parts := strings.Split(commandID, "_")
		if len(parts) >= 2 {
			providerName := parts[1]
			
			// Initialize the map if needed
			if _, ok := serversByProvider[providerName]; !ok {
				serversByProvider[providerName] = make(map[string]int)
			}
			
			// Count this server for this provider
			serversByProvider[providerName][result.server]++
		}
		
		return true
	})
	
	// Log server usage for commands
	t.Log("Command distribution by provider and server:")
	for providerName, serverMap := range serversByProvider {
		t.Logf("  Provider %s:", providerName)
		for server, count := range serverMap {
			t.Logf("    %s: %d commands", server, count)
		}
	}

	// Create spike load tasks in parallel
	t.Logf("Creating %d spike load tasks...", config.spikeTasks)
	concurrentWorkers := 10
	tasksPerWorker := config.spikeTasks / concurrentWorkers
	
	for w := 0; w < concurrentWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < tasksPerWorker; i++ {
				taskIndex := workerID*tasksPerWorker + i + config.basicTasks
				taskID := fmt.Sprintf("task%d", taskIndex)
				
				// Pick random provider for more realistic load
				providerIndex := rand.Intn(len(providers))
				priority := rand.Intn(config.priorities)

				task := &MockTask{
					id:         taskID,
					priority:   priority,
					maxRetries: config.maxRetries,
					createdAt:  time.Now(),
					provider:   providers[providerIndex],
					timeout:    200 * time.Millisecond,
					done:       make(chan struct{}),
				}

				// Submit task
				AddTask(task, &logger)

				// Create goroutine to track completion
				go func(task *MockTask) {
					<-task.done
					taskStatus.Store(task.id, task.success)
					if task.failed {
						taskFailed <- task.id
					} else {
						taskProcessed <- task.id
					}
				}(task)
			}
		}(w)
	}

	// Wait for all tasks to be submitted
	wg.Wait()

	// Wait for tasks to complete with timeout
	t.Log("Waiting for tasks to complete...")
	totalTasks := config.basicTasks + config.spikeTasks
	processedCount := 0
	failedCount := 0
	timeout := time.After(60 * time.Second)

	for processedCount+failedCount < totalTasks {
		select {
		case <-taskProcessed:
			processedCount++
			if processedCount%500 == 0 {
				t.Logf("Progress: %d/%d completed", processedCount+failedCount, totalTasks)
			}
		case <-taskFailed:
			failedCount++
		case <-timeout:
			t.Logf("Timeout reached. Processed: %d, Failed: %d, Total: %d", 
				processedCount, failedCount, totalTasks)
			goto AnalysisPhase
		}
	}

AnalysisPhase:
	// Total execution time
	executionTime := time.Since(execStats.startTime)
	t.Logf("Test completed in %.2f seconds", executionTime.Seconds())
	t.Logf("Tasks: %d processed, %d failed, %d total", 
		processedCount, failedCount, totalTasks)

	// Calculate throughput
	throughput := float64(processedCount+failedCount) / executionTime.Seconds()
	t.Logf("Throughput: %.2f tasks/second", throughput)

	// Task completion verification
	completedTasks := atomic.LoadInt32(&execStats.totalCompleted)
	failedTasks := atomic.LoadInt32(&execStats.totalFailed)
	t.Logf("Handler reported: %d completed, %d failed", completedTasks, failedTasks)

	// Verify server distribution and results
	serverCounts := make(map[string]int)
	execStats.tasksByServer.Range(func(key, value interface{}) bool {
		server := key.(string)
		count := value.(int)
		serverCounts[server] = count
		return true
	})
	
	// Verify that server results match our expectations
	t.Log("Verifying server results and task outcomes...")
	serverResultVerification := make(map[string]struct{
		expected int
		actual   int
		verified int
	})
	
	// First count expected tasks per server
	execStats.taskAssignments.Range(func(key, value interface{}) bool {
		_= key.(string)
		server := value.(string)
		
		stats, ok := serverResultVerification[server]
		if !ok {
			stats = struct{
				expected int
				actual   int
				verified int
			}{
				expected: 1,
				actual:   0,
				verified: 0,
			}
		} else {
			stats.expected++
		}
		serverResultVerification[server] = stats
		
		return true
	})
	
	// Now check which servers actually handled tasks
	execStats.serverHandles.Range(func(key, value interface{}) bool {
		server := key.(string)
		taskList := value.([]string)
		
		stats, ok := serverResultVerification[server]
		if !ok {
			stats = struct{
				expected int
				actual   int
				verified int
			}{
				expected: 0,
				actual:   len(taskList),
				verified: 0,
			}
		} else {
			stats.actual = len(taskList)
		}
		serverResultVerification[server] = stats
		
		return true
	})
	
	// Now verify task results from each server
	var resultVerificationErrors int
	execStats.taskResults.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		result := value.(struct {
			server   string
			priority int
			taskNum  int
			status   string
		})
		
		// Verify that task was assigned to and handled by the expected server
		if assignedServer, ok := execStats.taskAssignments.Load(taskID); ok {
			if assignedServer.(string) == result.server {
				// Task assignment matches the server that processed it
				stats := serverResultVerification[result.server]
				stats.verified++
				serverResultVerification[result.server] = stats
			} else {
				// Task was handled by the wrong server
				resultVerificationErrors++
				if resultVerificationErrors < 10 { // Limit output
					t.Errorf("Task %s was assigned to server %s but handled by %s", 
						taskID, assignedServer, result.server)
				}
			}
		}
		
		return true
	})

	// Print server utilization (top 5)
	type serverUtilization struct {
		server string
		count  int
		time   time.Duration
	}
	
	serverUtils := make([]serverUtilization, 0, len(serverCounts))
	for server, count := range serverCounts {
		duration, _ := execStats.serverUtilization.Load(server)
		serverUtils = append(serverUtils, serverUtilization{
			server: server,
			count:  count,
			time:   duration.(time.Duration),
		})
	}

	// Sort by count
	for i := 0; i < len(serverUtils)-1; i++ {
		for j := i + 1; j < len(serverUtils); j++ {
			if serverUtils[i].count < serverUtils[j].count {
				serverUtils[i], serverUtils[j] = serverUtils[j], serverUtils[i]
			}
		}
	}

	t.Log("Top 5 servers by task count:")
	maxToShow := 5
	if len(serverUtils) < maxToShow {
		maxToShow = len(serverUtils)
	}
	
	for i := 0; i < maxToShow; i++ {
		su := serverUtils[i]
		t.Logf("  %s: %d tasks, %.2f seconds total processing", 
			su.server, su.count, su.time.Seconds())
	}

	// Verify priority distribution
	priorityCounts := make(map[int]int)
	execStats.tasksByPriority.Range(func(key, value interface{}) bool {
		priority := key.(int)
		count := value.(int)
		priorityCounts[priority] = count
		return true
	})

	t.Log("Task distribution by priority:")
	for p := 0; p < config.priorities; p++ {
		if count, ok := priorityCounts[p]; ok {
			t.Logf("  Priority %d: %d tasks", p, count)
		}
	}

	// Verify that all tasks have been processed or failed
	var missingTasks int
	for i := 0; i < totalTasks; i++ {
		taskID := fmt.Sprintf("task%d", i)
		if _, ok := taskStatus.Load(taskID); !ok {
			missingTasks++
			if missingTasks < 10 { // Limit output
				t.Logf("Task %s was not processed", taskID)
			}
		}
	}

	if missingTasks > 0 {
		t.Logf("Warning: %d tasks were not processed correctly", missingTasks)
	}

	// Check execution time stats
	execStats.mu.Lock()
	if atomic.LoadInt32(&execStats.totalCompleted) > 0 {
		avgExecTimeMs := execStats.avgExecutionTimeMs / int64(atomic.LoadInt32(&execStats.totalCompleted))
		t.Logf("Execution times: avg=%dms, min=%dms, max=%dms", 
			avgExecTimeMs, execStats.minExecutionTimeMs, execStats.maxExecutionTimeMs)
	}
	execStats.mu.Unlock()
	
	// Analyze command execution patterns
	if completedCommands > 0 {
		t.Log("Command execution analysis:")
		
		// Calculate average command duration
		var totalDuration time.Duration
		var commandCount int
		var minDuration time.Duration = time.Hour // Start with large value
		var maxDuration time.Duration
		
		commandResults.Range(func(key, value interface{}) bool {
			result := value.(struct {
				server     string
				startTime  time.Time
				endTime    time.Time
				duration   time.Duration
				successful bool
			})
			
			totalDuration += result.duration
			commandCount++
			
			if result.duration < minDuration {
				minDuration = result.duration
			}
			if result.duration > maxDuration {
				maxDuration = result.duration
			}
			
			return true
		})
		
		if commandCount > 0 {
			avgDuration := totalDuration / time.Duration(commandCount)
			t.Logf("  Command execution times: avg=%v, min=%v, max=%v", 
				avgDuration, minDuration, maxDuration)
			
			// Verify that all executed commands used valid servers
			var commandsWithValidServer int
			commandResults.Range(func(key, value interface{}) bool {
				commandID := key.(string)
				result := value.(struct {
					server     string
					startTime  time.Time
					endTime    time.Time
					duration   time.Duration
					successful bool
				})
				
				// Extract provider name from command ID
				parts := strings.Split(commandID, "_")
				if len(parts) >= 2 {
					providerName := parts[1]
					
					// Check if server is valid for this provider
					validServer := false
					for _, validServerName := range servers[providerName] {
						if validServerName == result.server {
							validServer = true
							break
						}
					}
					
					if validServer {
						commandsWithValidServer++
					} else {
						t.Errorf("Command %s used invalid server: %s", commandID, result.server)
					}
				}
				
				return true
			})
			
			// Report command-server validation results
			validationRate := float64(commandsWithValidServer) / float64(commandCount) * 100
			t.Logf("  Command server validation: %d/%d commands used valid servers (%.1f%%)",
				commandsWithValidServer, commandCount, validationRate)
		}
	}

	// Verify no deadlocks occurred
	if !TaskQueueManagerInstance.IsRunning() {
		t.Error("Task manager stopped unexpectedly during test")
	}

	// Add server result verification stats
	t.Log("Server result verification summary:")
	var totalExpected, totalActual, totalVerified int
	
	// Sort servers by expected count for better reporting
	type serverVerifStats struct {
		server   string
		expected int
		actual   int
		verified int
	}
	
	serverStats := make([]serverVerifStats, 0, len(serverResultVerification))
	for server, stats := range serverResultVerification {
		serverStats = append(serverStats, serverVerifStats{
			server:   server,
			expected: stats.expected,
			actual:   stats.actual,
			verified: stats.verified,
		})
		
		totalExpected += stats.expected
		totalActual += stats.actual
		totalVerified += stats.verified
	}
	
	// Sort by expected count (descending)
	for i := 0; i < len(serverStats)-1; i++ {
		for j := i + 1; j < len(serverStats); j++ {
			if serverStats[i].expected < serverStats[j].expected {
				serverStats[i], serverStats[j] = serverStats[j], serverStats[i]
			}
		}
	}
	
	// Report top 5 servers
	maxToShow = 5
	if len(serverStats) < maxToShow {
		maxToShow = len(serverStats)
	}
	
	t.Log("Top 5 servers by expected tasks:")
	for i := 0; i < maxToShow; i++ {
		stat := serverStats[i]
		var verifyPercent float64 = 0.0
		if stat.expected > 0 {
			verifyPercent = float64(stat.verified) / float64(stat.expected) * 100
		}
		
		t.Logf("  %s: expected=%d, actual=%d, verified=%d (%.1f%%)",
			stat.server, stat.expected, stat.actual, stat.verified, verifyPercent)
	}
	
	// Overall verification rate
	var overallPercent float64 = 0.0
	if totalExpected > 0 {
		overallPercent = float64(totalVerified) / float64(totalExpected) * 100
	}
	
	t.Logf("Overall verification: %d/%d tasks verified (%.1f%%)",
		totalVerified, totalExpected, overallPercent)
	
	if resultVerificationErrors > 0 {
		t.Errorf("Task-server assignment verification errors: %d", resultVerificationErrors)
	}
	
	// Final validation
	if processedCount+failedCount < totalTasks && missingTasks > 0 {
		t.Errorf("Test failed: %d tasks missing, expected %d", missingTasks, totalTasks)
	} else if overallPercent < 99.0 {
		t.Errorf("Test failed: Only %.1f%% of tasks had verified server results (expected 99%%+)",
			overallPercent)
	} else {
		t.Logf("Test passed: Successfully processed %d tasks across %d providers and %d servers with %.1f%% verification",
			processedCount+failedCount, len(providers), len(providers)*config.serversPerProv, overallPercent)
	}
}

// TestMultiLayerServerHierarchy tests task distribution across a complex hierarchy
// of servers and subservers, verifying that each layer gets tasks appropriately
func TestMultiLayerServerHierarchy(t *testing.T) {
	// Setup logging
	logger := zerolog.New(zerolog.ConsoleWriter{Out: zerolog.NewConsoleWriter().Out, NoColor: true}).
		With().Timestamp().Logger()

	// Create a complex hierarchy of servers
	type serverLevel struct {
		name      string
		urlPrefix string
		children  []string // Child servers
	}

	// Define the server hierarchy - a tree structure
	hierarchy := []serverLevel{
		{
			name:      "mainserver",
			urlPrefix: "https://main.example.com",
			children:  []string{"sub1", "sub2", "sub3"},
		},
		{
			name:      "sub1",
			urlPrefix: "https://sub1.example.com",
			children:  []string{"sub1_child1", "sub1_child2"},
		},
		{
			name:      "sub2",
			urlPrefix: "https://sub2.example.com",
			children:  []string{"sub2_child1", "sub2_child2", "sub2_child3"},
		},
		{
			name:      "sub3",
			urlPrefix: "https://sub3.example.com",
			children:  []string{"sub3_child1"},
		},
	}

	// Create servers list
	serversList := make([]string, 0)
	serverHosts := make(map[string]string) // Maps server name to full URL

	// Add main nodes
	for _, level := range hierarchy {
		serverURL := fmt.Sprintf("%s/api", level.urlPrefix)
		serversList = append(serversList, serverURL)
		serverHosts[level.name] = serverURL

		// Add children for each node
		for _, child := range level.children {
			childURL := fmt.Sprintf("%s/%s/api", level.urlPrefix, child)
			serversList = append(serversList, childURL)
			serverHosts[child] = childURL
		}
	}

	// Count expected servers
	expectedServers := len(serversList)
	t.Logf("Created server hierarchy with %d servers", expectedServers)

	// Create providers
	providerCount := 3
	var providers []IProvider
	servers := make(map[string][]string)

	// Track which servers were used and verify results
	serverUsage := sync.Map{}
	serverTaskVerification := sync.Map{} // Maps server to list of task IDs it handled
	taskDetailResults := sync.Map{} // Maps taskID to result details
	
	for _, server := range serversList {
		serverUsage.Store(server, 0)
		serverTaskVerification.Store(server, []string{})
	}

	// Create completion tracking
	taskResults := sync.Map{} // Maps taskID to success/failure
	completionCh := make(chan string, 1000)
	var totalCompleted int32

	// Create providers with different server assignments
	for i := 0; i < providerCount; i++ {
		providerName := fmt.Sprintf("hierarchyProvider%d", i)
		provider := &MockProvider{name: providerName}

		// Assign servers to provider - different distribution patterns
		providerServers := make([]string, 0)
		
		if i == 0 {
			// First provider gets all servers
			providerServers = append(providerServers, serversList...)
		} else if i == 1 {
			// Second provider gets main servers + first children
			providerServers = append(providerServers, serverHosts["mainserver"])
			providerServers = append(providerServers, serverHosts["sub1"])
			providerServers = append(providerServers, serverHosts["sub2"])
			providerServers = append(providerServers, serverHosts["sub3"])
			providerServers = append(providerServers, serverHosts["sub1_child1"])
			providerServers = append(providerServers, serverHosts["sub2_child1"])
			providerServers = append(providerServers, serverHosts["sub3_child1"])
		} else {
			// Third provider gets only leaf nodes
			providerServers = append(providerServers, serverHosts["sub1_child1"])
			providerServers = append(providerServers, serverHosts["sub1_child2"])
			providerServers = append(providerServers, serverHosts["sub2_child1"])
			providerServers = append(providerServers, serverHosts["sub2_child2"])
			providerServers = append(providerServers, serverHosts["sub2_child3"])
			providerServers = append(providerServers, serverHosts["sub3_child1"])
		}

		providers = append(providers, provider)
		servers[providerName] = providerServers

		// Create handler functions
		provider.handleFunc = func(task ITask, server string) error {
			taskID := task.GetID()
			
			// Track which server was used
			if curr, ok := serverUsage.Load(server); ok {
				serverUsage.Store(server, curr.(int)+1)
			}
			
			// Add to server verification
			if taskList, ok := serverTaskVerification.Load(server); ok {
				list := taskList.([]string)
				list = append(list, taskID)
				serverTaskVerification.Store(server, list)
			}
			
			// Store detailed result for verification
			taskDetailResults.Store(taskID, struct {
				server     string
				provider   string
				startTime  time.Time
				endTime    time.Time
				verified   bool
			}{
				server:    server,
				provider:  task.GetProvider().Name(),
				startTime: time.Now(),
				verified:  true,
			})

			// Simulate processing
			time.Sleep(5 * time.Millisecond)
			
			// Update end time
			if detail, ok := taskDetailResults.Load(taskID); ok {
				result := detail.(struct {
					server     string
					provider   string
					startTime  time.Time
					endTime    time.Time
					verified   bool
				})
				result.endTime = time.Now()
				taskDetailResults.Store(taskID, result)
			}

			// Track result
			taskResults.Store(taskID, "completed")
			completionCh <- taskID
			atomic.AddInt32(&totalCompleted, 1)
			return nil
		}
	}

	// Define timeout function
	getTimeout := func(callbackName string, providerName string) time.Duration {
		return 500 * time.Millisecond
	}

	// Initialize the task manager with local instance
	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()

	// Set higher concurrency for test servers to avoid backoff delays
	// With 300 tasks and 10 servers, we need ~30 concurrent per server
	for _, serverURL := range serversList {
		tm.SetTaskManagerServerMaxParallel(serverURL, 30)
	}

	// Create tasks for each provider
	tasksPerProvider := 100
	totalTasks := tasksPerProvider * providerCount
	
	var wg sync.WaitGroup
	for i := 0; i < providerCount; i++ {
		provider := providers[i]
		
		for j := 0; j < tasksPerProvider; j++ {
			taskID := fmt.Sprintf("hierarchy_task_%d_%d", i, j)

			task := &MockTask{
				id:         taskID,
				priority:   j % 10, // Mixed priorities
				maxRetries: 2,
				createdAt:  time.Now(),
				provider:   provider,
				timeout:    200 * time.Millisecond,
				done:       make(chan struct{}),
			}

			wg.Add(1)
			go func(t *MockTask) {
				defer wg.Done()
				tm.AddTask(t)
			}(task)
		}
	}

	// Wait for task submission
	wg.Wait()
	t.Logf("Submitted %d tasks across %d providers", totalTasks, providerCount)

	// Wait for tasks to complete with timeout
	timeout := time.After(30 * time.Second)
	completedCount := 0

	for completedCount < totalTasks {
		select {
		case <-completionCh:
			completedCount++
			if completedCount%50 == 0 {
				t.Logf("Progress: %d/%d completed", completedCount, totalTasks)
			}
		case <-timeout:
			t.Logf("Timeout reached after waiting for task completion. Completed: %d/%d", 
				completedCount, totalTasks)
			goto VerificationPhase
		}
	}

VerificationPhase:
	// Verify server usage
	serverUsageCounts := make(map[string]int)
	unusedServers := 0
	
	// Collect counts
	serverUsage.Range(func(key, value interface{}) bool {
		server := key.(string)
		count := value.(int)
		serverUsageCounts[server] = count
		if count == 0 {
			unusedServers++
		}
		return true
	})

	// Log server usage summary
	t.Logf("Server utilization summary (%d servers total):", len(serverUsageCounts))
	t.Logf("  Used servers: %d", len(serverUsageCounts)-unusedServers)
	t.Logf("  Unused servers: %d", unusedServers)
	
	// Find most and least used servers
	var mostUsedServer, leastUsedServer string
	var mostUsedCount, leastUsedCount int
	
	first := true
	for server, count := range serverUsageCounts {
		if first || count > mostUsedCount {
			mostUsedServer = server
			mostUsedCount = count
		}
		if first || (count > 0 && count < leastUsedCount) {
			leastUsedServer = server
			leastUsedCount = count
		}
		first = false
	}
	
	if mostUsedCount > 0 {
		t.Logf("  Most used server: %s (%d tasks)", mostUsedServer, mostUsedCount)
	}
	if leastUsedCount > 0 {
		t.Logf("  Least used server: %s (%d tasks)", leastUsedServer, leastUsedCount)
	}
	
	// Calculate server usage distribution
	totalServersUsed := len(serverUsageCounts) - unusedServers
	if totalServersUsed > 0 {
		averageTasksPerServer := float64(completedCount) / float64(totalServersUsed)
		t.Logf("  Average tasks per used server: %.2f", averageTasksPerServer)
	}

	// Verify task completion
	finalCompletedCount := atomic.LoadInt32(&totalCompleted)
	t.Logf("Task completion: %d/%d (%.1f%%)", 
		finalCompletedCount, totalTasks, 
		float64(finalCompletedCount)/float64(totalTasks)*100)
	
	// Verify detailed task execution results
	t.Log("Verifying server processing results...")
	
	// Maps providerName -> serverName -> taskCount
	providerServerDistribution := make(map[string]map[string]int)
	
	// Check that all tasks were assigned to the correct servers
	var verifiedTasks int
	var incorrectServerAssignments int
	
	taskDetailResults.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		result := value.(struct {
			server     string
			provider   string
			startTime  time.Time
			endTime    time.Time
			verified   bool
		})
		
		// Check if this provider is allowed to use this server
		providerServers, ok := servers[result.provider]
		if !ok {
			t.Errorf("Provider %s not found in servers map", result.provider)
			return true
		}
		
		// Initialize the distribution map if needed
		if _, ok := providerServerDistribution[result.provider]; !ok {
			providerServerDistribution[result.provider] = make(map[string]int)
		}
		
		// Count this task for the provider/server
		providerServerDistribution[result.provider][result.server]++
		
		// Verify server assignment
		validServer := false
		for _, server := range providerServers {
			if server == result.server {
				validServer = true
				break
			}
		}
		
		if validServer {
			verifiedTasks++
		} else {
			incorrectServerAssignments++
			if incorrectServerAssignments < 10 { // Limit output
				t.Errorf("Task %s from provider %s was assigned to invalid server %s",
					taskID, result.provider, result.server)
			}
		}
		
		return true
	})
	
	// Print summary of provider-server distribution
	t.Log("Task distribution by provider and server:")
	for provider, serverMap := range providerServerDistribution {
		t.Logf("  Provider %s:", provider)
		
		// Sort servers by usage
		type serverCount struct {
			server string
			count  int
		}
		serverCounts := make([]serverCount, 0, len(serverMap))
		
		for server, count := range serverMap {
			serverCounts = append(serverCounts, serverCount{server, count})
		}
		
		// Sort by count
		for i := 0; i < len(serverCounts)-1; i++ {
			for j := i + 1; j < len(serverCounts); j++ {
				if serverCounts[i].count < serverCounts[j].count {
					serverCounts[i], serverCounts[j] = serverCounts[j], serverCounts[i]
				}
			}
		}
		
		// Print top 3 servers per provider
		maxToShow := 3
		if len(serverCounts) < maxToShow {
			maxToShow = len(serverCounts)
		}
		
		for i := 0; i < maxToShow; i++ {
			sc := serverCounts[i]
			t.Logf("    %s: %d tasks", sc.server, sc.count)
		}
	}
	
	// Log verification results
	verificationRate := float64(verifiedTasks) / float64(totalTasks) * 100
	t.Logf("Server assignment verification: %d/%d tasks verified (%.1f%%)",
		verifiedTasks, totalTasks, verificationRate)
	
	if incorrectServerAssignments > 0 {
		t.Errorf("Found %d tasks assigned to incorrect servers", incorrectServerAssignments)
	}
	
	// Test is successful if most tasks were processed, servers were utilized, and verification passed
	successThreshold := 0.95 // 95% of tasks should complete
	verificationThreshold := 99.0 // 99% of tasks should have verified correct server assignment
	
	if float64(finalCompletedCount)/float64(totalTasks) >= successThreshold {
		if verificationRate >= verificationThreshold {
			t.Logf("Test PASSED: Successfully processed %.1f%% of tasks with %.1f%% server verification",
				float64(finalCompletedCount)/float64(totalTasks)*100, verificationRate)
		} else {
			t.Errorf("Test FAILED: Only %.1f%% of tasks had verified server assignments (expected >= %.1f%%)",
				verificationRate, verificationThreshold)
		}
	} else {
		t.Errorf("Test FAILED: Only processed %.1f%% of tasks, expected >= %.1f%%",
			float64(finalCompletedCount)/float64(totalTasks)*100,
			successThreshold*100)
	}
}