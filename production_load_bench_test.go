package smt

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// BenchmarkProductionLoadPatterns tests SMT under realistic production load patterns
// with varying server counts and request patterns over extended periods
func BenchmarkProductionLoadPatterns(b *testing.B) {
	// Define configurations to test
	configs := []struct {
		name                string
		enablePooling       bool
		enableAdaptiveTimeout bool
		enableTwoLevel      bool
	}{
		{
			name:                "TwoLevelOnly",
			enablePooling:       false,
			enableAdaptiveTimeout: false,
			enableTwoLevel:      true,
		},
		{
			name:                "MemPool_TwoLevel", // Current optimized configuration
			enablePooling:       true,
			enableAdaptiveTimeout: false,
			enableTwoLevel:      true,
		},
		{
			name:                "AllFeatures",
			enablePooling:       true,
			enableAdaptiveTimeout: true,
			enableTwoLevel:      true,
		},
	}
	
	// Define production-like scenarios that run for 2 minutes each
	scenarios := []struct {
		name           string
		serverCounts   []int  // Number of servers per provider
		providerCount  int    // Number of providers
		loadPattern    string // Type of load pattern
	}{
		{
			name:          "SlowServers_SteadyLoad",
			serverCounts:  []int{2, 4}, // 6 servers total (fewer servers due to long response time)
			providerCount: 2,
			loadPattern:   "steady", // Steady, consistent load
		},
		{
			name:          "SlowServers_BurstyLoad",
			serverCounts:  []int{3, 3}, // 6 servers total
			providerCount: 2,
			loadPattern:   "bursty", // Periodic traffic spikes
		},
	}
	
	// Create logger
	logger := zerolog.Nop()
	
	// Define duration for each test (adjusted for tool timeout limits)
	testDuration := 90 * time.Second
	
	// Track metrics across all tests
	type BenchmarkResult struct {
		avgTaskTime      float64 // Average time per task in milliseconds
		p95TaskTime      float64 // 95th percentile task time in milliseconds
		tasksProcessed   int64   // Total tasks processed
		peakTasksPerSec  int64   // Peak tasks processed per second
		serverUtilization float64 // Server utilization percentage
	}
	
	benchResults := make(map[string]map[string]BenchmarkResult)
	
	// Run the benchmarks
	for _, scenario := range scenarios {
		benchResults[scenario.name] = make(map[string]BenchmarkResult)
		
		b.Run(scenario.name, func(b *testing.B) {
			for _, config := range configs {
				b.Run(config.name, func(b *testing.B) {
					// Initialize providers and servers
					providers := make([]IProvider, 0, scenario.providerCount)
					servers := make(map[string][]string)
					
					totalServers := 0
					for i := 0; i < scenario.providerCount; i++ {
						providerName := fmt.Sprintf("provider%d", i)
						
						// Create provider with slow processing behavior (4-7 seconds)
						provider := &ProductionProvider{
							name:          providerName,
							baseLatency:   5500 * time.Millisecond, // 5.5 seconds base
							jitterPercent: 25,                     // 25% variation (4.125-6.875 seconds)
						}
						
						providers = append(providers, provider)
						
						// Create servers for this provider
						serverCount := scenario.serverCounts[i%len(scenario.serverCounts)]
						serverList := make([]string, 0, serverCount)
						for j := 0; j < serverCount; j++ {
							serverList = append(serverList, fmt.Sprintf("server-%d-%d", i, j))
						}
						servers[providerName] = serverList
						totalServers += serverCount
					}
					
					// Create task manager with config (increased timeout for slow servers)
					tm := NewTaskManagerWithOptions(&providers, servers, &logger, func(string, string) time.Duration {
						return 10 * time.Second // Increased timeout for slow servers
					}, &TaskManagerOptions{
						EnablePooling:         config.enablePooling,
						EnableAdaptiveTimeout: config.enableAdaptiveTimeout,
						EnableTwoLevel:        config.enableTwoLevel,
						PoolConfig: &PoolConfig{
							PreWarmSize:  10000,
							TrackStats:   false,
							PreWarmAsync: true,
						},
					})
					
					// Start the task manager
					tm.Start()
					defer tm.Shutdown()
					
					// Reset timer
					b.ResetTimer()
					
					// Create channels and goroutines for load generation and metrics
					taskCompletionTimes := make(chan int64, 100000) // Holds task completion times in nanoseconds
					loadStopCh := make(chan struct{})
					metricsDoneCh := make(chan struct{})
					
					// Metric collection variables
					var totalTasks int64
					var totalTaskTimeNs int64
					var peakTasksPerSec int64
					timePoints := make([]int64, 0, 1000000) // Store all task times for percentiles
					
					// Start metrics collection goroutine
					go func() {
						defer close(metricsDoneCh)
						
						// Track tasks per second
						ticker := time.NewTicker(1 * time.Second)
						defer ticker.Stop()
						
						var tasksThisSecond int64
						
						for {
							select {
							case taskTimeNs := <-taskCompletionTimes:
								atomic.AddInt64(&totalTasks, 1)
								atomic.AddInt64(&totalTaskTimeNs, taskTimeNs)
								atomic.AddInt64(&tasksThisSecond, 1)
								
								// Add to slice for percentile calculation
								timePoints = append(timePoints, taskTimeNs)
								
							case <-ticker.C:
								// Check if this is a new peak
								currentCount := atomic.LoadInt64(&tasksThisSecond)
								currentPeak := atomic.LoadInt64(&peakTasksPerSec)
								
								if currentCount > currentPeak {
									atomic.StoreInt64(&peakTasksPerSec, currentCount)
								}
								
								// Reset counter
								atomic.StoreInt64(&tasksThisSecond, 0)
								
							case <-loadStopCh:
								return
							}
						}
					}()
					
					// Start load generation based on scenario
					
					// Create base rates based on pattern (reduced for slow servers)
					baseTasksPerSec := 2 * totalServers // Tasks per second (reduced for slow servers)
					burstMultiplier := 5.0               // Burst multiplier
					
					switch scenario.loadPattern {
					case "steady":
						// Generate steady load
						go generateSteadyLoad(tm, providers, baseTasksPerSec, testDuration, taskCompletionTimes)
						
					case "bursty":
						// Generate bursty load
						go generateBurstyLoad(tm, providers, baseTasksPerSec, burstMultiplier, testDuration, taskCompletionTimes)
						
					case "mixed":
						// Generate mixed load
						go generateMixedLoad(tm, providers, baseTasksPerSec, burstMultiplier, testDuration, taskCompletionTimes)
					}
					
					// Wait for test duration
					time.Sleep(testDuration)
					
					// Signal load generators to stop
					close(loadStopCh)
					
					// Wait for metrics collection to finish
					<-metricsDoneCh
					
					// Calculate metrics
					avgTaskTimeMs := float64(totalTaskTimeNs) / float64(totalTasks) / 1e6
					
					// Calculate P95 (95th percentile)
					p95 := calculatePercentile(timePoints, 95) / 1e6
					
					// Calculate server utilization
					// Theoretical max: totalServers * testDuration / avgTaskTime
					theoreticalMaxTasks := float64(totalServers) * float64(testDuration) / float64(5500*time.Millisecond)
					utilization := (float64(totalTasks) / theoreticalMaxTasks) * 100
					
					// Store results
					benchResults[scenario.name][config.name] = BenchmarkResult{
						avgTaskTime:      avgTaskTimeMs,
						p95TaskTime:      p95,
						tasksProcessed:   totalTasks,
						peakTasksPerSec:  peakTasksPerSec,
						serverUtilization: utilization,
					}
					
					// Report metrics
					b.ReportMetric(avgTaskTimeMs, "ms/task")
					b.ReportMetric(float64(p95), "p95ms")
					b.ReportMetric(float64(totalTasks), "tasks")
					b.ReportMetric(float64(peakTasksPerSec), "peak/sec")
					b.ReportMetric(utilization, "%util")
					
					// Stop timer
					b.StopTimer()
				})
			}
		})
	}
	
	// Print summary table of results
	b.Logf("\nSummary of Results:\n")
	for scenarioName, results := range benchResults {
		b.Logf("Scenario: %s", scenarioName)
		b.Logf("Config          Avg(ms)  P95(ms)  Tasks     Peak/sec  Util%%")
		b.Logf("-----------------------------------------------------------")
		
		for configName, result := range results {
			b.Logf("%-15s %-8.2f %-8.2f %-9d %-9d %.2f", 
				configName, 
				result.avgTaskTime, 
				result.p95TaskTime,
				result.tasksProcessed,
				result.peakTasksPerSec,
				result.serverUtilization)
		}
		b.Logf("\n")
	}
}

// Helper functions for load generation

// generateSteadyLoad generates a steady load of tasks
func generateSteadyLoad(tm *TaskManagerSimple, providers []IProvider, tasksPerSec int, duration time.Duration, completionTimes chan<- int64) {
	// Calculate interval between tasks
	intervalMs := time.Duration(1000.0 / float64(tasksPerSec) * float64(time.Millisecond))
	
	// Create ticker for consistent load
	ticker := time.NewTicker(intervalMs)
	defer ticker.Stop()
	
	endTime := time.Now().Add(duration)
	taskID := 0
	
	for time.Now().Before(endTime) {
		<-ticker.C
		
		// Launch task
		taskID++
		providerIdx := taskID % len(providers)
		priority := taskID % 10
		
		task := &ProductionTask{
			id:             fmt.Sprintf("steady-task-%d", taskID),
			priority:       priority,
			provider:       providers[providerIdx],
			completionTime: completionTimes,
		}
		
		tm.AddTask(task)
	}
}

// generateBurstyLoad generates load with periodic traffic spikes
func generateBurstyLoad(tm *TaskManagerSimple, providers []IProvider, baseTasksPerSec int, burstMultiplier float64, duration time.Duration, completionTimes chan<- int64) {
	// Calculate intervals
	normalIntervalMs := time.Duration(1000.0 / float64(baseTasksPerSec) * float64(time.Millisecond))
	burstIntervalMs := time.Duration(1000.0 / (float64(baseTasksPerSec) * burstMultiplier) * float64(time.Millisecond))
	
	// Create ticker for load
	ticker := time.NewTicker(normalIntervalMs)
	defer ticker.Stop()
	
	endTime := time.Now().Add(duration)
	taskID := 0
	
	// Burst every 30 seconds for 10 seconds
	burstDuration := 10 * time.Second
	cycleDuration := 30 * time.Second
	
	startTime := time.Now()
	
	for time.Now().Before(endTime) {
		// Check if we're in a burst period
		elapsed := time.Since(startTime)
		cycleElapsed := elapsed % cycleDuration
		inBurst := cycleElapsed < burstDuration
		
		// Update ticker rate if entering/leaving burst
		if inBurst {
			ticker.Reset(burstIntervalMs)
		} else {
			ticker.Reset(normalIntervalMs)
		}
		
		<-ticker.C
		
		// Launch task
		taskID++
		providerIdx := taskID % len(providers)
		priority := taskID % 10
		
		task := &ProductionTask{
			id:             fmt.Sprintf("bursty-task-%d", taskID),
			priority:       priority,
			provider:       providers[providerIdx],
			completionTime: completionTimes,
		}
		
		tm.AddTask(task)
	}
}

// generateMixedLoad generates a mix of steady and bursty traffic
func generateMixedLoad(tm *TaskManagerSimple, providers []IProvider, baseTasksPerSec int, burstMultiplier float64, duration time.Duration, completionTimes chan<- int64) {
	// Launch both steady and bursty loads in parallel
	steadyRate := baseTasksPerSec / 2
	burstyRate := baseTasksPerSec / 2
	
	go generateSteadyLoad(tm, providers, steadyRate, duration, completionTimes)
	go generateBurstyLoad(tm, providers, burstyRate, burstMultiplier, duration, completionTimes)
}

// calculatePercentile calculates the Nth percentile of the given values
func calculatePercentile(values []int64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	
	// Sort values (simple insertion sort)
	for i := 1; i < len(values); i++ {
		key := values[i]
		j := i - 1
		
		for j >= 0 && values[j] > key {
			values[j+1] = values[j]
			j--
		}
		
		values[j+1] = key
	}
	
	// Calculate percentile index
	index := int((percentile / 100) * float64(len(values)-1))
	return float64(values[index])
}

// ProductionProvider implements IProvider for production-like loads
type ProductionProvider struct {
	name          string
	baseLatency   time.Duration
	jitterPercent int // How much latency can vary (percent)
}

func (p *ProductionProvider) Name() string {
	return p.name
}

func (p *ProductionProvider) Handle(task ITask, server string) error {
	// Calculate actual processing time with jitter
	jitterFactor := 1.0 + (float64(rand.Intn(p.jitterPercent*2+1)-p.jitterPercent) / 100.0)
	processingTime := time.Duration(float64(p.baseLatency) * jitterFactor)
	
	// Simulate work
	time.Sleep(processingTime)
	return nil
}

// ProductionTask implements ITask for production-like benchmarks
type ProductionTask struct {
	id             string
	priority       int
	provider       IProvider
	timeout        time.Duration
	createdAt      time.Time
	startTime      int64 // Nanoseconds since epoch
	completionTime chan<- int64
	mu             sync.Mutex
}

func (t *ProductionTask) MarkAsSuccess(execTime int64) {
	// Nothing to do
}

func (t *ProductionTask) MarkAsFailed(execTime int64, err error) {
	// Nothing to do
}

func (t *ProductionTask) GetPriority() int {
	return t.priority
}

func (t *ProductionTask) GetID() string {
	return t.id
}

func (t *ProductionTask) GetMaxRetries() int {
	return 1
}

func (t *ProductionTask) GetRetries() int {
	return 0
}

func (t *ProductionTask) GetCreatedAt() time.Time {
	if t.createdAt.IsZero() {
		return time.Now()
	}
	return t.createdAt
}

func (t *ProductionTask) GetTaskGroup() ITaskGroup {
	return nil
}

func (t *ProductionTask) GetProvider() IProvider {
	return t.provider
}

func (t *ProductionTask) UpdateRetries(r int) error {
	return nil
}

func (t *ProductionTask) GetTimeout() time.Duration {
	return t.timeout
}

func (t *ProductionTask) UpdateLastError(s string) error {
	return nil
}

func (t *ProductionTask) GetCallbackName() string {
	return "benchmark"
}

func (t *ProductionTask) OnComplete() {
	// Record task completion time if channel is provided
	if t.completionTime != nil && t.startTime > 0 {
		// Calculate time taken
		endTime := time.Now().UnixNano()
		taskTime := endTime - t.startTime
		
		// Send to metrics channel
		select {
		case t.completionTime <- taskTime:
			// Sent successfully
		default:
			// Channel full, discard (shouldn't happen often)
		}
	}
}

func (t *ProductionTask) OnStart() {
	// Record start time
	t.startTime = time.Now().UnixNano()
}