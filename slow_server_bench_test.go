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

// BenchmarkSlowServersConfig tests how SMT performs with very slow servers (4-7 seconds)
// to identify the optimal configuration for high-latency backends
func BenchmarkSlowServersConfig(b *testing.B) {
	// Skip in short mode
	if testing.Short() {
		b.Skip("Skipping slow server benchmark in short mode")
	}
	
	// Print a summary table for the results
	fmt.Println("\nSlow Server Benchmark Results (4-7 second response times):")
	fmt.Println("Configuration  | Avg Time | P50 Time | P95 Time | Max Time")
	
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
	
	// Create a logger that doesn't output during benchmarks
	logger := zerolog.Nop()
	
	// Test parameters - reduced for Claude CLI timeout limits
	numServers := 6
	totalTasks := 18 // Further reduced for multiple rounds
	baseLatency := 5500 * time.Millisecond // 5.5 seconds
	jitterPercent := 25                    // ±25% jitter (4.125-6.875s)
	timeout := 10 * time.Second
	
	// Create server names
	serverNames := make([]string, numServers)
	for i := 0; i < numServers; i++ {
		serverNames[i] = fmt.Sprintf("server%d", i+1)
	}
	
	// Run benchmarks for each configuration
	for _, config := range configs {
		b.Run(config.name, func(b *testing.B) {
			// Create provider that simulates slow processing
			provider := &SlowServerProvider{
				name:          "slow-provider",
				baseLatency:   baseLatency,
				jitterPercent: jitterPercent,
			}
			
			providers := []IProvider{provider}
			servers := map[string][]string{
				"slow-provider": serverNames,
			}
			
			// Create task manager with specified configuration
			tm := NewTaskManagerWithOptions(&providers, servers, &logger, func(string, string) time.Duration {
				return timeout
			}, &TaskManagerOptions{
				EnablePooling:         config.enablePooling,
				EnableAdaptiveTimeout: config.enableAdaptiveTimeout,
				EnableTwoLevel:        config.enableTwoLevel,
			})
			
			// Start the task manager
			tm.Start()
			defer tm.Shutdown()
			
			// Track metrics for this configuration
			var totalTaskTimeMs int64
			var tasksProcessed int64
			var maxTaskTimeMs int64
			var p95TaskTimeMs int64
			var p50TaskTimeMs int64
			var taskTimesMs []int64
			
			// Reset timer before benchmark
			b.ResetTimer()
			
			// Run the benchmark b.N times
			for i := 0; i < b.N; i++ {
				// Setup task completion tracking
				var wg sync.WaitGroup
				wg.Add(totalTasks)
				
				// Clear metrics for this iteration
				atomic.StoreInt64(&totalTaskTimeMs, 0)
				atomic.StoreInt64(&tasksProcessed, 0)
				atomic.StoreInt64(&maxTaskTimeMs, 0)
				taskTimesMs = make([]int64, 0, totalTasks)
				
				// Process tasks
				taskTimesMutex := &sync.Mutex{}
				
				// Submit all tasks at the start
				for t := 0; t < totalTasks; t++ {
					taskID := fmt.Sprintf("task-%d-%d", i, t)
					
					task := &SlowServerTask{
						id:       taskID,
						priority: t % 10,
						provider: provider,
						
						// Task completion callback
						onComplete: func(taskTimeMs int64) {
							// Update metrics
							atomic.AddInt64(&totalTaskTimeMs, taskTimeMs)
							atomic.AddInt64(&tasksProcessed, 1)
							
							// Update max time
							for {
								currentMax := atomic.LoadInt64(&maxTaskTimeMs)
								if taskTimeMs <= currentMax || atomic.CompareAndSwapInt64(&maxTaskTimeMs, currentMax, taskTimeMs) {
									break
								}
							}
							
							// Store task time for percentile calculation
							taskTimesMutex.Lock()
							taskTimesMs = append(taskTimesMs, taskTimeMs)
							taskTimesMutex.Unlock()
							
							// Signal completion
							wg.Done()
						},
					}
					
					tm.AddTask(task)
					
					// Add a small delay between task submissions to avoid overwhelming the system
					time.Sleep(50 * time.Millisecond)
				}
				
				// Wait for all tasks to complete
				wg.Wait()
				
				// Calculate metrics
				avgTaskTimeMs := float64(totalTaskTimeMs) / float64(tasksProcessed)
				
				// Sort task times for percentile calculation
				taskTimesMutex.Lock()
				insertionSort(taskTimesMs)
				
				// Calculate p50 and p95 if we have enough data
				if len(taskTimesMs) > 0 {
					p50Index := len(taskTimesMs) / 2
					p95Index := int(float64(len(taskTimesMs)) * 0.95)
					
					if p50Index < len(taskTimesMs) {
						p50TaskTimeMs = taskTimesMs[p50Index]
					}
					
					if p95Index < len(taskTimesMs) {
						p95TaskTimeMs = taskTimesMs[p95Index]
					}
				}
				taskTimesMutex.Unlock()
				
				// Report metrics
				b.ReportMetric(avgTaskTimeMs, "avg_ms")
				b.ReportMetric(float64(p50TaskTimeMs), "p50_ms")
				b.ReportMetric(float64(p95TaskTimeMs), "p95_ms")
				b.ReportMetric(float64(maxTaskTimeMs), "max_ms")
				b.ReportMetric(float64(tasksProcessed), "tasks")
				
				// Print summary row for this configuration
				fmt.Printf("%-14s | %7.1f | %7.1f | %7.1f | %7.1f\n", 
					config.name, 
					avgTaskTimeMs, 
					float64(p50TaskTimeMs), 
					float64(p95TaskTimeMs),
					float64(maxTaskTimeMs))
			}
			
			// Stop timer
			b.StopTimer()
		})
	}
}

// Simple insertion sort for small slice
func insertionSort(a []int64) {
	for i := 1; i < len(a); i++ {
		key := a[i]
		j := i - 1
		
		for j >= 0 && a[j] > key {
			a[j+1] = a[j]
			j--
		}
		
		a[j+1] = key
	}
}

// SlowServerProvider implements IProvider for simulating slow servers
type SlowServerProvider struct {
	name          string
	baseLatency   time.Duration
	jitterPercent int
}

func (p *SlowServerProvider) Name() string {
	return p.name
}

func (p *SlowServerProvider) Handle(task ITask, server string) error {
	// Calculate processing time with jitter
	jitterFactor := 1.0 + (float64(rand.Intn(p.jitterPercent*2+1)-p.jitterPercent) / 100.0)
	processingTime := time.Duration(float64(p.baseLatency) * jitterFactor)
	
	// Simulate slow processing
	time.Sleep(processingTime)
	
	return nil
}

// SlowServerTask implements ITask for slow server testing
type SlowServerTask struct {
	id         string
	priority   int
	provider   IProvider
	startTime  time.Time
	onComplete func(int64)
	mu         sync.Mutex
}

func (t *SlowServerTask) MarkAsSuccess(execTime int64) {
	// Record completion time
	if t.onComplete != nil {
		t.onComplete(execTime)
	}
}

func (t *SlowServerTask) MarkAsFailed(execTime int64, err error) {
	// Record completion time even for failures
	if t.onComplete != nil {
		t.onComplete(execTime)
	}
}

func (t *SlowServerTask) GetPriority() int {
	return t.priority
}

func (t *SlowServerTask) GetID() string {
	return t.id
}

func (t *SlowServerTask) GetMaxRetries() int {
	return 1
}

func (t *SlowServerTask) GetRetries() int {
	return 0
}

func (t *SlowServerTask) GetCreatedAt() time.Time {
	if t.startTime.IsZero() {
		return time.Now()
	}
	return t.startTime
}

func (t *SlowServerTask) GetTaskGroup() ITaskGroup {
	return nil
}

func (t *SlowServerTask) GetProvider() IProvider {
	return t.provider
}

func (t *SlowServerTask) UpdateRetries(r int) error {
	return nil
}

func (t *SlowServerTask) GetTimeout() time.Duration {
	return 0 // Use default
}

func (t *SlowServerTask) UpdateLastError(s string) error {
	return nil
}

func (t *SlowServerTask) GetCallbackName() string {
	return "slow-benchmark"
}

func (t *SlowServerTask) OnComplete() {
	// Already handled in MarkAsSuccess/MarkAsFailed
}

func (t *SlowServerTask) OnStart() {
	t.startTime = time.Now()
}