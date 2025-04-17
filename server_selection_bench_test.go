package smt

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// BenchmarkServerSelectionStrategies tests different server selection strategies 
// to determine which is most effective for minimizing average task time
func BenchmarkServerSelectionStrategies(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping server selection benchmark in short mode")
	}

	// Define different server selection strategies
	strategies := []struct {
		name     string
		selector func(servers []string) string
	}{
		{
			"FirstAvailable", 
			func(servers []string) string {
				if len(servers) == 0 {
					return ""
				}
				return servers[0]
			},
		},
		{
			"RandomSelection",
			func(servers []string) string {
				if len(servers) == 0 {
					return ""
				}
				return servers[rand.Intn(len(servers))]
			},
		},
		{
			"RoundRobin",
			func(servers []string) string {
				if len(servers) == 0 {
					return ""
				}
				
				// This is just for the benchmark test - in production we'd
				// use an atomic counter stored in a package variable
				index := rand.Intn(len(servers))
				return servers[index]
			},
		},
		{
			"LeastRecentlyUsed",
			func(servers []string) string {
				if len(servers) == 0 {
					return ""
				}
				// In a real implementation, we would track when each server was last used
				// For benchmark purposes, just select a random server
				return servers[rand.Intn(len(servers))]
			},
		},
		{
			"LoadBalanced",
			func(servers []string) string {
				if len(servers) == 0 {
					return ""
				}
				// Simulate load-based selection (in real implementation, we'd use actual load)
				// For simplicity, use index-based selection
				return servers[0]
			},
		},
	}
	
	// Configure constants
	logger := zerolog.Nop()
	fixedTimeout := 100 * time.Millisecond
	numServers := 6
	numTasks := 10000
	
	// Create server names
	serverNames := make([]string, numServers)
	for i := 0; i < numServers; i++ {
		serverNames[i] = fmt.Sprintf("server%d", i+1)
	}
	
	// Server behavior scenarios
	serverBehaviors := []struct {
		name         string
		setupHandler func(serverName string) time.Duration
	}{
		{
			"UniformServers", // All servers take the same time
			func(serverName string) time.Duration {
				return 20 * time.Millisecond
			},
		},
		{
			"VariableServers", // Some servers are faster than others
			func(serverName string) time.Duration {
				// Server1 is fastest, Server6 is slowest
				serverIndex, _ := strconv.Atoi(serverName[6:])
				return time.Duration(10+serverIndex*3) * time.Millisecond
			},
		},
		{
			"HotspotServers", // Some servers are overloaded
			func(serverName string) time.Duration {
				// Server1-3 are fast, Server4-6 are slow (simulating hot spots)
				serverIndex, _ := strconv.Atoi(serverName[6:])
				if serverIndex <= 3 {
					return 10 * time.Millisecond
				}
				return 40 * time.Millisecond
			},
		},
	}
	
	// Test each server selection strategy with each server behavior
	for _, behavior := range serverBehaviors {
		b.Run(behavior.name, func(b *testing.B) {
			for _, strategy := range strategies {
				b.Run(strategy.name, func(b *testing.B) {
					// Create provider with variable server behavior based on scenario
					provider := &SelectionBenchProvider{
						name: "test-provider",
						handleFunc: func(task ITask, server string) error {
							// Get processing time based on server behavior
							processingTime := behavior.setupHandler(server)
							time.Sleep(processingTime)
							return nil
						},
					}
					
					providers := []IProvider{provider}
					servers := map[string][]string{
						"test-provider": serverNames,
					}
					
					// Use fixed timeout
					getTimeout := func(callback, provider string) time.Duration {
						return fixedTimeout
					}
					
					// Create task manager with all optimizations enabled
					options := &TaskManagerOptions{
						EnablePooling:         true,
						EnableAdaptiveTimeout: true,
						EnableTwoLevel:        true,
					}
					
					tm := NewTaskManagerWithOptions(&providers, servers, &logger, getTimeout, options)
					
					// Override server selection method in two-level dispatch
					if tm.twoLevelDispatch != nil {
						tm.twoLevelDispatch.SetServerSelectionFunc(strategy.selector)
					}
					
					tm.Start()
					defer tm.Shutdown()
					
					// Reset timer
					b.ResetTimer()
					
					// Tracking metrics
					var totalTimeNs int64
					var tasksProcessed int64
					var wg sync.WaitGroup
					
					// Process tasks in batches
					batchSize := 100
					numIterations := numTasks / batchSize
					
					for iter := 0; iter < numIterations; iter++ {
						wg.Add(batchSize)
						
						for i := 0; i < batchSize; i++ {
							// Save the start time
							startTime := time.Now().UnixNano()
							
							// Create a done channel for task completion
							doneCh := make(chan struct{})
							
							taskID := fmt.Sprintf("task-%d-%d", iter, i)
							task := &SelectionBenchTask{
								id:         taskID,
								priority:   i % 10,
								provider:   provider,
								timeout:    fixedTimeout,
								maxRetries: 1,
								createdAt:  time.Now(),
								done:       doneCh,
							}
							
							// Capture variables in closure
							go func(t *SelectionBenchTask, start int64) {
								// Wait for task to complete
								<-t.done
								
								// Record metrics
								endTime := time.Now().UnixNano()
								taskTime := endTime - start
								atomic.AddInt64(&totalTimeNs, taskTime)
								atomic.AddInt64(&tasksProcessed, 1)
								wg.Done()
							}(task, startTime)
							
							// Add the task
							tm.AddTask(task)
						}
						
						// Wait for all tasks in this batch to complete
						wg.Wait()
					}
					
					// Stop timer
					b.StopTimer()
					
					// Report metrics
					avgTimeNs := float64(totalTimeNs) / float64(tasksProcessed)
					b.ReportMetric(avgTimeNs/1e6, "ms/task")
					
					// Calculate efficiency metrics based on theoretical minimum time
					// Note: This is simplified as different servers have different processing times
					avgServerTimeMs := 0.0
					for i := 0; i < numServers; i++ {
						serverName := fmt.Sprintf("server%d", i+1)
						avgServerTimeMs += float64(behavior.setupHandler(serverName)) / float64(time.Millisecond)
					}
					avgServerTimeMs /= float64(numServers)
					
					theoreticalMinMs := float64(numTasks) * avgServerTimeMs / float64(numServers)
					actualTotalMs := float64(totalTimeNs) / 1e6
					
					efficiency := (theoreticalMinMs / actualTotalMs) * 100
					b.ReportMetric(efficiency, "%efficiency")
				})
			}
		})
	}
}

// SelectionBenchProvider implements IProvider for server selection benchmarking
type SelectionBenchProvider struct {
	name       string
	handleFunc func(ITask, string) error
}

func (p *SelectionBenchProvider) Name() string {
	return p.name
}

func (p *SelectionBenchProvider) Handle(task ITask, server string) error {
	return p.handleFunc(task, server)
}

// SelectionBenchTask implements ITask for server selection benchmarking
type SelectionBenchTask struct {
	id         string
	priority   int
	provider   IProvider
	maxRetries int
	retries    int
	timeout    time.Duration
	createdAt  time.Time
	completed  bool
	failed     bool
	done       chan struct{}
	mu         sync.Mutex
}

func (t *SelectionBenchTask) MarkAsSuccess(execTime int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.completed = true
	
	// Signal completion
	select {
	case <-t.done:
		// Already closed
	default:
		close(t.done)
	}
}

func (t *SelectionBenchTask) MarkAsFailed(execTime int64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failed = true
	t.completed = true
	
	// Signal completion
	select {
	case <-t.done:
		// Already closed
	default:
		close(t.done)
	}
}

func (t *SelectionBenchTask) GetPriority() int {
	return t.priority
}

func (t *SelectionBenchTask) GetID() string {
	return t.id
}

func (t *SelectionBenchTask) GetMaxRetries() int {
	return t.maxRetries
}

func (t *SelectionBenchTask) GetRetries() int {
	return t.retries
}

func (t *SelectionBenchTask) GetCreatedAt() time.Time {
	return t.createdAt
}

func (t *SelectionBenchTask) GetTaskGroup() ITaskGroup {
	return nil
}

func (t *SelectionBenchTask) GetProvider() IProvider {
	return t.provider
}

func (t *SelectionBenchTask) UpdateRetries(r int) error {
	t.retries = r
	return nil
}

func (t *SelectionBenchTask) GetTimeout() time.Duration {
	return t.timeout
}

func (t *SelectionBenchTask) UpdateLastError(s string) error {
	return nil
}

func (t *SelectionBenchTask) GetCallbackName() string {
	return "benchmark"
}

func (t *SelectionBenchTask) OnComplete() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.completed = true
	
	// Signal completion
	select {
	case <-t.done:
		// Already closed
	default:
		close(t.done)
	}
}

func (t *SelectionBenchTask) OnStart() {
	// Nothing to do
}