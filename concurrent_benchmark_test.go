package smt

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"runtime"
	"github.com/rs/zerolog"
)

func BenchmarkConcurrentAddTask(b *testing.B) {
	cases := []struct {
		name       string
		goroutines int
		tasks      int
	}{
		{"1-goroutine-1000-tasks", 1, 1000},
		{"10-goroutines-100-tasks", 10, 100},
		{"100-goroutines-10-tasks", 100, 10},
		{"1000-goroutines-1-task", 1000, 1},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			logger := zerolog.New(zerolog.NewConsoleWriter()).Level(zerolog.ErrorLevel)
			
			// Setup
			var providers []IProvider
			servers := make(map[string][]string)
			
			for i := 0; i < 10; i++ {
				providerName := fmt.Sprintf("provider%d", i)
				provider := &MockProvider{
					name: providerName,
					handleFunc: func(task ITask, server string) error {
						time.Sleep(1 * time.Millisecond)
						return nil
					},
				}
				providers = append(providers, provider)
				
				serverList := make([]string, 20)
				for j := 0; j < 20; j++ {
					serverList[j] = fmt.Sprintf("server_%s_%d", providerName, j)
				}
				servers[providerName] = serverList
			}
			
			getTimeout := func(string, string) time.Duration {
				return 30 * time.Second
			}
			
			InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)
			defer TaskQueueManagerInstance.Shutdown()
			
			// Let system stabilize
			time.Sleep(100 * time.Millisecond)
			
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				totalTasks := tc.goroutines * tc.tasks
				
				start := time.Now()
				
				// Launch goroutines
				for g := 0; g < tc.goroutines; g++ {
					wg.Add(1)
					go func(goroutineID int) {
						defer wg.Done()
						for t := 0; t < tc.tasks; t++ {
							task := &MockTask{
								id:         fmt.Sprintf("task_%d_%d_%d", i, goroutineID, t),
								priority:   t % 10,
								maxRetries: 1,
								provider:   providers[(goroutineID+t)%len(providers)],
								timeout:    100 * time.Millisecond,
								done:       make(chan struct{}),
							}
							AddTask(task, &logger)
						}
					}(g)
				}
				
				wg.Wait()
				elapsed := time.Since(start)
				
				// Report throughput
				throughput := float64(totalTasks) / elapsed.Seconds()
				b.ReportMetric(throughput, "tasks/sec")
			}
		})
	}
}

func BenchmarkGlobalLockContention(b *testing.B) {
	logger := zerolog.New(zerolog.NewConsoleWriter()).Level(zerolog.ErrorLevel)
	
	var providers []IProvider
	servers := make(map[string][]string)
	
	provider := &MockProvider{
		name: "provider",
		handleFunc: func(task ITask, server string) error {
			return nil
		},
	}
	providers = append(providers, provider)
	servers["provider"] = []string{"server1"}
	
	getTimeout := func(string, string) time.Duration {
		return 30 * time.Second
	}
	
	InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)
	defer TaskQueueManagerInstance.Shutdown()
	
	time.Sleep(50 * time.Millisecond)
	
	b.Run("sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			task := &MockTask{
				id:       fmt.Sprintf("task_%d", i),
				provider: provider,
				done:     make(chan struct{}),
			}
			AddTask(task, &logger)
		}
	})
	
	b.Run("parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				task := &MockTask{
					id:       fmt.Sprintf("task_%d_%d", i, runtime.GOMAXPROCS(0)),
					provider: provider,
					done:     make(chan struct{}),
				}
				AddTask(task, &logger)
				i++
			}
		})
	})
}

func TestAddTaskStress(t *testing.T) {
	logger := zerolog.New(zerolog.NewConsoleWriter()).Level(zerolog.ErrorLevel)
	
	// Setup with multiple providers
	var providers []IProvider
	servers := make(map[string][]string)
	
	for i := 0; i < 10; i++ {
		providerName := fmt.Sprintf("provider%d", i)
		provider := &MockProvider{
			name: providerName,
			handleFunc: func(task ITask, server string) error {
				// Simulate varying processing times
				time.Sleep(time.Duration(1+task.GetPriority()) * time.Millisecond)
				return nil
			},
		}
		providers = append(providers, provider)
		
		serverList := make([]string, 10)
		for j := 0; j < 10; j++ {
			serverList[j] = fmt.Sprintf("server_%s_%d", providerName, j)
		}
		servers[providerName] = serverList
	}
	
	getTimeout := func(string, string) time.Duration {
		return 30 * time.Second
	}
	
	InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)
	defer TaskQueueManagerInstance.Shutdown()
	
	// Stress test with high concurrency
	const numGoroutines = 1000
	const tasksPerGoroutine = 10
	
	var successCount int32
	var failCount int32
	var wg sync.WaitGroup
	
	start := time.Now()
	
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			
			for i := 0; i < tasksPerGoroutine; i++ {
				task := &MockTask{
					id:         fmt.Sprintf("stress_task_%d_%d", gid, i),
					priority:   i % 10,
					maxRetries: 1,
					provider:   providers[gid%len(providers)],
					timeout:    100 * time.Millisecond,
					done:       make(chan struct{}),
				}
				
				// Try to add task
				startAdd := time.Now()
				AddTask(task, &logger)
				addDuration := time.Since(startAdd)
				
				if addDuration > 100*time.Millisecond {
					atomic.AddInt32(&failCount, 1)
					t.Logf("Slow AddTask: %v", addDuration)
				} else {
					atomic.AddInt32(&successCount, 1)
				}
			}
		}(g)
	}
	
	wg.Wait()
	elapsed := time.Since(start)
	
	totalTasks := numGoroutines * tasksPerGoroutine
	throughput := float64(totalTasks) / elapsed.Seconds()
	
	t.Logf("Stress test completed:")
	t.Logf("  Total tasks: %d", totalTasks)
	t.Logf("  Duration: %v", elapsed)
	t.Logf("  Throughput: %.2f tasks/sec", throughput)
	t.Logf("  Fast adds: %d", atomic.LoadInt32(&successCount))
	t.Logf("  Slow adds: %d", atomic.LoadInt32(&failCount))
	
	if atomic.LoadInt32(&failCount) > int32(totalTasks/10) {
		t.Errorf("Too many slow adds: %d out of %d", atomic.LoadInt32(&failCount), totalTasks)
	}
}