package smt

import (
	"container/heap"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// Basic benchmark for adding tasks
func BenchmarkAddTask(b *testing.B) {
	// Setup
	logger := zerolog.Nop()
	
	providerName := "benchProvider"
	provider := &MockProvider{name: providerName}
	provider.handleFunc = func(task ITask, server string) error {
		return nil
	}
	
	providers := []IProvider{provider}
	servers := map[string][]string{
		providerName: {"server1", "server2", "server3", "server4"},
	}
	
	getTimeout := func(string, string) time.Duration {
		return time.Second * 5
	}

	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()
	
	b.ResetTimer()
	
	// Benchmark
	for i := 0; i < b.N; i++ {
		task := &MockTask{
			id:         fmt.Sprintf("task%d", i),
			priority:   i % 10,
			maxRetries: 3,
			createdAt:  time.Now(),
			provider:   provider,
			timeout:    time.Second * 5,
		}
		tm.AddTask(task)
	}
}

// Benchmark task processing throughput
func BenchmarkTaskProcessing(b *testing.B) {
	// Setup
	logger := zerolog.Nop()
	
	providerName := "benchProvider"
	provider := &MockProvider{name: providerName}
	
	// Use a wait group to track task completion
	var wg sync.WaitGroup
	
	provider.handleFunc = func(task ITask, server string) error {
		// Simulate very fast processing
		time.Sleep(time.Millisecond)
		wg.Done()
		return nil
	}
	
	providers := []IProvider{provider}
	servers := map[string][]string{
		providerName: {"server1", "server2", "server3", "server4"},
	}
	
	getTimeout := func(string, string) time.Duration {
		return time.Second * 5
	}

	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()
	
	b.ResetTimer()
	
	// Add tasks to be processed
	wg.Add(b.N)
	for i := 0; i < b.N; i++ {
		task := &MockTask{
			id:         fmt.Sprintf("task%d", i),
			priority:   i % 10,
			maxRetries: 3,
			createdAt:  time.Now(),
			provider:   provider,
			timeout:    time.Second * 5,
		}
		tm.AddTask(task)
	}
	
	// Wait for all tasks to complete
	wg.Wait()
}

// Benchmark concurrent task processing with many servers
func BenchmarkConcurrentTaskProcessing(b *testing.B) {
	// Setup
	logger := zerolog.Nop()
	
	// Create multiple providers
	numProviders := 5
	var providers []IProvider
	servers := make(map[string][]string)
	
	// Use a wait group to track task completion
	var wg sync.WaitGroup
	
	for i := 0; i < numProviders; i++ {
		providerName := fmt.Sprintf("benchProvider%d", i)
		provider := &MockProvider{name: providerName}
		
		provider.handleFunc = func(task ITask, server string) error {
			// Simulate fast processing with some variability
			time.Sleep(time.Millisecond * time.Duration(1+i%5))
			wg.Done()
			return nil
		}
		
		providers = append(providers, provider)
		
		// Create multiple servers for each provider
		serverList := make([]string, 0)
		for j := 0; j < 3; j++ {
			serverList = append(serverList, fmt.Sprintf("server%d_%d", i, j))
		}
		servers[providerName] = serverList
	}
	
	getTimeout := func(string, string) time.Duration {
		return time.Second * 5
	}

	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()
	
	b.ResetTimer()
	
	// Add tasks to be processed across all providers
	wg.Add(b.N)
	for i := 0; i < b.N; i++ {
		providerIdx := i % numProviders
		provider := providers[providerIdx]
		
		task := &MockTask{
			id:         fmt.Sprintf("task%d", i),
			priority:   i % 10,
			maxRetries: 3,
			createdAt:  time.Now(),
			provider:   provider,
			timeout:    time.Second * 5,
		}
		tm.AddTask(task)
	}
	
	// Wait for all tasks to complete
	wg.Wait()
}

// Benchmark priority queue operations
func BenchmarkTaskQueuePriority(b *testing.B) {
	pq := &TaskQueuePrio{}
	heap.Init(pq)
	
	// Create a bunch of tasks with random priorities
	tasks := make([]*TaskWithPriority, b.N)
	for i := 0; i < b.N; i++ {
		tasks[i] = &TaskWithPriority{
			task:     &MockTask{id: fmt.Sprintf("task%d", i)},
			priority: i % 10, // priorities 0-9
		}
	}
	
	b.ResetTimer()
	
	// Push all tasks to the queue
	for i := 0; i < b.N; i++ {
		heap.Push(pq, tasks[i])
	}
	
	// Pop all tasks from the queue
	for i := 0; i < b.N; i++ {
		_ = heap.Pop(pq)
	}
}

// Benchmark command queue operations
func BenchmarkCommandQueue(b *testing.B) {
	queue := NewCommandQueue(b.N / 2) // Initial capacity half of N
	
	b.ResetTimer()
	
	// Enqueue operations
	for i := 0; i < b.N; i++ {
		cmd := Command{
			id: uuid.New(),
			commandFunc: func(server string) error {
				return nil
			},
		}
		queue.Enqueue(cmd)
	}
	
	// Dequeue operations
	for i := 0; i < b.N; i++ {
		_, _ = queue.Dequeue()
	}
}

// Benchmark high concurrency with realistic workload
func BenchmarkHighConcurrencyRealisticWorkload(b *testing.B) {
	// Setup
	logger := zerolog.Nop()
	
	// Create providers
	providerNames := []string{"provider1", "provider2", "provider3"}
	var providers []IProvider
	providerHandleFuncs := make(map[string]func(task ITask, server string) error)
	
	// Use a wait group to track task completion
	var wg sync.WaitGroup
	
	for _, name := range providerNames {
		provider := &MockProvider{name: name}
		providers = append(providers, provider)
		
		// Create handle function for this provider
		providerHandleFuncs[name] = func(task ITask, server string) error {
			// Simulate variable processing time based on task ID
			taskNum, _ := strconv.Atoi(task.GetID()[4:])
			time.Sleep(time.Millisecond * time.Duration(1+taskNum%10))
			wg.Done()
			return nil
		}
		provider.handleFunc = providerHandleFuncs[name]
	}
	
	// Define servers for each provider
	servers := map[string][]string{
		"provider1": {"server1", "server2", "server3", "server4"},
		"provider2": {"server5", "server6", "server7", "server8"},
		"provider3": {"server9", "server10", "server11", "server12"},
	}
	
	getTimeout := func(string, string) time.Duration {
		return time.Second * 5
	}
	
	// Initialize TaskManager
	tm := NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()
	
	// Set server concurrency limits for half the servers
	tm.SetTaskManagerServerMaxParallel("server1", 2)
	tm.SetTaskManagerServerMaxParallel("server5", 3)
	tm.SetTaskManagerServerMaxParallel("server9", 4)
	
	b.ResetTimer()
	
	// Add tasks to be processed
	wg.Add(b.N)
	for i := 0; i < b.N; i++ {
		providerIdx := i % len(providers)
		provider := providers[providerIdx]
		
		// Create task with variable priority
		priority := 5
		if i%23 == 0 {
			priority = 9 // High priority occasionally
		} else if i%7 == 0 {
			priority = 7 // Medium-high priority sometimes
		}
		
		task := &MockTask{
			id:         fmt.Sprintf("task%d", i),
			priority:   priority,
			maxRetries: 3,
			createdAt:  time.Now(),
			provider:   provider,
			timeout:    time.Second * 5,
		}
		tm.AddTask(task)
	}
	
	// Wait for all tasks to complete
	wg.Wait()
}