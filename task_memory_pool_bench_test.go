package smt

import (
	"fmt"
	"sync"
	"testing"
	"time"
	
	"github.com/rs/zerolog"
)

// Compare memory allocations with and without the pool
func BenchmarkTaskWithPriorityPoolAllocations(b *testing.B) {
	// Test with default pool
	defaultPool := NewTaskWithPriorityPool()
	
	// Test with configured pool
	configuredPool := NewTaskWithPriorityPoolConfig(&PoolConfig{
		PreWarmSize:  5000,
		TrackStats:   true,
		PreWarmAsync: false,
	})
	
	b.Run("WithDefaultPoolGetPut", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tp := defaultPool.Get()
			tp.task = &MockTask{id: fmt.Sprintf("task%d", i)}
			tp.priority = i % 10
			defaultPool.Put(tp)
		}
	})
	
	b.Run("WithConfiguredPoolGetPut", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tp := configuredPool.Get()
			tp.task = &MockTask{id: fmt.Sprintf("task%d", i)}
			tp.priority = i % 10
			configuredPool.Put(tp)
		}
	})
	
	b.Run("WithDefaultPoolGetWithTask", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tp := defaultPool.GetWithTask(&MockTask{id: fmt.Sprintf("task%d", i)}, i%10)
			defaultPool.Put(tp)
		}
	})
	
	b.Run("WithConfiguredPoolGetWithTask", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tp := configuredPool.GetWithTask(&MockTask{id: fmt.Sprintf("task%d", i)}, i%10)
			configuredPool.Put(tp)
		}
	})
	
	b.Run("WithoutPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = &TaskWithPriority{
				task:     &MockTask{id: fmt.Sprintf("task%d", i)},
				priority: i % 10,
			}
		}
	})
}

// Test impact on AddTask performance
func BenchmarkAddTaskWithPool(b *testing.B) {
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
	
	// Create task manager with memory pool
	tm := NewTaskManagerWithPool(&providers, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()
	
	b.ResetTimer()
	b.ReportAllocs()
	
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
// BenchmarkHighConcurrencyPoolPerformance tests the pool under high concurrency conditions
func BenchmarkHighConcurrencyPoolPerformance(b *testing.B) {
	// Create pools with different configurations
	defaultPool := NewTaskWithPriorityPool()
	
	configuredPool := NewTaskWithPriorityPoolConfig(&PoolConfig{
		PreWarmSize:  10000,
		TrackStats:   true, 
		PreWarmAsync: false,
	})
	
	// Test with 100 goroutines hammering the pool
	b.Run("DefaultPool-HighConcurrency", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		
		// Use b.N for iteration count
		const goroutines = 100
		iterationsPerGoroutine := b.N / goroutines
		if iterationsPerGoroutine < 1 {
			iterationsPerGoroutine = 1
		}
		
		// Create a wait group to synchronize goroutines
		var wg sync.WaitGroup
		wg.Add(goroutines)
		
		// Launch goroutines
		for g := 0; g < goroutines; g++ {
			go func(id int) {
				defer wg.Done()
				
				// Each goroutine performs multiple Get/Put operations
				for i := 0; i < iterationsPerGoroutine; i++ {
					taskID := fmt.Sprintf("task-%d-%d", id, i)
					tp := defaultPool.GetWithTask(&MockTask{id: taskID}, i%10)
					defaultPool.Put(tp)
				}
			}(g)
		}
		
		// Wait for all goroutines to complete
		wg.Wait()
	})
	
	b.Run("ConfiguredPool-HighConcurrency", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		
		// Use b.N for iteration count
		const goroutines = 100
		iterationsPerGoroutine := b.N / goroutines
		if iterationsPerGoroutine < 1 {
			iterationsPerGoroutine = 1
		}
		
		// Create a wait group to synchronize goroutines
		var wg sync.WaitGroup
		wg.Add(goroutines)
		
		// Launch goroutines
		for g := 0; g < goroutines; g++ {
			go func(id int) {
				defer wg.Done()
				
				// Each goroutine performs multiple Get/Put operations
				for i := 0; i < iterationsPerGoroutine; i++ {
					taskID := fmt.Sprintf("task-%d-%d", id, i)
					tp := configuredPool.GetWithTask(&MockTask{id: taskID}, i%10)
					configuredPool.Put(tp)
				}
			}(g)
		}
		
		// Wait for all goroutines to complete
		wg.Wait()
		
		// Print stats after test
		gets, puts, misses, inUse, maxInUse := configuredPool.GetPoolStats()
		if b.N == 1 { // Only print in first run to avoid spamming output
			b.Logf("Pool Stats - Gets: %d, Puts: %d, Misses: %d, InUse: %d, MaxInUse: %d", 
				gets, puts, misses, inUse, maxInUse)
		}
	})
}
