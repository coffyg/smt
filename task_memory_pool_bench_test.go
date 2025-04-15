package smt

import (
	"fmt"
	"testing"
	"time"
	
	"github.com/rs/zerolog"
)

// Compare memory allocations with and without the pool
func BenchmarkTaskWithPriorityPoolAllocations(b *testing.B) {
	pool := NewTaskWithPriorityPool()
	
	b.Run("WithPoolGetPut", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tp := pool.Get()
			tp.task = &MockTask{id: fmt.Sprintf("task%d", i)}
			tp.priority = i % 10
			pool.Put(tp)
		}
	})
	
	b.Run("WithPoolGetWithTask", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tp := pool.GetWithTask(&MockTask{id: fmt.Sprintf("task%d", i)}, i%10)
			pool.Put(tp)
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