package smt

import (
	"sync"
	"testing"
	"time"
)

// Test basic pooling functionality
func TestTaskWithPriorityPool(t *testing.T) {
	pool := NewTaskWithPriorityPool()
	
	// Get an object from the pool
	tp1 := pool.Get()
	if tp1 == nil {
		t.Fatal("Pool should never return nil")
	}
	
	// Initialize it
	task := &MockTask{
		id:         "test-task-1",
		priority:   5,
		maxRetries: 3,
		createdAt:  time.Now(),
	}
	tp1.task = task
	tp1.priority = 5
	
	// Return it to the pool
	pool.Put(tp1)
	
	// Get another object (should be the same one)
	tp2 := pool.Get()
	if tp2.task != nil {
		t.Error("Pool should reset task to nil")
	}
	if tp2.priority != 0 {
		t.Error("Pool should reset priority to 0")
	}
}

// Test convenience method
func TestTaskWithPriorityPoolGetWithTask(t *testing.T) {
	pool := NewTaskWithPriorityPool()
	
	task := &MockTask{
		id:         "test-task-2",
		priority:   7,
		maxRetries: 3,
		createdAt:  time.Now(),
	}
	
	// Get an initialized object from the pool
	tp := pool.GetWithTask(task, 7)
	
	if tp.task != task {
		t.Error("GetWithTask should set the task")
	}
	if tp.priority != 7 {
		t.Error("GetWithTask should set the priority")
	}
}

// Benchmark pool performance against regular allocation
func BenchmarkTaskWithPriorityPool(b *testing.B) {
	pool := NewTaskWithPriorityPool()
	task := &MockTask{id: "benchmark-task"}
	
	b.Run("WithPool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tp := pool.GetWithTask(task, i%10)
			pool.Put(tp)
		}
	})
	
	b.Run("WithoutPool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = &TaskWithPriority{task: task, priority: i % 10}
			// No reuse, object will be garbage collected
		}
	})
}

// Test concurrent usage of the pool
func TestTaskWithPriorityPoolConcurrent(t *testing.T) {
	pool := NewTaskWithPriorityPool()
	const goroutines = 100
	const iterations = 1000
	
	var wg sync.WaitGroup
	wg.Add(goroutines)
	
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			
			task := &MockTask{id: "concurrent-task"}
			for i := 0; i < iterations; i++ {
				tp := pool.GetWithTask(task, i%10)
				// Simply verify the object is usable
				if tp.task == nil {
					t.Error("Task should not be nil")
				}
				pool.Put(tp)
			}
		}(g)
	}
	
	wg.Wait()
}