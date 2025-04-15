package smt

import (
	"fmt"
	"sync"
	"testing"
)

// Test that the lock-free queue works correctly in a single-threaded scenario
func TestTaskQueueLockFree(t *testing.T) {
	queue := NewTaskQueueLockFree(4)
	
	// Add tasks with different priorities
	for i := 0; i < 100; i++ {
		task := &TaskWithPriority{
			task:     &MockTask{id: fmt.Sprintf("task%d", i)},
			priority: i % 10, // Priorities 0-9
		}
		queue.Push(task)
	}
	
	// Verify the queue length
	if queue.Len() != 100 {
		t.Errorf("Expected queue length 100, got %d", queue.Len())
	}
	
	// Pop all tasks and verify they come out in priority order
	prevPriority := 10 // Higher than any actual priority
	for i := 0; i < 100; i++ {
		task := queue.Pop()
		if task == nil {
			t.Errorf("Expected task at position %d, got nil", i)
			break
		}
		
		// Verify priority is descending (or equal)
		if task.priority > prevPriority {
			t.Errorf("Task %d has priority %d, which is higher than previous priority %d", 
				i, task.priority, prevPriority)
		}
		prevPriority = task.priority
	}
	
	// Verify queue is empty
	if queue.Len() != 0 {
		t.Errorf("Expected empty queue, got length %d", queue.Len())
	}
}

// Test that the lock-free queue works correctly in a multi-threaded scenario
func TestTaskQueueLockFreeConcurrent(t *testing.T) {
	queue := NewTaskQueueLockFree(4)
	
	// Use 4 goroutines to add tasks
	const numTasks = 400
	const numGoroutines = 4
	tasksPerGoroutine := numTasks / numGoroutines
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	
	// Launch producers
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			
			startIdx := goroutineID * tasksPerGoroutine
			endIdx := startIdx + tasksPerGoroutine
			
			for i := startIdx; i < endIdx; i++ {
				task := &TaskWithPriority{
					task:     &MockTask{id: fmt.Sprintf("task%d", i)},
					priority: i % 10, // Priorities 0-9
				}
				queue.Push(task)
			}
		}(g)
	}
	
	// Wait for all tasks to be added
	wg.Wait()
	
	// Verify the queue length
	if queue.Len() != numTasks {
		t.Errorf("Expected queue length %d, got %d", numTasks, queue.Len())
	}
	
	// Single thread popping to test correctness
	poppedCount := 0
	for {
		task := queue.Pop()
		if task == nil {
			// No more tasks
			break
		}
		poppedCount++
	}
	
	// Verify we popped all tasks
	if poppedCount != numTasks {
		t.Errorf("Expected to pop %d tasks, popped %d", numTasks, poppedCount)
	}
	
	// Verify queue is empty
	if queue.Len() != 0 {
		t.Errorf("Expected empty queue, got length %d", queue.Len())
	}
}

// Benchmark the lock-free queue
func BenchmarkTaskQueueLockFree(b *testing.B) {
	queue := NewTaskQueueLockFree(4)
	
	// Pre-create some tasks
	const numTasks = 1000
	tasks := make([]*TaskWithPriority, numTasks)
	for i := 0; i < numTasks; i++ {
		tasks[i] = &TaskWithPriority{
			task:     &MockTask{id: fmt.Sprintf("task%d", i)},
			priority: i % 10, // Priorities 0-9
		}
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		taskIdx := i % numTasks
		queue.Push(tasks[taskIdx])
		_ = queue.Pop()
	}
}

// Benchmark the lock-free queue with concurrent operations
func BenchmarkTaskQueueLockFreeConcurrent(b *testing.B) {
	queue := NewTaskQueueLockFree(8)
	
	// Pre-create some tasks to avoid allocation during the benchmark
	const numTasks = 1000
	tasks := make([]*TaskWithPriority, numTasks)
	for i := 0; i < numTasks; i++ {
		tasks[i] = &TaskWithPriority{
			task:     &MockTask{id: fmt.Sprintf("task%d", i)},
			priority: i % 10, // Priorities 0-9
		}
	}
	
	b.ResetTimer()
	
	// Use multiple goroutines to push and pop
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			taskIdx := i % numTasks
			queue.Push(tasks[taskIdx])
			// We don't need to Pop in every iteration to avoid contention
			if i%10 == 0 {
				_ = queue.Pop()
			}
			i++
		}
	})
}