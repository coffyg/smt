package smt

import (
	"container/heap"
	"testing"
)

// BenchmarkTaskQueuePrio benchmarks priority queue operations
func BenchmarkTaskQueuePrio(b *testing.B) {
	// Create a pool for task objects
	pool := NewTaskWithPriorityPool()
	
	// Run with different queue sizes to test scalability
	queueSizes := []int{10, 100, 1000}
	
	for _, size := range queueSizes {
		// Setup tasks with different priorities
		tasks := make([]*MockTask, size)
		for i := 0; i < size; i++ {
			tasks[i] = &MockTask{id: string(rune(i))}
		}
		
		b.Run("Push-"+string(rune(size)), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				queue := make(TaskQueuePrio, 0, size)
				heap.Init(&queue)
				
				for j := 0; j < size; j++ {
					// Get item from pool
					item := pool.Get()
					item.task = tasks[j]
					item.priority = j % 10 // Priorities from 0 to 9
					
					heap.Push(&queue, item)
				}
				
				// Return all to pool
				for j := 0; j < size; j++ {
					item := heap.Pop(&queue).(*TaskWithPriority)
					pool.Put(item)
				}
			}
		})
		
		b.Run("PopTask-"+string(rune(size)), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// Setup
				queue := make(TaskQueuePrio, 0, size)
				heap.Init(&queue)
				
				for j := 0; j < size; j++ {
					item := pool.GetWithTask(tasks[j], j%10)
					heap.Push(&queue, item)
				}
				
				// Benchmark optimized PopTask vs heap.Pop
				for j := 0; j < size; j++ {
					_, _ = queue.PopTask(pool)
				}
			}
		})
		
		b.Run("BatchRemove-"+string(rune(size)), func(b *testing.B) {
			batchSizes := []int{10, size / 2, size}
			
			for _, batchSize := range batchSizes {
				if batchSize > size {
					batchSize = size
				}
				
				b.Run(string(rune(batchSize)), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						// Setup
						queue := make(TaskQueuePrio, 0, size)
						heap.Init(&queue)
						
						for j := 0; j < size; j++ {
							item := pool.GetWithTask(tasks[j], j%10)
							heap.Push(&queue, item)
						}
						
						// Benchmark BatchRemove
						queue.BatchRemove(batchSize, pool)
					}
				})
			}
		})
	}
}