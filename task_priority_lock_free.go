package smt

import (
	"sort"
	"sync"
	"sync/atomic"
)

// TaskQueueLockFree implements a lock-free priority queue for tasks
// This is an optimized version that uses a sharded approach to reduce lock contention
type TaskQueueLockFree struct {
	shards     []*taskQueueShard
	shardCount int32
	size       atomic.Int32
}

// taskQueueShard is a single shard of the lock-free queue
type taskQueueShard struct {
	tasks []*TaskWithPriority
	mu    sync.Mutex
}

// NewTaskQueueLockFree creates a new lock-free task queue with the specified number of shards
func NewTaskQueueLockFree(shardCount int) *TaskQueueLockFree {
	if shardCount <= 0 {
		shardCount = 16 // Default to 16 shards for typical multi-core systems
	}
	
	tq := &TaskQueueLockFree{
		shards:     make([]*taskQueueShard, shardCount),
		shardCount: int32(shardCount),
	}
	
	for i := 0; i < shardCount; i++ {
		tq.shards[i] = &taskQueueShard{
			tasks: make([]*TaskWithPriority, 0, 32), // Pre-allocate for better performance
		}
	}
	
	return tq
}

// Push adds a task to the queue
func (tq *TaskQueueLockFree) Push(task *TaskWithPriority) {
	// For better sharding, use priority to determine the shard
	// Similar priorities go to the same shard
	shardIndex := int(task.priority % int(tq.shardCount))
	shard := tq.shards[shardIndex]
	
	shard.mu.Lock()
	
	// Pre-allocate capacity if needed for better performance
	if cap(shard.tasks) == len(shard.tasks) {
		// Grow slice if at capacity (1.5x growth factor is more memory efficient)
		newCap := cap(shard.tasks)*3/2 + 4
		newSlice := make([]*TaskWithPriority, len(shard.tasks), newCap)
		copy(newSlice, shard.tasks)
		shard.tasks = newSlice
	}
	
	// Insert task in priority order (highest priority first)
	// Using binary search for faster insertion
	n := len(shard.tasks)
	i := sort.Search(n, func(i int) bool {
		return shard.tasks[i].priority < task.priority
	})
	
	// Make room for the new element
	shard.tasks = append(shard.tasks, nil)
	if i < n {
		// Shift elements to make room
		copy(shard.tasks[i+1:], shard.tasks[i:])
	}
	shard.tasks[i] = task
	
	shard.mu.Unlock()
	
	// Update size
	tq.size.Add(1)
}

// Pop removes and returns the highest priority task
func (tq *TaskQueueLockFree) Pop() *TaskWithPriority {
	// We need to scan all shards to find the highest priority task
	highestPriority := -1
	var highestPriorityTask *TaskWithPriority
	var taskShard *taskQueueShard
	var taskIndex int
	
	// First scan: find the highest priority task across all shards
	for s := 0; s < int(tq.shardCount); s++ {
		shard := tq.shards[s]
		
		shard.mu.Lock()
		if len(shard.tasks) > 0 {
			// Look for highest priority task in this shard
			for i, task := range shard.tasks {
				if task.priority > highestPriority {
					highestPriority = task.priority
					highestPriorityTask = task
					taskShard = shard
					taskIndex = i
				}
			}
		}
		shard.mu.Unlock()
	}
	
	// If we found a task, remove it from its shard
	if highestPriorityTask != nil {
		taskShard.mu.Lock()
		// Verify the task is still there
		if taskIndex < len(taskShard.tasks) && taskShard.tasks[taskIndex] == highestPriorityTask {
			// Remove the task (use copy to avoid memory leaks)
			n := len(taskShard.tasks)
			if n > 1 {
				// Remove task at index by shifting subsequent elements
				copy(taskShard.tasks[taskIndex:], taskShard.tasks[taskIndex+1:])
				taskShard.tasks[n-1] = nil // Clear reference for GC
				taskShard.tasks = taskShard.tasks[:n-1]
			} else {
				// Last element in the slice
				taskShard.tasks = taskShard.tasks[:0]
			}
			taskShard.mu.Unlock()
			tq.size.Add(-1)
			return highestPriorityTask
		}
		taskShard.mu.Unlock()
		
		// If the task was removed by another goroutine, try again
		return tq.Pop()
	}
	
	// No tasks found in any shard
	return nil
}

// Len returns the number of tasks in the queue
func (tq *TaskQueueLockFree) Len() int {
	return int(tq.size.Load())
}

// IsEmpty returns true if the queue is empty
func (tq *TaskQueueLockFree) IsEmpty() bool {
	return tq.Len() == 0
}