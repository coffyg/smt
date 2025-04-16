package smt

import (
	"container/heap"
)

// Task priority queue (task_priority.go)
type TaskWithPriority struct {
	task     ITask
	priority int
	index    int    // The index is needed by the heap.Interface methods.
	sortKey  uint64 // Pre-computed sort key for faster comparisons
}

// ComputeSortKey creates a sortable key that combines priority and timestamp
// Higher priorities will have higher sort keys
// Within the same priority, lower indices (older tasks) will have higher sort keys
func ComputeSortKey(priority, index int) uint64 {
	// Shift priority to higher bits (assuming priority < 2^32)
	// This ensures priority is the primary sort key
	priorityBits := uint64(priority) << 32
	
	// Index is negated and masked to lower 32 bits
	// This ensures that lower indices (older tasks) get higher sort values
	indexBits := uint64(0xFFFFFFFF - uint32(index))
	
	// Combine the two parts
	return priorityBits | indexBits
}

// Fast paths for common priority values
const (
	// Cache sizes 
	highPriorityCacheSize  = 64  // Cache for priorities >= highPriorityThreshold
	normalPriorityCacheSize = 128 // Cache for normal priorities
	lowPriorityCacheSize   = 32  // Cache for priorities < lowPriorityThreshold
	
	// Priority thresholds
	highPriorityThreshold = 90
	lowPriorityThreshold  = 10
)

// TaskQueuePrio implements heap.Interface for task prioritization
// Using a binary heap with cache-friendly operations
type TaskQueuePrio []*TaskWithPriority

// Len returns the length of the queue
func (tq TaskQueuePrio) Len() int { 
	return len(tq) 
}

// Less compares priority values using pre-computed sort keys
// This is much faster than comparing both priority and index separately
func (tq TaskQueuePrio) Less(i, j int) bool {
	// Higher sort keys should come first (sort in descending order)
	return tq[i].sortKey > tq[j].sortKey
}

// Swap swaps two elements and updates their indices
func (tq TaskQueuePrio) Swap(i, j int) {
	tq[i], tq[j] = tq[j], tq[i]
	
	// Update indices
	tq[i].index = i
	tq[j].index = j
}

// Push adds a task to the priority queue with optimized capacity management
func (tq *TaskQueuePrio) Push(x interface{}) {
	n := len(*tq)
	// Preallocate when growing near capacity
	capNeeded := cap(*tq)
	if n >= capNeeded {
		// Initial capacity for better performance
		newCap := 64
		if capNeeded > 0 {
			// Grow to 1.5x (better than Go default 2x for most use cases)
			// Add more padding for high throughput scenarios
			newCap = capNeeded*3/2 + 16
		}
		// Create new slice with increased capacity
		newSlice := make([]*TaskWithPriority, n, newCap)
		// Fast copy of slice contents
		copy(newSlice, *tq)
		*tq = newSlice
	}
	
	item := x.(*TaskWithPriority)
	item.index = n
	
	// Compute sort key for faster heap operations
	if item.sortKey == 0 { // Only compute if not already set
		item.sortKey = ComputeSortKey(item.priority, n)
	}
	
	*tq = append(*tq, item)
}

// Pop removes and returns the highest priority task
func (tq *TaskQueuePrio) Pop() interface{} {
	old := *tq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // For safety and GC
	item.index = -1 // For safety
	*tq = old[0 : n-1]
	return item
}

// Update modifies the priority of a task in the queue
// More efficient than removing and re-adding
func (tq *TaskQueuePrio) Update(item *TaskWithPriority, priority int) {
	// Only update if priority actually changed
	if item.priority == priority {
		return
	}
	
	// Update priority and recompute sort key
	item.priority = priority
	item.sortKey = ComputeSortKey(priority, item.index)
	
	// After changing the priority, we need to restore the heap property
	heap.Fix(tq, item.index)
}

// PeekHighestPriority returns the highest priority task without removing it
// This is a constant-time operation since the heap keeps highest priority at index 0
func (tq *TaskQueuePrio) PeekHighestPriority() (ITask, bool) {
	if len(*tq) == 0 {
		return nil, false
	}
	return (*tq)[0].task, true
}

// taskCache is a simple cache for high, normal, and low priority tasks
// to avoid heap operations for common scenarios
type taskCache struct {
	highPriority  [highPriorityCacheSize]*TaskWithPriority
	normalPriority [normalPriorityCacheSize]*TaskWithPriority
	lowPriority   [lowPriorityCacheSize]*TaskWithPriority
	highCount, normalCount, lowCount int
}

// shared global task cache
var globalTaskCache = &taskCache{}

// PopTask returns the highest priority task and removes it from the queue
// Optimized with fast paths for common priorities and reduced heap operations
func (tq *TaskQueuePrio) PopTask(pool *TaskWithPriorityPool) (ITask, bool) {
	if len(*tq) == 0 {
		return nil, false
	}
	
	// Fast path: Check if we have high priority tasks in the cache
	if globalTaskCache.highCount > 0 {
		// Use a high priority task from cache
		globalTaskCache.highCount--
		taskWithPriority := globalTaskCache.highPriority[globalTaskCache.highCount]
		globalTaskCache.highPriority[globalTaskCache.highCount] = nil // Clear reference
		task := taskWithPriority.task
		
		// Return the wrapper to the pool for reuse
		if pool != nil {
			pool.Put(taskWithPriority)
		}
		
		return task, true
	}
	
	// Get the highest priority task from the heap
	taskWithPriority := heap.Pop(tq).(*TaskWithPriority)
	task := taskWithPriority.task
	
	// Prefetch the next few tasks based on priority to reduce future heap operations
	// This significantly improves performance for continuous task processing
	if len(*tq) > 0 {
		remainingLen := len(*tq)
		prefetchCount := 8 // Prefetch up to 8 tasks at once
		if remainingLen < prefetchCount {
			prefetchCount = remainingLen
		}
		
		for i := 0; i < prefetchCount; i++ {
			if len(*tq) == 0 {
				break
			}
			
			next := heap.Pop(tq).(*TaskWithPriority)
			
			// Add to appropriate cache based on priority
			if next.priority >= highPriorityThreshold && globalTaskCache.highCount < highPriorityCacheSize {
				globalTaskCache.highPriority[globalTaskCache.highCount] = next
				globalTaskCache.highCount++
			} else if next.priority >= lowPriorityThreshold && globalTaskCache.normalCount < normalPriorityCacheSize {
				globalTaskCache.normalPriority[globalTaskCache.normalCount] = next
				globalTaskCache.normalCount++
			} else if globalTaskCache.lowCount < lowPriorityCacheSize {
				globalTaskCache.lowPriority[globalTaskCache.lowCount] = next
				globalTaskCache.lowCount++
			} else {
				// Cache full, push back to heap
				heap.Push(tq, next)
				break
			}
		}
	}
	
	// Return the wrapper to the pool for reuse
	if pool != nil {
		pool.Put(taskWithPriority)
	}
	
	return task, true
}

// BatchRemove removes multiple lowest priority tasks from the queue at once
// This is useful for bulk cleanup operations
func (tq *TaskQueuePrio) BatchRemove(count int, pool *TaskWithPriorityPool) {
	n := len(*tq)
	if n == 0 || count <= 0 {
		return
	}
	
	// Fast path: If removing all items, just clear the queue entirely
	if count >= n {
		// Return all objects to pool if provided
		if pool != nil {
			for i := 0; i < n; i++ {
				item := (*tq)[i]
				item.index = -1
				pool.Put(item)
				(*tq)[i] = nil // Clear for GC
			}
		}
		
		// Reset slice to empty
		*tq = (*tq)[:0]
		return
	}
	
	// Fast path: If removing from the cache first would be more efficient
	totalCached := globalTaskCache.highCount + globalTaskCache.normalCount + globalTaskCache.lowCount
	if count <= totalCached {
		// Remove from caches first
		remaining := count
		
		// Start with low priority cache
		if remaining > 0 && globalTaskCache.lowCount > 0 {
			toRemove := remaining
			if toRemove > globalTaskCache.lowCount {
				toRemove = globalTaskCache.lowCount
			}
			
			// Return items to pool
			if pool != nil {
				for i := 0; i < toRemove; i++ {
					globalTaskCache.lowCount--
					item := globalTaskCache.lowPriority[globalTaskCache.lowCount]
					item.index = -1
					pool.Put(item)
					globalTaskCache.lowPriority[globalTaskCache.lowCount] = nil // Clear for GC
				}
			} else {
				// Just clear the cache
				for i := 0; i < toRemove; i++ {
					globalTaskCache.lowCount--
					globalTaskCache.lowPriority[globalTaskCache.lowCount] = nil
				}
			}
			
			remaining -= toRemove
		}
		
		// Then normal priority cache if needed
		if remaining > 0 && globalTaskCache.normalCount > 0 {
			toRemove := remaining
			if toRemove > globalTaskCache.normalCount {
				toRemove = globalTaskCache.normalCount
			}
			
			// Return items to pool
			if pool != nil {
				for i := 0; i < toRemove; i++ {
					globalTaskCache.normalCount--
					item := globalTaskCache.normalPriority[globalTaskCache.normalCount]
					item.index = -1
					pool.Put(item)
					globalTaskCache.normalPriority[globalTaskCache.normalCount] = nil // Clear for GC
				}
			} else {
				// Just clear the cache
				for i := 0; i < toRemove; i++ {
					globalTaskCache.normalCount--
					globalTaskCache.normalPriority[globalTaskCache.normalCount] = nil
				}
			}
			
			remaining -= toRemove
		}
		
		// Finally, high priority cache if still needed
		if remaining > 0 && globalTaskCache.highCount > 0 {
			toRemove := remaining
			if toRemove > globalTaskCache.highCount {
				toRemove = globalTaskCache.highCount
			}
			
			// Return items to pool
			if pool != nil {
				for i := 0; i < toRemove; i++ {
					globalTaskCache.highCount--
					item := globalTaskCache.highPriority[globalTaskCache.highCount]
					item.index = -1
					pool.Put(item)
					globalTaskCache.highPriority[globalTaskCache.highCount] = nil // Clear for GC
				}
			} else {
				// Just clear the cache
				for i := 0; i < toRemove; i++ {
					globalTaskCache.highCount--
					globalTaskCache.highPriority[globalTaskCache.highCount] = nil
				}
			}
		}
		
		return
	}
	
	// Standard path: Remove from the heap
	// Return objects to pool if provided
	if pool != nil {
		for i := 0; i < count; i++ {
			item := (*tq)[n-i-1]
			item.index = -1
			pool.Put(item)
			(*tq)[n-i-1] = nil // Clear for GC
		}
	}
	
	// Resize the slice
	*tq = (*tq)[:n-count]
}