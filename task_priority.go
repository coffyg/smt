package smt

import (
	"container/heap"
)

// Task priority queue (task_priority.go)
type TaskWithPriority struct {
	task     ITask
	priority int
	index    int // The index is needed by the heap.Interface methods.
}

// TaskQueuePrio implements heap.Interface for task prioritization
// Using a binary heap with cache-friendly operations
type TaskQueuePrio []*TaskWithPriority

// Len returns the length of the queue
func (tq TaskQueuePrio) Len() int { 
	return len(tq) 
}

// Less compares priority values - higher values have higher priority
func (tq TaskQueuePrio) Less(i, j int) bool {
	// Higher priority tasks come first
	if tq[i].priority == tq[j].priority {
		// If priorities are equal, older tasks come first 
		// (assuming lower indexes were added earlier)
		return tq[i].index < tq[j].index
	}
	return tq[i].priority > tq[j].priority
}

// Swap swaps two elements and updates their indices
func (tq TaskQueuePrio) Swap(i, j int) {
	tq[i], tq[j] = tq[j], tq[i]
	tq[i].index = i
	tq[j].index = j
}

// Push adds a task to the priority queue
func (tq *TaskQueuePrio) Push(x interface{}) {
	n := len(*tq)
	// Preallocate when growing near capacity
	capNeeded := cap(*tq)
	if n >= capNeeded {
		// Grow to at least 1.5x (better than Go default 2x for this use case)
		newCap := capNeeded*3/2 + 1
		if capNeeded == 0 {
			newCap = 8 // Start with 8 for very small queues
		}
		// Create new slice with increased capacity
		newSlice := make([]*TaskWithPriority, n, newCap)
		copy(newSlice, *tq)
		*tq = newSlice
	}
	
	item := x.(*TaskWithPriority)
	item.index = n
	*tq = append(*tq, item)
}

// Pop removes and returns the highest priority task
func (tq *TaskQueuePrio) Pop() interface{} {
	old := *tq
	n := len(old)
	item := old[n-1]
	item.index = -1 // For safety
	// Clear pointer to help GC for large objects
	old[n-1] = nil
	*tq = old[0 : n-1]
	
	// The returned item will be handled by the consumer,
	// who is responsible for returning it to the pool when done
	return item
}

// Update modifies the priority of a task in the queue
// More efficient than removing and re-adding
func (tq *TaskQueuePrio) Update(item *TaskWithPriority, priority int) {
	item.priority = priority
	// After changing the priority, we need to restore the heap property
	heap.Fix(tq, item.index)
}
