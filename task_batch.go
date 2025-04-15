package smt

import (
	"sync"
	"time"
)

// BatchableTask extends ITask interface for tasks that can be batched
type BatchableTask interface {
	ITask
	// BatchKey returns a key used for batching similar tasks
	// Tasks with the same batch key can be processed together
	BatchKey() string
	// CanBatchWith returns true if this task can be batched with other task
	CanBatchWith(other BatchableTask) bool
	// MergeWith combines this task with another task where possible
	MergeWith(other BatchableTask) BatchableTask
}

// TaskBatch represents a group of tasks that can be processed together
type TaskBatch struct {
	tasks       []BatchableTask
	lock        sync.Mutex      // protects all fields below
	batchKey    string
	createdAt   time.Time
	maxSize     int
	maxWaitTime time.Duration
	timer       *time.Timer
	processed   bool            // flag to track if batch has been processed
	processFn   func(batch *TaskBatch)
	priority    int // Highest priority of any task in the batch
}

// NewTaskBatch creates a new batch of tasks
func NewTaskBatch(initialTask BatchableTask, maxSize int, maxWaitTime time.Duration, processFn func(batch *TaskBatch)) *TaskBatch {
	batch := &TaskBatch{
		tasks:       make([]BatchableTask, 0, maxSize),
		batchKey:    initialTask.BatchKey(),
		createdAt:   time.Now(),
		maxSize:     maxSize,
		maxWaitTime: maxWaitTime,
		processFn:   processFn,
		priority:    initialTask.GetPriority(),
		lock:        sync.Mutex{}, // Initialize the mutex explicitly
		processed:   false,
	}
	
	// Add initial task
	batch.tasks = append(batch.tasks, initialTask)
	
	// We need to make sure the batch reference is complete before starting the timer
	// Create a local copy to avoid race conditions with the timer callback
	timerBatch := batch
	
	// Start timer for max wait time in a more thread-safe way
	batch.lock.Lock()
	batch.timer = time.AfterFunc(maxWaitTime, func() {
		// Process the batch via closure capturing the batch reference
		timerBatch.lock.Lock()
		
		// Don't process if already processed
		if timerBatch.processed {
			timerBatch.lock.Unlock()
			return
		}
		
		// Mark as processed
		timerBatch.processed = true
		
		// Stop the timer
		if timerBatch.timer != nil {
			timerBatch.timer.Stop()
			timerBatch.timer = nil
		}
		
		// Get tasks safely before unlocking
		tasks := timerBatch.tasks
		processFn := timerBatch.processFn
		
		// Unlock before processing to avoid deadlock
		timerBatch.lock.Unlock()
		
		// Process tasks outside of lock if there are any and we have a process function
		if len(tasks) > 0 && processFn != nil {
			processFn(timerBatch)
		}
	})
	batch.lock.Unlock()
	
	return batch
}

// Add attempts to add a task to the batch
func (b *TaskBatch) Add(task BatchableTask) bool {
	if task.BatchKey() != b.batchKey {
		return false
	}
	
	b.lock.Lock()
	
	// Don't add to already processed batches
	if b.processed {
		b.lock.Unlock()
		return false
	}
	
	// Check if batch is full
	if len(b.tasks) >= b.maxSize {
		b.lock.Unlock()
		return false
	}
	
	// Check if any task can't batch with this new task
	for _, existingTask := range b.tasks {
		if !existingTask.CanBatchWith(task) {
			b.lock.Unlock()
			return false
		}
	}
	
	// Update highest priority
	if task.GetPriority() > b.priority {
		b.priority = task.GetPriority()
	}
	
	// Add task to batch
	b.tasks = append(b.tasks, task)
	
	// Check if batch is now full
	isFull := len(b.tasks) >= b.maxSize
	
	if isFull {
		// Mark as processed before we start processing
		b.processed = true
		
		// Stop the timer
		if b.timer != nil {
			b.timer.Stop()
			b.timer = nil
		}
	}
	
	// Need local copy for goroutine to avoid race conditions
	var processFn func(batch *TaskBatch)
	var batchToProcess *TaskBatch
	
	if isFull {
		processFn = b.processFn
		batchToProcess = b
	}
	
	// Release lock
	b.lock.Unlock()
	
	// Process in background if batch is full
	if isFull && processFn != nil {
		go processFn(batchToProcess)
	}
	
	return true
}

// Process processes the batch
func (b *TaskBatch) Process() {
	// Acquire lock
	b.lock.Lock()
	
	// If already processed, return early
	if b.processed {
		b.lock.Unlock()
		return
	}
	
	// Mark as processed
	b.processed = true
	
	// Stop the timer to avoid calling Process twice
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	
	// Copy necessary data before releasing the lock
	tasks := b.tasks
	processFn := b.processFn
	
	// Release the lock before processing to avoid deadlocks
	b.lock.Unlock()
	
	// Don't process empty batches
	if len(tasks) == 0 {
		return
	}
	
	// Call the process function outside of the lock
	if processFn != nil {
		processFn(b)
	}
}

// Tasks returns a copy of the tasks in the batch
func (b *TaskBatch) Tasks() []BatchableTask {
	b.lock.Lock()
	defer b.lock.Unlock()
	
	result := make([]BatchableTask, len(b.tasks))
	copy(result, b.tasks)
	return result
}

// Size returns the number of tasks in the batch
func (b *TaskBatch) Size() int {
	b.lock.Lock()
	defer b.lock.Unlock()
	
	return len(b.tasks)
}

// Priority returns the highest priority of any task in the batch
func (b *TaskBatch) Priority() int {
	return b.priority
}

// TaskBatcher manages task batching
type TaskBatcher struct {
	batches   map[string]*TaskBatch
	lock      sync.RWMutex
	maxSize   int
	maxWait   time.Duration
	processFn func(batch *TaskBatch)
}

// NewTaskBatcher creates a new task batcher
func NewTaskBatcher(maxBatchSize int, maxWaitTime time.Duration, processFn func(batch *TaskBatch)) *TaskBatcher {
	return &TaskBatcher{
		batches:   make(map[string]*TaskBatch),
		maxSize:   maxBatchSize,
		maxWait:   maxWaitTime,
		processFn: processFn,
	}
}

// Add adds a task to an appropriate batch or creates a new batch
func (tb *TaskBatcher) Add(task BatchableTask) {
	batchKey := task.BatchKey()
	
	// Try to find an existing batch
	tb.lock.RLock()
	batch, exists := tb.batches[batchKey]
	tb.lock.RUnlock()
	
	if exists {
		// Try to add to existing batch
		if batch.Add(task) {
			return
		}
	}
	
	// Create a new batch
	tb.lock.Lock()
	defer tb.lock.Unlock()
	
	// Check again in case another goroutine created the batch while we were waiting
	batch, exists = tb.batches[batchKey]
	if exists {
		if batch.Add(task) {
			return
		}
	}
	
	// Create new batch and add it to the map
	newBatch := NewTaskBatch(task, tb.maxSize, tb.maxWait, tb.processFn)
	tb.batches[batchKey] = newBatch
}

// ProcessBatch handles batch processing and cleanup
func (tb *TaskBatcher) ProcessBatch(batch *TaskBatch) {
	// Remove the batch from the map
	tb.lock.Lock()
	delete(tb.batches, batch.batchKey)
	tb.lock.Unlock()
	
	// Process each task in the batch
	// Implementation depends on how batched tasks should be processed
	// This is a placeholder for custom batch processing logic
}