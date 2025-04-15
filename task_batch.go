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
	lock        sync.Mutex
	batchKey    string
	createdAt   time.Time
	maxSize     int
	maxWaitTime time.Duration
	timer       *time.Timer
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
	}
	
	batch.tasks = append(batch.tasks, initialTask)
	
	// Start timer for max wait time
	batch.timer = time.AfterFunc(maxWaitTime, func() {
		batch.Process()
	})
	
	return batch
}

// Add attempts to add a task to the batch
func (b *TaskBatch) Add(task BatchableTask) bool {
	if task.BatchKey() != b.batchKey {
		return false
	}
	
	b.lock.Lock()
	defer b.lock.Unlock()
	
	// Check if batch is full
	if len(b.tasks) >= b.maxSize {
		return false
	}
	
	// Check if any task can't batch with this new task
	for _, existingTask := range b.tasks {
		if !existingTask.CanBatchWith(task) {
			return false
		}
	}
	
	// Update highest priority
	if task.GetPriority() > b.priority {
		b.priority = task.GetPriority()
	}
	
	// Add task to batch
	b.tasks = append(b.tasks, task)
	
	// Process immediately if batch is full
	if len(b.tasks) >= b.maxSize {
		// Stop the timer and process
		if b.timer != nil {
			b.timer.Stop()
		}
		go b.Process()
	}
	
	return true
}

// Process processes the batch
func (b *TaskBatch) Process() {
	b.lock.Lock()
	defer b.lock.Unlock()
	
	// Stop the timer to avoid calling Process twice
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	
	// Don't process empty batches
	if len(b.tasks) == 0 {
		return
	}
	
	// Call the process function
	if b.processFn != nil {
		b.processFn(b)
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