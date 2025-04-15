package smt

import (
	"sync"
)

// TaskWithPriorityPool provides a thread-safe pool for TaskWithPriority objects
// to reduce GC pressure by reusing objects.
type TaskWithPriorityPool struct {
	pool sync.Pool
}

// NewTaskWithPriorityPool creates a new pool for TaskWithPriority objects.
func NewTaskWithPriorityPool() *TaskWithPriorityPool {
	return &TaskWithPriorityPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &TaskWithPriority{
					// Initialize with default values
					priority: 0,
					index:    -1,
					task:     nil,
				}
			},
		},
	}
}

// Get retrieves a TaskWithPriority object from the pool or creates a new one if needed.
func (p *TaskWithPriorityPool) Get() *TaskWithPriority {
	return p.pool.Get().(*TaskWithPriority)
}

// Put returns a TaskWithPriority object to the pool for future reuse.
// The caller must ensure the object is no longer referenced after calling Put.
func (p *TaskWithPriorityPool) Put(tp *TaskWithPriority) {
	// Reset the object before returning it to the pool
	tp.task = nil
	tp.priority = 0
	tp.index = -1
	p.pool.Put(tp)
}

// GetWithTask creates and initializes a TaskWithPriority from the pool with the given task and priority.
func (p *TaskWithPriorityPool) GetWithTask(task ITask, priority int) *TaskWithPriority {
	tp := p.Get()
	tp.task = task
	tp.priority = priority
	return tp
}