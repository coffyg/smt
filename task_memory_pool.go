package smt

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// TaskWithPriorityPool provides a thread-safe pool for TaskWithPriority objects
// to reduce GC pressure by reusing objects.
type TaskWithPriorityPool struct {
	pool          sync.Pool
	stats         poolStats
	preWarmSize   int
	preWarmThread sync.Once
}

// poolStats tracks statistics about pool usage for monitoring and tuning
type poolStats struct {
	gets     uint64
	puts     uint64
	misses   uint64
	maxInUse uint64
	inUse    uint64
}

// PoolConfig provides configuration options for the TaskWithPriorityPool
type PoolConfig struct {
	PreWarmSize  int  // Number of objects to pre-allocate (default: 1000)
	TrackStats   bool // Whether to track usage statistics (adds small overhead)
	PreWarmAsync bool // Whether to pre-warm in background thread
}

// NewTaskWithPriorityPool creates a new pool for TaskWithPriority objects.
func NewTaskWithPriorityPool() *TaskWithPriorityPool {
	return NewTaskWithPriorityPoolConfig(nil)
}

// NewTaskWithPriorityPoolConfig creates a new pool with the specified configuration.
func NewTaskWithPriorityPoolConfig(config *PoolConfig) *TaskWithPriorityPool {
	// Use default config if none provided
	if config == nil {
		config = &PoolConfig{
			PreWarmSize:  1000,
			TrackStats:   false,
			PreWarmAsync: true,
		}
	}

	// Ensure sensible defaults
	if config.PreWarmSize <= 0 {
		config.PreWarmSize = 1000
	}

	pool := &TaskWithPriorityPool{
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
		preWarmSize: config.PreWarmSize,
	}

	// Pre-warm the pool to reduce initial allocation pressure
	if config.PreWarmAsync {
		go pool.preWarm()
	} else {
		pool.preWarm()
	}

	return pool
}

// preWarm fills the pool with a set number of objects to avoid
// initial allocation pressure.
func (p *TaskWithPriorityPool) preWarm() {
	p.preWarmThread.Do(func() {
		// Create a batch of objects
		objects := make([]*TaskWithPriority, p.preWarmSize)
		for i := 0; i < p.preWarmSize; i++ {
			objects[i] = &TaskWithPriority{
				priority: 0,
				index:    -1,
				task:     nil,
			}
		}

		// Return them to the pool
		for i := 0; i < p.preWarmSize; i++ {
			p.pool.Put(objects[i])
		}

		// Clear the reference to the slice to allow GC
		objects = nil
		runtime.GC() // Force a GC to clean up temporary slice
	})
}

// Get retrieves a TaskWithPriority object from the pool or creates a new one if needed.
func (p *TaskWithPriorityPool) Get() *TaskWithPriority {
	// Track statistics if enabled
	atomic.AddUint64(&p.stats.gets, 1)
	inUse := atomic.AddUint64(&p.stats.inUse, 1)
	
	// Update max in use count if needed
	for {
		current := atomic.LoadUint64(&p.stats.maxInUse)
		if inUse <= current {
			break
		}
		if atomic.CompareAndSwapUint64(&p.stats.maxInUse, current, inUse) {
			break
		}
	}
	
	// Get from pool
	obj := p.pool.Get()
	if obj == nil {
		atomic.AddUint64(&p.stats.misses, 1)
		return &TaskWithPriority{
			priority: 0,
			index:    -1,
			task:     nil,
		}
	}
	
	return obj.(*TaskWithPriority)
}

// Put returns a TaskWithPriority object to the pool for future reuse.
// The caller must ensure the object is no longer referenced after calling Put.
func (p *TaskWithPriorityPool) Put(tp *TaskWithPriority) {
	if tp == nil {
		return
	}
	
	// Track statistics if enabled
	atomic.AddUint64(&p.stats.puts, 1)
	atomic.AddUint64(&p.stats.inUse, ^uint64(0)) // Decrement
	
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

// GetPoolStats returns a copy of the current pool statistics.
// This is useful for monitoring and tuning.
func (p *TaskWithPriorityPool) GetPoolStats() (gets, puts, misses, currentInUse, maxInUse uint64) {
	gets = atomic.LoadUint64(&p.stats.gets)
	puts = atomic.LoadUint64(&p.stats.puts)
	misses = atomic.LoadUint64(&p.stats.misses)
	currentInUse = atomic.LoadUint64(&p.stats.inUse)
	maxInUse = atomic.LoadUint64(&p.stats.maxInUse)
	return
}