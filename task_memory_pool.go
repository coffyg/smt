package smt

import (
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
			PreWarmSize:  2000,
			TrackStats:   false,
			PreWarmAsync: true,
		}
	}

	// Ensure sensible defaults
	if config.PreWarmSize <= 0 {
		config.PreWarmSize = 2000
	}

	pool := &TaskWithPriorityPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &TaskWithPriority{
					// Initialize with default values
					priority: 0,
					index:    -1,
					task:     nil,
					sortKey:  0,
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
		// Put objects directly into the pool to reduce memory overhead
		// and avoid allocating a large slice
		const batchSize = 200 // Process in smaller batches to reduce memory pressure
		remaining := p.preWarmSize
		
		for remaining > 0 {
			count := batchSize
			if remaining < batchSize {
				count = remaining
			}
			
			for i := 0; i < count; i++ {
				tp := &TaskWithPriority{
					priority: 0,
					index:    -1,
					task:     nil,
					sortKey:  0,
				}
				p.pool.Put(tp)
			}
			
			remaining -= count
		}
	})
}

// Get retrieves a TaskWithPriority object from the pool or creates a new one if needed.
func (p *TaskWithPriorityPool) Get() *TaskWithPriority {
	// Track statistics if enabled
	atomic.AddUint64(&p.stats.gets, 1)
	inUse := atomic.AddUint64(&p.stats.inUse, 1)
	
	// Update max in use count if needed - use simplified approach
	maxInUse := atomic.LoadUint64(&p.stats.maxInUse)
	if inUse > maxInUse {
		atomic.CompareAndSwapUint64(&p.stats.maxInUse, maxInUse, inUse)
	}
	
	// Get from pool - obj should never be nil for sync.Pool
	obj := p.pool.Get()
	tp, ok := obj.(*TaskWithPriority)
	
	// Fallback in case of unexpected behavior
	if !ok || tp == nil {
		atomic.AddUint64(&p.stats.misses, 1)
		return &TaskWithPriority{
			priority: 0,
			index:    -1,
			task:     nil,
			sortKey:  0,
		}
	}
	
	return tp
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
	tp.sortKey = 0
	
	p.pool.Put(tp)
}

// GetWithTask creates and initializes a TaskWithPriority from the pool with the given task and priority.
// This is a more efficient way to get a pooled object with task and priority already set.
func (p *TaskWithPriorityPool) GetWithTask(task ITask, priority int) *TaskWithPriority {
	tp := p.Get()
	tp.task = task
	tp.priority = priority
	// Generate a sort key (default index of 0 for now, will be updated on insertion)
	tp.sortKey = ComputeSortKey(priority, 0)
	return tp
}

// BatchGet retrieves multiple TaskWithPriority objects at once for bulk operations.
// This can be more efficient than multiple individual Get calls.
func (p *TaskWithPriorityPool) BatchGet(count int) []*TaskWithPriority {
	if count <= 0 {
		return nil
	}
	
	result := make([]*TaskWithPriority, count)
	for i := 0; i < count; i++ {
		result[i] = p.Get()
	}
	return result
}

// BatchPut returns multiple TaskWithPriority objects to the pool at once.
// This can be more efficient than multiple individual Put calls.
func (p *TaskWithPriorityPool) BatchPut(items []*TaskWithPriority) {
	if items == nil {
		return
	}
	
	for i := range items {
		if items[i] != nil {
			p.Put(items[i])
			// Clear reference to help GC
			items[i] = nil
		}
	}
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