package smt

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerPool manages a pool of worker goroutines to execute tasks
type WorkerPool struct {
	tasks          chan func()
	wg             sync.WaitGroup
	maxWorkers     int32
	currentWorkers atomic.Int32
	activeWorkers  atomic.Int32
	isShutdown     atomic.Bool
	
	// Statistics
	tasksProcessed atomic.Int64
	totalTaskTime  atomic.Int64
}

// NewWorkerPool creates a new worker pool with dynamic sizing
func NewWorkerPool(maxWorkers int) *WorkerPool {
	if maxWorkers <= 0 {
		// Default to number of CPUs if not specified
		maxWorkers = runtime.NumCPU() * 4
	}
	
	pool := &WorkerPool{
		tasks:      make(chan func(), maxWorkers*10), // Buffer tasks to avoid blocking
		maxWorkers: int32(maxWorkers),
	}
	
	// Start minimum number of workers
	minWorkers := min32(4, int32(maxWorkers))
	for i := int32(0); i < minWorkers; i++ {
		pool.startWorker()
	}
	
	// Start goroutine to monitor and adjust worker count
	go pool.adjustWorkerCount()
	
	return pool
}

// startWorker starts a new worker goroutine
func (p *WorkerPool) startWorker() {
	p.wg.Add(1)
	p.currentWorkers.Add(1)
	
	go func() {
		defer p.wg.Done()
		defer p.currentWorkers.Add(-1)
		
		idleTimeout := time.After(30 * time.Second)
		
		for {
			select {
			case task, ok := <-p.tasks:
				if !ok {
					// Channel closed, exit worker
					return
				}
				
				// Reset idle timeout
				idleTimeout = time.After(30 * time.Second)
				
				// Execute task
				p.activeWorkers.Add(1)
				startTime := time.Now()
				
				task()
				
				duration := time.Since(startTime)
				p.activeWorkers.Add(-1)
				p.tasksProcessed.Add(1)
				p.totalTaskTime.Add(duration.Nanoseconds())
				
			case <-idleTimeout:
				// Exit if we have more than minimum workers and this one is idle
				if p.currentWorkers.Load() > min32(4, p.maxWorkers) {
					return
				}
				
				// Reset timeout
				idleTimeout = time.After(30 * time.Second)
			}
		}
	}()
}

// adjustWorkerCount periodically adjusts the number of workers based on load
func (p *WorkerPool) adjustWorkerCount() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		if p.isShutdown.Load() {
			return
		}
		
		// Get current stats
		queueSize := len(p.tasks)
		currentWorkers := p.currentWorkers.Load()
		// activeWorkers variable used in future metrics
		_ = p.activeWorkers.Load()
		
		// If queue is building up, add more workers
		if queueSize > int(currentWorkers) && currentWorkers < p.maxWorkers {
			// Add workers proportional to queue size
			workersToAdd := min32(int32(queueSize)/4+1, p.maxWorkers-currentWorkers)
			for i := int32(0); i < workersToAdd; i++ {
				p.startWorker()
			}
		}
		
		// If we have too many idle workers, they'll time out automatically
		// The timeout mechanism will reduce the worker count gradually
	}
}

// Submit adds a task to the worker pool
func (p *WorkerPool) Submit(task func()) bool {
	if p.isShutdown.Load() {
		return false
	}
	
	// Add task to channel
	select {
	case p.tasks <- task:
		return true
	default:
		// Channel full, start an additional worker if possible
		if p.currentWorkers.Load() < p.maxWorkers {
			p.startWorker()
			
			// Try again
			select {
			case p.tasks <- task:
				return true
			default:
				return false // Still full
			}
		}
		return false
	}
}

// Shutdown gracefully shuts down the worker pool
func (p *WorkerPool) Shutdown() {
	if !p.isShutdown.CompareAndSwap(false, true) {
		return // Already shut down
	}
	
	// Close task channel to signal workers to exit
	close(p.tasks)
	
	// Wait for workers to finish
	p.wg.Wait()
}

// min32 returns the minimum of two int32 values
func min32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

// WorkerPoolStats contains statistics about a worker pool
type WorkerPoolStats struct {
	CurrentWorkers  int32
	MaxWorkers      int32
	ActiveWorkers   int32
	QueueSize       int
	TasksProcessed  int64
	AverageTaskTime time.Duration
}

// Stats returns statistics about the worker pool
func (p *WorkerPool) Stats() WorkerPoolStats {
	tasksProcessed := p.tasksProcessed.Load()
	totalTaskTime := p.totalTaskTime.Load()
	
	var avgTaskTime time.Duration
	if tasksProcessed > 0 {
		avgTaskTime = time.Duration(totalTaskTime / tasksProcessed)
	}
	
	return WorkerPoolStats{
		CurrentWorkers:  p.currentWorkers.Load(),
		MaxWorkers:      p.maxWorkers,
		ActiveWorkers:   p.activeWorkers.Load(),
		QueueSize:       len(p.tasks),
		TasksProcessed:  tasksProcessed,
		AverageTaskTime: avgTaskTime,
	}
}