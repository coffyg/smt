package smt

import (
	"sync"
	"time"
)

// ServerBatchableTask extends BatchableTask interface for tasks that can be batched by server
type ServerBatchableTask interface {
	BatchableTask
	// GetTargetServer returns the target server for this task
	GetTargetServer() string
}

// ServerTaskBatcher manages task batching by server
type ServerTaskBatcher struct {
	// Map of server -> map of batchKey -> batch
	serverBatches map[string]map[string]*TaskBatch
	lock          sync.RWMutex
	maxSize       int
	maxWait       time.Duration
	processFn     func(batch *TaskBatch)
}

// NewServerTaskBatcher creates a new task batcher that organizes batches by server
func NewServerTaskBatcher(maxBatchSize int, maxWaitTime time.Duration, processFn func(batch *TaskBatch)) *ServerTaskBatcher {
	return &ServerTaskBatcher{
		serverBatches: make(map[string]map[string]*TaskBatch),
		maxSize:       maxBatchSize,
		maxWait:       maxWaitTime,
		processFn:     processFn,
	}
}

// Add adds a task to an appropriate batch based on its server and batch key
func (sb *ServerTaskBatcher) Add(task ServerBatchableTask) {
	server := task.GetTargetServer()
	batchKey := task.BatchKey()
	
	// Try to find an existing batch for this server and batch key
	sb.lock.RLock()
	serverMap, serverExists := sb.serverBatches[server]
	if serverExists {
		batch, batchExists := serverMap[batchKey]
		if batchExists {
			// Try to add to existing batch
			sb.lock.RUnlock()
			if batch.Add(task) {
				return
			}
			// If add failed, we'll need to create a new batch
		} else {
			sb.lock.RUnlock()
		}
	} else {
		sb.lock.RUnlock()
	}
	
	// Create a new batch
	sb.lock.Lock()
	defer sb.lock.Unlock()
	
	// Check again to avoid race conditions
	serverMap, serverExists = sb.serverBatches[server]
	if !serverExists {
		// First batch for this server
		serverMap = make(map[string]*TaskBatch)
		sb.serverBatches[server] = serverMap
	} else {
		// Check if batch was created while we were waiting for the lock
		batch, batchExists := serverMap[batchKey]
		if batchExists {
			if batch.Add(task) {
				return
			}
		}
	}
	
	// Create new batch with custom process function that handles cleanup
	newBatch := NewTaskBatch(task, sb.maxSize, sb.maxWait, func(batch *TaskBatch) {
		// First process the batch
		if sb.processFn != nil {
			sb.processFn(batch)
		}
		
		// Then remove it from our maps
		sb.removeBatch(server, batchKey)
	})
	
	serverMap[batchKey] = newBatch
}

// removeBatch removes a batch from the server batches map
func (sb *ServerTaskBatcher) removeBatch(server, batchKey string) {
	sb.lock.Lock()
	defer sb.lock.Unlock()
	
	serverMap, exists := sb.serverBatches[server]
	if !exists {
		return
	}
	
	delete(serverMap, batchKey)
	
	// If the server map is now empty, remove it too
	if len(serverMap) == 0 {
		delete(sb.serverBatches, server)
	}
}

// GetBatchesForServer returns all batches for a given server
func (sb *ServerTaskBatcher) GetBatchesForServer(server string) []*TaskBatch {
	sb.lock.RLock()
	defer sb.lock.RUnlock()
	
	serverMap, exists := sb.serverBatches[server]
	if !exists {
		return nil
	}
	
	batches := make([]*TaskBatch, 0, len(serverMap))
	for _, batch := range serverMap {
		batches = append(batches, batch)
	}
	
	return batches
}

// GetAllBatches returns all batches from all servers
func (sb *ServerTaskBatcher) GetAllBatches() []*TaskBatch {
	sb.lock.RLock()
	defer sb.lock.RUnlock()
	
	var batches []*TaskBatch
	
	for _, serverMap := range sb.serverBatches {
		for _, batch := range serverMap {
			batches = append(batches, batch)
		}
	}
	
	return batches
}

// CountBatches returns the total number of active batches
func (sb *ServerTaskBatcher) CountBatches() int {
	sb.lock.RLock()
	defer sb.lock.RUnlock()
	
	count := 0
	for _, serverMap := range sb.serverBatches {
		count += len(serverMap)
	}
	
	return count
}

// CountServers returns the number of servers with active batches
func (sb *ServerTaskBatcher) CountServers() int {
	sb.lock.RLock()
	defer sb.lock.RUnlock()
	
	return len(sb.serverBatches)
}