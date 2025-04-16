package smt

import (
	"container/heap"
	"sync"
	"time"
)

// ServerDispatcher manages tasks for a specific server
type ServerDispatcher struct {
	server            string
	taskQueue         TaskQueuePrio
	taskQueueLock     sync.Mutex
	taskQueueCond     *sync.Cond
	workerCount       int
	maxWorkers        int
	isShutdown        bool
	wg                sync.WaitGroup
	parent            *ProviderDispatcher
}

// ProviderDispatcher manages task dispatching for a provider
type ProviderDispatcher struct {
	providerName      string
	servers           map[string]*ServerDispatcher
	serverLock        sync.RWMutex
	taskManager       *TaskManagerSimple
	isShutdown        bool
}

// NewProviderDispatcher creates a new provider dispatcher
func NewProviderDispatcher(providerName string, tm *TaskManagerSimple) *ProviderDispatcher {
	return &ProviderDispatcher{
		providerName: providerName,
		servers:      make(map[string]*ServerDispatcher),
		taskManager:  tm,
	}
}

// GetOrCreateServerDispatcher gets or creates a server dispatcher
func (pd *ProviderDispatcher) GetOrCreateServerDispatcher(server string, maxWorkers int) *ServerDispatcher {
	pd.serverLock.RLock()
	sd, exists := pd.servers[server]
	pd.serverLock.RUnlock()
	
	if exists {
		return sd
	}
	
	// Create a new server dispatcher
	pd.serverLock.Lock()
	defer pd.serverLock.Unlock()
	
	// Check again in case it was created while waiting for the lock
	if sd, exists = pd.servers[server]; exists {
		return sd
	}
	
	sd = &ServerDispatcher{
		server:     server,
		taskQueue:  TaskQueuePrio{},
		maxWorkers: maxWorkers,
		parent:     pd,
	}
	sd.taskQueueCond = sync.NewCond(&sd.taskQueueLock)
	pd.servers[server] = sd
	
	// Start the server dispatcher
	sd.Start()
	
	return sd
}

// EnqueueTask enqueues a task for the appropriate server
func (pd *ProviderDispatcher) EnqueueTask(task ITask, server string) {
	// Get or create server dispatcher
	sd := pd.GetOrCreateServerDispatcher(server, 10) // Default max workers
	
	// Enqueue the task
	sd.EnqueueTask(task)
}

// EnqueueTaskToAll enqueues a task to all servers
func (pd *ProviderDispatcher) EnqueueTaskToAll(task ITask, selectServer func([]string) string) {
	// Get list of all servers
	pd.serverLock.RLock()
	serverList := make([]string, 0, len(pd.servers))
	for serverName := range pd.servers {
		serverList = append(serverList, serverName)
	}
	pd.serverLock.RUnlock()
	
	// If no servers, return
	if len(serverList) == 0 {
		return
	}
	
	// Select a server and enqueue
	server := selectServer(serverList)
	if server != "" {
		pd.EnqueueTask(task, server)
	}
}

// Shutdown shuts down all server dispatchers
func (pd *ProviderDispatcher) Shutdown() {
	pd.isShutdown = true
	
	pd.serverLock.RLock()
	defer pd.serverLock.RUnlock()
	
	for _, sd := range pd.servers {
		sd.Shutdown()
	}
}

// EnqueueTask adds a task to the server's queue
func (sd *ServerDispatcher) EnqueueTask(task ITask) {
	sd.taskQueueLock.Lock()
	defer sd.taskQueueLock.Unlock()
	
	// Check shutdown status while holding the lock
	if sd.isShutdown {
		return
	}
	
	// Use the parent's task pool if available to avoid memory allocation
	var taskWithPriority *TaskWithPriority
	if tm := sd.parent.taskManager; tm != nil && tm.enablePooling && tm.taskPool != nil {
		taskWithPriority = tm.taskPool.GetWithTask(task, task.GetPriority())
	} else {
		taskWithPriority = &TaskWithPriority{
			task:     task,
			priority: task.GetPriority(),
		}
	}
	heap.Push(&sd.taskQueue, taskWithPriority)
	sd.taskQueueCond.Signal()
}

// Start starts the server dispatcher
func (sd *ServerDispatcher) Start() {
	// Create at least one worker
	sd.startWorker()
}

// startWorker starts a new worker if needed
func (sd *ServerDispatcher) startWorker() {
	sd.taskQueueLock.Lock()
	defer sd.taskQueueLock.Unlock()
	
	// Check if we already have enough workers
	if sd.workerCount >= sd.maxWorkers {
		return
	}
	
	// Start a new worker
	sd.workerCount++
	sd.wg.Add(1)
	
	go func() {
		defer sd.wg.Done()
		defer sd.decrementWorkerCount()
		
		for {
			// Get a task from the queue or wait
			sd.taskQueueLock.Lock()
			
			// Wait for tasks or shutdown
			for sd.taskQueue.Len() == 0 && !sd.isShutdown {
				sd.taskQueueCond.Wait()
			}
			
			// Check for shutdown
			if sd.isShutdown {
				sd.taskQueueLock.Unlock()
				return
			}
			
			// Check if queue is still empty
			if sd.taskQueue.Len() == 0 {
				sd.taskQueueLock.Unlock()
				
				// Sleep briefly to avoid spinning
				time.Sleep(10 * time.Millisecond)
				continue
			}
			
			// Get the next task
			taskWithPriority := heap.Pop(&sd.taskQueue).(*TaskWithPriority)
			
			// Check if we need more workers
			shouldStartNewWorker := sd.taskQueue.Len() > 0 && sd.workerCount < sd.maxWorkers
			
			// Release the lock before processing
			sd.taskQueueLock.Unlock()
			
			// Start a new worker if needed
			if shouldStartNewWorker {
				sd.startWorker()
			}
			
			// Process the task
			task := taskWithPriority.task
			tm := sd.parent.taskManager
			
			// Process the task
			tm.processTaskTwoLevel(task, sd.parent.providerName, sd.server, taskWithPriority)
		}
	}()
}

// decrementWorkerCount decrements the worker count
func (sd *ServerDispatcher) decrementWorkerCount() {
	sd.taskQueueLock.Lock()
	defer sd.taskQueueLock.Unlock()
	
	sd.workerCount--
}

// Shutdown shuts down the server dispatcher
func (sd *ServerDispatcher) Shutdown() {
	sd.taskQueueLock.Lock()
	sd.isShutdown = true
	sd.taskQueueCond.Broadcast()
	sd.taskQueueLock.Unlock()
	
	// Wait for all workers to finish
	sd.wg.Wait()
}

// TwoLevelDispatchManager manages two-level dispatching
type TwoLevelDispatchManager struct {
	providers      map[string]*ProviderDispatcher
	providerLock   sync.RWMutex
	taskManager    *TaskManagerSimple
	enabled        bool
}

// NewTwoLevelDispatchManager creates a new two-level dispatch manager
func NewTwoLevelDispatchManager(tm *TaskManagerSimple, enabled bool) *TwoLevelDispatchManager {
	return &TwoLevelDispatchManager{
		providers:   make(map[string]*ProviderDispatcher),
		taskManager: tm,
		enabled:     enabled,
	}
}

// GetOrCreateProviderDispatcher gets or creates a provider dispatcher
func (tdm *TwoLevelDispatchManager) GetOrCreateProviderDispatcher(providerName string) *ProviderDispatcher {
	if !tdm.enabled {
		return nil
	}
	
	tdm.providerLock.RLock()
	pd, exists := tdm.providers[providerName]
	tdm.providerLock.RUnlock()
	
	if exists {
		return pd
	}
	
	// Create a new provider dispatcher
	tdm.providerLock.Lock()
	defer tdm.providerLock.Unlock()
	
	// Check again in case it was created while waiting for the lock
	if pd, exists = tdm.providers[providerName]; exists {
		return pd
	}
	
	pd = NewProviderDispatcher(providerName, tdm.taskManager)
	tdm.providers[providerName] = pd
	
	return pd
}

// EnqueueTask enqueues a task using two-level dispatching
func (tdm *TwoLevelDispatchManager) EnqueueTask(task ITask, server string) bool {
	if !tdm.enabled {
		return false
	}
	
	providerName := task.GetProvider().Name()
	pd := tdm.GetOrCreateProviderDispatcher(providerName)
	if pd == nil {
		return false
	}
	
	pd.EnqueueTask(task, server)
	return true
}

// EnqueueTaskWithServerSelection enqueues a task using server selection
func (tdm *TwoLevelDispatchManager) EnqueueTaskWithServerSelection(task ITask, selectServer func([]string) string) bool {
	if !tdm.enabled {
		return false
	}
	
	providerName := task.GetProvider().Name()
	pd := tdm.GetOrCreateProviderDispatcher(providerName)
	if pd == nil {
		return false
	}
	
	pd.EnqueueTaskToAll(task, selectServer)
	return true
}

// Shutdown shuts down all provider dispatchers
func (tdm *TwoLevelDispatchManager) Shutdown() {
	tdm.providerLock.RLock()
	defer tdm.providerLock.RUnlock()
	
	for _, pd := range tdm.providers {
		pd.Shutdown()
	}
}

// SetEnabled enables or disables two-level dispatching
func (tdm *TwoLevelDispatchManager) SetEnabled(enabled bool) {
	tdm.enabled = enabled
}

// IsEnabled returns whether two-level dispatching is enabled
func (tdm *TwoLevelDispatchManager) IsEnabled() bool {
	return tdm.enabled
}