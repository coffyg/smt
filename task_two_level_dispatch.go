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
	taskQueueLock     *sync.RWMutex  // Pointer for better performance with conditional variables
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
	
	// Create mutex for the task queue
	taskQueueLock := &sync.RWMutex{}
	
	sd = &ServerDispatcher{
		server:        server,
		taskQueue:     TaskQueuePrio{},
		taskQueueLock: taskQueueLock,
		maxWorkers:    maxWorkers,
		parent:        pd,
	}
	sd.taskQueueCond = sync.NewCond(taskQueueLock)
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

// EnqueueTaskToAll enqueues a task to all servers with optimized server collection
func (pd *ProviderDispatcher) EnqueueTaskToAll(task ITask, selectServer func([]string) string) {
	// Get list of all servers with shutdown check - this prevents race conditions
	pd.serverLock.RLock()
	
	// Check shutdown status while holding the lock
	if pd.isShutdown {
		pd.serverLock.RUnlock()
		return
	}
	
	serverCount := len(pd.servers)
	
	// Fast path for common case of no servers
	if serverCount == 0 {
		pd.serverLock.RUnlock()
		return
	}
	
	// Fast path for single server case
	if serverCount == 1 {
		// With only one server, we can directly get it without allocating a slice
		var serverName string
		for name := range pd.servers {
			serverName = name
			break
		}
		pd.serverLock.RUnlock()
		
		if serverName != "" {
			pd.EnqueueTask(task, serverName)
		}
		return
	}
	
	// Pre-allocate serverList with exact capacity
	serverList := make([]string, 0, serverCount)
	for serverName := range pd.servers {
		serverList = append(serverList, serverName)
	}
	pd.serverLock.RUnlock()
	
	// Select a server and enqueue - already checked if empty above
	server := selectServer(serverList)
	if server != "" {
		pd.EnqueueTask(task, server)
	}
}

// Shutdown shuts down all server dispatchers
func (pd *ProviderDispatcher) Shutdown() {
	pd.serverLock.Lock()
	pd.isShutdown = true
	
	// Create a copy of server dispatchers to shutdown while holding the lock
	servers := make([]*ServerDispatcher, 0, len(pd.servers))
	for _, sd := range pd.servers {
		servers = append(servers, sd)
	}
	pd.serverLock.Unlock()
	
	// Shutdown each server dispatcher without holding the lock
	for _, sd := range servers {
		sd.Shutdown()
	}
}

// EnqueueTask adds a task to the server's queue
func (sd *ServerDispatcher) EnqueueTask(task ITask) {
	sd.taskQueueLock.Lock()
	
	// Check shutdown status while holding the lock - early exit
	if sd.isShutdown {
		sd.taskQueueLock.Unlock()
		return
	}
	
	// Get task pool once to avoid repeated nil checks and property access
	var taskPool *TaskWithPriorityPool
	if tm := sd.parent.taskManager; tm != nil && tm.enablePooling {
		taskPool = tm.taskPool
	}
	
	// Use the parent's task pool if available to avoid memory allocation
	var taskWithPriority *TaskWithPriority
	if taskPool != nil {
		taskWithPriority = taskPool.GetWithTask(task, task.GetPriority())
	} else {
		taskWithPriority = &TaskWithPriority{
			task:     task,
			priority: task.GetPriority(),
		}
	}
	
	heap.Push(&sd.taskQueue, taskWithPriority)
	sd.taskQueueCond.Signal()
	sd.taskQueueLock.Unlock()
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
		
		// Get references to frequently used objects upfront
		// to minimize indirect access and pointer chasing in the loop
		taskQueue := &sd.taskQueue
		taskQueueCond := sd.taskQueueCond
		var tm *TaskManagerSimple
		if sd.parent != nil {
			tm = sd.parent.taskManager
		}
		providerName := sd.parent.providerName
		server := sd.server
		
		for {
			// Get a task from the queue or wait
			sd.taskQueueLock.Lock()
			
			// Wait for tasks or shutdown - use fast path 
			if taskQueue.Len() == 0 && !sd.isShutdown {
				taskQueueCond.Wait()
			}
			
			// Check for shutdown - fast exit path
			if sd.isShutdown {
				sd.taskQueueLock.Unlock()
				return
			}
			
			// Check if queue is still empty - rare condition but needs to be handled
			if taskQueue.Len() == 0 {
				sd.taskQueueLock.Unlock()
				time.Sleep(5 * time.Millisecond) // Reduced sleep time for better responsiveness
				continue
			}
			
			// Determine if we should start a new worker before popping the task
			shouldStartNewWorker := taskQueue.Len() > 1 && sd.workerCount < sd.maxWorkers
			
			// Get the next task
			taskWithPriority := heap.Pop(taskQueue).(*TaskWithPriority)
			
			// Release the lock before processing
			sd.taskQueueLock.Unlock()
			
			// Start a new worker if needed - do this in parallel with processing
			if shouldStartNewWorker {
				go sd.startWorker() // Start in separate goroutine to not block processing
			}
			
			// Process the task - using cached values from above
			task := taskWithPriority.task
			if tm != nil {
				tm.processTaskTwoLevel(task, providerName, server, taskWithPriority)
			}
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
	providers        map[string]*ProviderDispatcher
	providerLock     sync.RWMutex
	taskManager      *TaskManagerSimple
	enabled          bool
	serverSelectFunc func([]string) string
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
	
	// Use provided selector or fall back to default
	serverSelector := selectServer
	if serverSelector == nil {
		serverSelector = tdm.GetServerSelectionFunc()
	}
	
	pd.EnqueueTaskToAll(task, serverSelector)
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

// SetServerSelectionFunc sets the server selection function
func (tdm *TwoLevelDispatchManager) SetServerSelectionFunc(selectFunc func([]string) string) {
	tdm.serverSelectFunc = selectFunc
}

// GetServerSelectionFunc returns the current server selection function
func (tdm *TwoLevelDispatchManager) GetServerSelectionFunc() func([]string) string {
	if tdm.serverSelectFunc == nil {
		// Default server selection function - prioritize first server
		return func(servers []string) string {
			if len(servers) == 0 {
				return ""
			}
			return servers[0]
		}
	}
	return tdm.serverSelectFunc
}