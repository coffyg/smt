package smt

import (
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// TaskManagerSimple interface (optimized)
type TaskManagerSimple struct {
	providers            map[string]*ProviderData
	taskInQueue          sync.Map
	isRunning            int32
	shutdownRequest      int32
	shutdownCh           chan struct{}
	wg                   sync.WaitGroup
	logger               *zerolog.Logger
	getTimeout           func(string, string) time.Duration
	serverConcurrencyMap map[string]chan struct{} // Map of servers to semaphores
	serverConcurrencyMu  sync.RWMutex
}

type ProviderData struct {
	taskQueue           TaskQueuePrio
	taskQueueLock       sync.Mutex
	taskQueueCond       *sync.Cond
	servers             []string
	availableServers    chan string             // Fast server allocation channel
	availableServerList []string                // Cache of available servers for fast checking
	availableServerLock sync.RWMutex            // Lock for availableServerList
	commandQueue        *CommandQueue
	commandSet          map[uuid.UUID]struct{}
	commandSetLock      sync.Mutex
}

func NewTaskManagerSimple(providers *[]IProvider, servers map[string][]string, logger *zerolog.Logger, getTimeout func(string, string) time.Duration) *TaskManagerSimple {
	tm := &TaskManagerSimple{
		providers:            make(map[string]*ProviderData),
		isRunning:            0,
		shutdownRequest:      0,
		shutdownCh:           make(chan struct{}),
		logger:               logger,
		getTimeout:           getTimeout,
		serverConcurrencyMap: make(map[string]chan struct{}),
		serverConcurrencyMu:  sync.RWMutex{},
	}

	// Initialize providers
	for _, provider := range *providers {
		providerName := provider.Name()
		serverList, ok := servers[providerName]
		if !ok {
			serverList = []string{}
		}
		pd := &ProviderData{
			taskQueue:        TaskQueuePrio{},
			taskQueueLock:    sync.Mutex{},
			commandQueue:     NewCommandQueue(32), // Initialize with a reasonable capacity
			commandSet:       make(map[uuid.UUID]struct{}),
			commandSetLock:   sync.Mutex{},
			taskQueueCond:    sync.NewCond(&sync.Mutex{}),
			servers:          serverList,
			availableServers: make(chan string, len(serverList)),
		}
		for _, server := range serverList {
			pd.availableServers <- server
		}
		pd.taskQueueCond = sync.NewCond(&pd.taskQueueLock)
		tm.providers[providerName] = pd
	}

	return tm
}
// IsServerAvailable checks if a server is available without blocking on channel operations
// This is a faster alternative to checking channel status when you only need to know availability
func (pd *ProviderData) IsServerAvailable(serverName string) bool {
	pd.availableServerLock.RLock()
	defer pd.availableServerLock.RUnlock()
	
	// Check if server is in the list of available servers
	for _, s := range pd.availableServerList {
		if s == serverName {
			return true
		}
	}
	return false
}

func (tm *TaskManagerSimple) SetTaskManagerServerMaxParallel(prefix string, maxParallel int) {
	tm.serverConcurrencyMu.Lock()
	defer tm.serverConcurrencyMu.Unlock()
	if maxParallel <= 0 {
		delete(tm.serverConcurrencyMap, prefix)
	} else {
		tm.serverConcurrencyMap[prefix] = make(chan struct{}, maxParallel)
	}
}

func (tm *TaskManagerSimple) HasShutdownRequest() bool {
	return atomic.LoadInt32(&tm.shutdownRequest) == 1
}

func (tm *TaskManagerSimple) IsRunning() bool {
	return atomic.LoadInt32(&tm.isRunning) == 1
}

func (tm *TaskManagerSimple) setTaskInQueue(task ITask) {
	tm.taskInQueue.Store(task.GetID(), struct{}{})
}

func (tm *TaskManagerSimple) isTaskInQueue(task ITask) bool {
	_, ok := tm.taskInQueue.Load(task.GetID())
	return ok
}

func (tm *TaskManagerSimple) delTaskInQueue(task ITask) {
	tm.taskInQueue.Delete(task.GetID())
}

func (tm *TaskManagerSimple) AddTasks(tasks []ITask) (count int, err error) {
	for _, task := range tasks {
		if tm.AddTask(task) {
			count++
		}
	}
	return count, err
}

func (tm *TaskManagerSimple) AddTask(task ITask) bool {
	// Use atomic variable for extremely fast check without map access
	if atomic.LoadInt32(&tm.isRunning) != 1 {
		return false
	}

	// Quick check for task already being in queue
	if tm.isTaskInQueue(task) {
		return false
	}

	// Pre-fetch task information to reduce duplicated getter calls
	taskID := task.GetID()
	provider := task.GetProvider()
	
	// Check provider early to avoid unnecessary work
	if provider == nil {
		err := fmt.Errorf("task '%s' has no provider", taskID)
		// Only create the error message once
		tm.logger.Error().Err(err).Msg("[tms|nil_provider|error] error")
		task.MarkAsFailed(0, err)
		task.OnComplete()
		// Do not set the task as in queue
		return false
	}

	// Get provider information with only one call to Name() 
	// and cache map lookup result to avoid repeated accesses
	providerName := provider.Name()
	
	// Cache the providers map in a local variable to avoid map lookup overhead
	providers := tm.providers
	pd, ok := providers[providerName]
	if !ok {
		// Use pre-formatted error message to avoid string concatenation
		err := fmt.Errorf(errProviderNotFound, providerName)
		tm.logger.Error().Err(err).
			Str("provider", providerName).
			Str("taskID", taskID).
			Msg("[tms] provider not found error")
		task.MarkAsFailed(0, err)
		task.OnComplete()
		return false
	}

	// Now that the task is confirmed valid and the provider exists, set it as in queue
	tm.setTaskInQueue(task)

	// Cache task priority to avoid method call inside critical section
	priority := task.GetPriority()
	
	// Get a TaskWithPriority object from the pool
	taskWithPriority := taskWithPriorityPool.Get().(*TaskWithPriority)
	taskWithPriority.task = task
	taskWithPriority.priority = priority
	taskWithPriority.index = 0 // Will be set in Push
	
	// Minimize critical section time
	pd.taskQueueLock.Lock()
	heap.Push(&pd.taskQueue, taskWithPriority)
	pd.taskQueueCond.Signal() // Signal that a new task is available
	pd.taskQueueLock.Unlock()

	return true
}

func (tm *TaskManagerSimple) Start() {
	if tm.IsRunning() {
		return
	}

	atomic.StoreInt32(&tm.isRunning, 1)

	for providerName := range tm.providers {
		tm.wg.Add(1)
		go tm.providerDispatcher(providerName)
	}
}

// HasAvailableServer checks if the provider has any available servers without blocking
func (pd *ProviderData) HasAvailableServer() bool {
	pd.availableServerLock.RLock()
	defer pd.availableServerLock.RUnlock()
	return len(pd.availableServerList) > 0
}

func (tm *TaskManagerSimple) providerDispatcher(providerName string) {
	defer tm.wg.Done()
	// Pre-fetch provider data to avoid map lookups in the loop
	pd := tm.providers[providerName]
	shutdownCh := tm.shutdownCh

	// Create a batch channel to help with server acquisition
	// This can reduce contention when multiple tasks are ready
	const batchSize = 4 // Small batch size to reduce latency
	serverBatch := make([]string, 0, batchSize)

	// Reusable variables to avoid repeated allocations in the loop
	var isCommand bool
	var command Command
	var task ITask
	var hasWork bool
	var taskWithPriority *TaskWithPriority
	var server string
	var serverCount int

	for {
		// First check if we need to process any pending tasks with already acquired servers
		if len(serverBatch) > 0 {
			// Process one task with an available server from the batch
			server = serverBatch[0]
			serverBatch = serverBatch[1:]
			
			// Reset variables for reuse
			isCommand = false
			command = Command{}
			task = nil
			hasWork = false
			
			// Get the next task or command under lock
			pd.taskQueueLock.Lock()
			if pd.taskQueue.Len() > 0 {
				// Get the next task
				taskWithPriority = heap.Pop(&pd.taskQueue).(*TaskWithPriority)
				task = taskWithPriority.task
				isCommand = false
				hasWork = true
			} else if pd.commandQueue.Len() > 0 {
				// Dequeue the command
				command, _ = pd.commandQueue.Dequeue()
				isCommand = true
				hasWork = true
			} else {
				// No work to do, return the server
				pd.availableServers <- server
				pd.taskQueueLock.Unlock()
				continue
			}
			pd.taskQueueLock.Unlock()
			
			if hasWork {
				tm.wg.Add(1)
				if isCommand {
					go tm.processCommand(command, providerName, server)
				} else {
					// Process the task (no need to return to pool since we got it from the queue directly)
					go tm.processTask(task, providerName, server)
				}
			}
			continue
		}

		// No servers in batch, need to get work and wait for servers
		// Reset variables for reuse
		isCommand = false
		command = Command{}
		taskWithPriority = nil
		hasWork = false

		pd.taskQueueLock.Lock()
		for pd.taskQueue.Len() == 0 && pd.commandQueue.Len() == 0 && !tm.HasShutdownRequest() {
			pd.taskQueueCond.Wait()
		}
		if tm.HasShutdownRequest() {
			pd.taskQueueLock.Unlock()
			return
		}

		if pd.taskQueue.Len() > 0 {
			// Get the next task
			taskWithPriority = heap.Pop(&pd.taskQueue).(*TaskWithPriority)
			isCommand = false
			hasWork = true
		} else if pd.commandQueue.Len() > 0 {
			// Dequeue the command
			command, _ = pd.commandQueue.Dequeue()
			isCommand = true
			hasWork = true
		} else {
			// No work to do, go back to waiting
			pd.taskQueueLock.Unlock()
			continue
		}
		pd.taskQueueLock.Unlock()

		if !hasWork {
			continue
		}

		// Wait for an available server
		select {
		case <-shutdownCh:
			// Return task to pool if we're shutting down
			if !isCommand && taskWithPriority != nil {
				taskWithPriority.task = nil // Clear references
				taskWithPriorityPool.Put(taskWithPriority)
			}
			return
		case server = <-pd.availableServers:
			// We got a server, process the task or command
			tm.wg.Add(1)
			if isCommand {
				go tm.processCommand(command, providerName, server)
			} else {
				// Extract task from TaskWithPriority before processing
				task = taskWithPriority.task
				
				// Return TaskWithPriority to the pool
				taskWithPriority.task = nil // Clear references
				taskWithPriorityPool.Put(taskWithPriority)
				
				// Process the task
				go tm.processTask(task, providerName, server)
			}
			
			// Try to get more servers for batch processing
			// This is non-blocking to avoid delays
			serverCount = 0
			// Clear the batch slice but keep capacity
			serverBatch = serverBatch[:0]
			for serverCount < batchSize-1 {
				select {
				case server = <-pd.availableServers:
					serverBatch = append(serverBatch, server)
					serverCount++
				default:
					// No more immediately available servers
					goto DoneBatching
				}
			}
		DoneBatching:
		}
	}
}

// cachedTimeNow provides a cached time.Time that's updated periodically
// to reduce the overhead of frequent time.Now() calls in hot paths
var (
	cachedTime     atomic.Value
	cachedTimeOnce sync.Once
)

// Context key types to avoid conflicts
type contextKey string
const (
	taskIDKey      contextKey = "taskID"
	providerNameKey contextKey = "providerName"
	serverNameKey   contextKey = "serverName"
)

// getCachedTime returns the current time, using a cached value if available
func getCachedTime() time.Time {
	// Start the background updater if needed
	cachedTimeOnce.Do(func() {
		t := time.Now()
		cachedTime.Store(t)
		
		// Update the cached time every 1ms in the background
		go func() {
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			
			for t := range ticker.C {
				cachedTime.Store(t)
			}
		}()
	})
	
	// Return the cached time
	return cachedTime.Load().(time.Time)
}

func (tm *TaskManagerSimple) processTask(task ITask, providerName, server string) {
	started := getCachedTime() // Use cached time to reduce syscalls
	var onCompleteCalled bool

	// Pre-fetch task information to reduce repeated getter calls
	taskID := task.GetID()
	provider := task.GetProvider()

	// Pre-fetch provider information
	pd, providerExists := tm.providers[providerName]

	// Acquire semaphore if server URL matches any prefix with concurrency limit
	tm.serverConcurrencyMu.RLock()
	var semaphore chan struct{}
	var hasLimit bool
	for prefix, sem := range tm.serverConcurrencyMap {
		if strings.HasPrefix(server, prefix) {
			semaphore = sem
			hasLimit = true
			break
		}
	}
	tm.serverConcurrencyMu.RUnlock()
	if hasLimit {
		semaphore <- struct{}{}
	}

	defer tm.wg.Done()
	defer func() {
		if hasLimit {
			<-semaphore
		}
		// Return the server to the available servers pool
		// Use non-blocking send when possible to reduce contention
		if providerExists {
			select {
			case pd.availableServers <- server:
				// Server returned successfully
			default:
				// Channel is full, use goroutine to avoid blocking
				go func(s string) {
					pd.availableServers <- s
				}(server)
			}
		}
	}()
	// Handle panics
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic occurred: %v\n%s", r, string(debug.Stack()))
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic", providerName, taskID, server)
			// Use cached value of started to avoid another time.Now() call
			now := getCachedTime()
				elapsed := now.Sub(started)
			task.MarkAsFailed(elapsed.Milliseconds(), err)
			if !onCompleteCalled {
				task.OnComplete()
				onCompleteCalled = true
			}
			tm.delTaskInQueue(task)
		}
	}()

	// Ensure the task has a valid provider
	if provider == nil {
		err := fmt.Errorf("task '%s' has no provider", taskID)
		tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] Task has no provider", providerName, taskID, server)
		// Use cached value of started to avoid another time.Now() call
		now := getCachedTime()
		elapsed := now.Sub(started)
		task.MarkAsFailed(elapsed.Milliseconds(), err)
		if !onCompleteCalled {
			task.OnComplete()
			onCompleteCalled = true
		}
		tm.delTaskInQueue(task)
		return
	}

	err, totalTime := tm.HandleWithTimeout(providerName, task, server, tm.HandleTask)
	if err != nil {
		retries := task.GetRetries()
		maxRetries := task.GetMaxRetries()
		if retries >= maxRetries || err == sql.ErrNoRows {
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] max retries reached", providerName, taskID, server)
			task.MarkAsFailed(totalTime, err)
			if !onCompleteCalled {
				task.OnComplete()
				onCompleteCalled = true
			}
			tm.delTaskInQueue(task)
		} else {
			// Cache level check to avoid function call overhead when debugging is disabled
			if level := tm.logger.GetLevel(); level <= zerolog.DebugLevel {
				tm.logger.Debug().Err(err).
					Str("provider", providerName).
					Str("taskID", taskID).
					Str("server", server).
					Int("retry", retries+1).
					Int("maxRetries", maxRetries).
					Msg("[tms] retrying task")
			}
			task.UpdateRetries(retries + 1)
			tm.delTaskInQueue(task)
			tm.AddTask(task)
		}
	} else {
		task.MarkAsSuccess(totalTime)
		if !onCompleteCalled {
			task.OnComplete()
			onCompleteCalled = true
		}
		tm.delTaskInQueue(task)
	}
}

func (tm *TaskManagerSimple) HandleTask(task ITask, server string) error {
	provider := task.GetProvider()
	if provider == nil {
		return fmt.Errorf("task '%s' has no provider", task.GetID())
	}

	return provider.Handle(task, server)
}

func (tm *TaskManagerSimple) Shutdown() {
	if !tm.IsRunning() {
		// Only log if debug level is enabled
		if tm.logger.GetLevel() <= zerolog.DebugLevel {
			tm.logger.Debug().Msg("[tms] Task manager shutdown [ALREADY STOPPED]")
		}
		return
	}
	atomic.StoreInt32(&tm.shutdownRequest, 1)
	close(tm.shutdownCh)

	// Signal all provider dispatchers to wake up
	// Pre-fetch providers map to avoid repeated map access within the loop
	providers := tm.providers
	for _, pd := range providers {
		pd.taskQueueLock.Lock()
		pd.taskQueueCond.Broadcast()
		pd.taskQueueLock.Unlock()
	}

	tm.wg.Wait()
	atomic.StoreInt32(&tm.isRunning, 0)
	
	// Only log if debug level is enabled
	if tm.logger.GetLevel() <= zerolog.DebugLevel {
		tm.logger.Debug().Msg("[tms] Task manager shutdown [FINISHED]")
	}
}

func (tm *TaskManagerSimple) HandleWithTimeout(pn string, task ITask, server string, handler func(ITask, string) error) (error, int64) {
	var err error
	taskID := task.GetID() // Cache task ID to reduce getter calls
	callbackName := task.GetCallbackName()
	
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic occurred: %v", r)
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic in task", pn, taskID, server)
		}
	}()

	maxTimeout := tm.getTimeout(callbackName, pn)
	
	// Store common task data in context to avoid repeated retrievals
	ctx := context.WithValue(context.Background(), taskIDKey, taskID)
	ctx = context.WithValue(ctx, providerNameKey, pn)
	ctx = context.WithValue(ctx, serverNameKey, server)
	ctx, cancel := context.WithTimeout(ctx, maxTimeout)
	defer cancel()

	done := make(chan error, 1)
	startTime := getCachedTime() // Use cached time to reduce syscall overhead

	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("panic occurred in handler: %v\n%s", r, string(debug.Stack()))
				tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic in handler", pn, taskID, server)
				done <- err
			}
		}()

		// Only log at debug level if it's enabled (avoid string formatting overhead)
		if tm.logger.GetLevel() <= zerolog.DebugLevel {
			tm.logger.Debug().Msgf("[tms|%s|%s] Task STARTED on server %s", pn, taskID, server)
		}
		done <- handler(task, server)
	}()

	var elapsed time.Duration
	select {
	case <-ctx.Done():
		now := getCachedTime()
		elapsed = now.Sub(startTime) // Use cached time to reduce syscall overhead
		err = fmt.Errorf(errTaskTimeout, pn, taskID, server)
		tm.logger.Error().Err(err).Msgf("[%s|%s] Task FAILED-TIMEOUT on server %s, took %s", pn, taskID, server, elapsed)
	case err = <-done:
		now := getCachedTime()
		elapsed = now.Sub(startTime) // Use cached time to reduce syscall overhead
		if err == nil {
			// Cache level check and use structured logging to avoid string formatting
			if level := tm.logger.GetLevel(); level <= zerolog.DebugLevel {
				tm.logger.Debug().
					Str("provider", pn).
					Str("taskID", taskID).
					Str("server", server).
					Dur("duration", elapsed).
					Msg("[tms] Task COMPLETED")
			}
		} else {
			// Use structured logging for better performance
			tm.logger.Error().
				Err(err).
				Str("provider", pn).
				Str("taskID", taskID).
				Str("server", server).
				Dur("duration", elapsed).
				Msg("[tms] Task FAILED")
		}
	}

	// Convert elapsed time to milliseconds only once
	return err, elapsed.Milliseconds()
}

var (
	TaskQueueManagerInstance *TaskManagerSimple
	taskManagerMutex         sync.Mutex
	taskManagerCond          = sync.NewCond(&taskManagerMutex)
	addMaxRetries            = 3
	
	// Object pool for TaskWithPriority instances to reduce memory allocations
	taskWithPriorityPool = sync.Pool{
		New: func() interface{} {
			return &TaskWithPriority{}
		},
	}
	
	// Pre-formatted error messages to avoid runtime string formatting in hot paths
	errTaskNoProvider     = "task '%s' has no provider"
	errProviderNotFound   = "provider '%s' not found"
	errTaskTimeout        = "[tms|%s|%s] Task timed out on server %s"
	errPanicOccurred      = "panic occurred: %v\n%s"
	errPanicInHandler     = "panic occurred in handler: %v\n%s"
)

// Initialize the TaskManager
func InitTaskQueueManager(logger *zerolog.Logger, providers *[]IProvider, tasks []ITask, servers map[string][]string, getTimeout func(string, string) time.Duration) {
	taskManagerMutex.Lock()
	defer taskManagerMutex.Unlock()
	logger.Info().Msg("[tms] Task manager initialization")

	TaskQueueManagerInstance = NewTaskManagerSimple(providers, servers, logger, getTimeout)
	TaskQueueManagerInstance.Start()
	logger.Info().Msg("[tms] Task manager started")

	// Signal that the TaskManager is ready
	taskManagerCond.Broadcast()

	// Add uncompleted tasks
	RequeueTaskIfNeeded(logger, tasks)
}

func RequeueTaskIfNeeded(logger *zerolog.Logger, tasks []ITask) {
	// Get all uncompleted tasks
	count, _ := TaskQueueManagerInstance.AddTasks(tasks)
	logger.Info().Msgf("[tms] Requeued %d. (%d tasks in queue)", count, len(tasks))
}

func AddTask(task ITask, logger *zerolog.Logger) {
	if TaskQueueManagerInstance == nil || TaskQueueManagerInstance.HasShutdownRequest() {
		return
	}
	tries := 0
	for {
		if tries >= addMaxRetries {
			logger.Debug().Msg("[tms|add-task] Task not added, max retries reached")
			return
		}
		taskManagerMutex.Lock()
		for TaskQueueManagerInstance == nil || !TaskQueueManagerInstance.IsRunning() {
			taskManagerCond.Wait()
		}
		tmInstance := TaskQueueManagerInstance
		taskManagerMutex.Unlock()

		// Try to add the task
		if added := tmInstance.AddTask(task); !added {
			logger.Debug().Msg("[tms|add-task] Task not added, retrying")
			time.Sleep(250 * time.Millisecond)
			tries++
			continue
		}
		break
	}
}