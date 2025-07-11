package smt

import (
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// TaskManagerSimple interface (optimized)
type TaskManagerSimple struct {
	providers            map[string]*ProviderData
	taskInQueue          map[string]struct{}
	taskInQueueMu        sync.RWMutex
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
	availableServers    chan string
	taskCount           int32 // atomic counter for tasks
	commandCount        int32 // atomic counter for commands

	commandQueue   *CommandQueue
	commandSet     map[uuid.UUID]struct{}
	commandSetLock sync.Mutex
}

func NewTaskManagerSimple(
	providers *[]IProvider,
	servers map[string][]string,
	logger *zerolog.Logger,
	getTimeout func(string, string) time.Duration,
) *TaskManagerSimple {
	tm := &TaskManagerSimple{
		providers:            make(map[string]*ProviderData),
		taskInQueue:          make(map[string]struct{}),
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
			taskQueue:        make(TaskQueuePrio, 0, 1024), // Pre-allocate for better performance
			taskQueueLock:    sync.Mutex{},
			commandQueue:     NewCommandQueue(256), // Increased initial capacity to reduce resizing
			commandSet:       make(map[uuid.UUID]struct{}),
			commandSetLock:   sync.Mutex{},
			servers:          serverList,
			availableServers: make(chan string, len(serverList)*2), // Double buffer for re-queuing scenarios
		}

		// Fill the channel with all servers
		for _, srv := range serverList {
			pd.availableServers <- srv
		}

		// IMPORTANT: tie the condition to the same mutex used for the queue
		pd.taskQueueCond = sync.NewCond(&pd.taskQueueLock)

		tm.providers[providerName] = pd
	}

	return tm
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

// Helper methods for thread-safe taskInQueue operations
func (tm *TaskManagerSimple) isTaskInQueue(taskID string) bool {
	tm.taskInQueueMu.RLock()
	_, exists := tm.taskInQueue[taskID]
	tm.taskInQueueMu.RUnlock()
	return exists
}

func (tm *TaskManagerSimple) addTaskToQueue(taskID string) bool {
	tm.taskInQueueMu.Lock()
	if _, exists := tm.taskInQueue[taskID]; exists {
		tm.taskInQueueMu.Unlock()
		return false // Task already in queue
	}
	tm.taskInQueue[taskID] = struct{}{}
	tm.taskInQueueMu.Unlock()
	return true // Task was added
}

// delTaskInQueue removes a task ID from the map
func (tm *TaskManagerSimple) delTaskInQueue(task ITask) {
	tm.taskInQueueMu.Lock()
	delete(tm.taskInQueue, task.GetID())
	// Periodically recreate the map to release memory
	// This prevents long-term memory growth from map internals
	if len(tm.taskInQueue) == 0 {
		// Map is empty, recreate it to release internal buckets
		tm.taskInQueue = make(map[string]struct{})
	}
	tm.taskInQueueMu.Unlock()
}

func (tm *TaskManagerSimple) AddTasks(tasks []ITask) (count int, err error) {
	for _, task := range tasks {
		if tm.AddTask(task) {
			count++
		}
	}
	return count, err
}

// AddTask enqueues a task if not already known. Returns true if successfully enqueued.
func (tm *TaskManagerSimple) AddTask(task ITask) bool {
	if atomic.LoadInt32(&tm.isRunning) != 1 {
		// Manager not running; cannot queue
		return false
	}

	taskID := task.GetID()
	provider := task.GetProvider()
	if provider == nil {
		err := fmt.Errorf("task '%s' has no provider", taskID)
		tm.logger.Error().Err(err).Msg("[tms|nil_provider|error]")
		task.MarkAsFailed(0, err)
		task.OnComplete()
		return false
	}

	providerName := provider.Name()
	pd, ok := tm.providers[providerName]
	if !ok {
		err := fmt.Errorf(errProviderNotFound, providerName)
		tm.logger.Error().Err(err).
			Str("provider", providerName).
			Str("taskID", taskID).
			Msg("[tms] provider not found")
		task.MarkAsFailed(0, err)
		task.OnComplete()
		return false
	}

	// Use our thread-safe helper method
	if !tm.addTaskToQueue(taskID) {
		return false
	}

	// Push into the priority queue
	priority := task.GetPriority()
	twp := taskWithPriorityPool.Get().(*TaskWithPriority)
	twp.task = task
	twp.priority = priority
	twp.index = 0

	pd.taskQueueLock.Lock()
	heap.Push(&pd.taskQueue, twp)
	atomic.AddInt32(&pd.taskCount, 1)
	pd.taskQueueCond.Signal()
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


func (tm *TaskManagerSimple) providerDispatcher(providerName string) {
	defer tm.wg.Done()
	pd := tm.providers[providerName]
	shutdownCh := tm.shutdownCh

	const batchSize = 4
	serverBatch := make([]string, 0, batchSize)

	var isCommand bool
	var command Command
	var task ITask
	var taskWithPriority *TaskWithPriority
	var hasWork bool
	var server string

	for {
		// If we have leftover servers in the local batch, try to use them
		if len(serverBatch) > 0 {
			server = serverBatch[0]
			serverBatch = serverBatch[1:]

			isCommand = false
			command = Command{}
			task = nil
			hasWork = false

			pd.taskQueueLock.Lock()
			if pd.taskQueue.Len() > 0 {
				taskWithPriority = heap.Pop(&pd.taskQueue).(*TaskWithPriority)
				atomic.AddInt32(&pd.taskCount, -1)
				task = taskWithPriority.task
				hasWork = true
			} else if pd.commandQueue.Len() > 0 {
				command, _ = pd.commandQueue.Dequeue()
				atomic.AddInt32(&pd.commandCount, -1)
				isCommand = true
				hasWork = true
			} else {
				// No tasks or commands; return the server
				pd.taskQueueLock.Unlock()
				pd.availableServers <- server
				continue
			}
			pd.taskQueueLock.Unlock()

			if hasWork {
				tm.wg.Add(1)
				if isCommand {
					go tm.processCommand(command, providerName, server)
				} else {
					go tm.processTask(task, providerName, server)
				}
			}
			continue
		}

		// Otherwise, wait for tasks/commands
		isCommand = false
		command = Command{}
		taskWithPriority = nil
		hasWork = false

		// Fast path: check atomic counters first
		if atomic.LoadInt32(&pd.taskCount) == 0 && atomic.LoadInt32(&pd.commandCount) == 0 {
			pd.taskQueueLock.Lock()
			// Double-check under lock
			for pd.taskQueue.Len() == 0 && pd.commandQueue.Len() == 0 && !tm.HasShutdownRequest() {
				pd.taskQueueCond.Wait()
			}
			pd.taskQueueLock.Unlock()
			if tm.HasShutdownRequest() {
				return
			}
		}

		pd.taskQueueLock.Lock()
		if pd.taskQueue.Len() > 0 {
			taskWithPriority = heap.Pop(&pd.taskQueue).(*TaskWithPriority)
			atomic.AddInt32(&pd.taskCount, -1)
			hasWork = true
		} else if pd.commandQueue.Len() > 0 {
			command, _ = pd.commandQueue.Dequeue()
			atomic.AddInt32(&pd.commandCount, -1)
			isCommand = true
			hasWork = true
		}
		pd.taskQueueLock.Unlock()

		if !hasWork {
			continue
		}

		// Grab a server
		select {
		case <-shutdownCh:
			if !isCommand && taskWithPriority != nil {
				// Return TWP to pool
				taskWithPriority.task = nil
				taskWithPriorityPool.Put(taskWithPriority)
			}
			return

		case server = <-pd.availableServers:
			tm.wg.Add(1)
			if isCommand {
				go tm.processCommand(command, providerName, server)
			} else {
				task = taskWithPriority.task
				// Return TWP to pool
				taskWithPriority.task = nil
				taskWithPriorityPool.Put(taskWithPriority)

				go tm.processTask(task, providerName, server)
			}

			// Skip server batching to reduce complexity
		}
	}
}

type contextKey string

const (
	taskIDKey       contextKey = "taskID"
	providerNameKey contextKey = "providerName"
	serverNameKey   contextKey = "serverName"
)

func (tm *TaskManagerSimple) processTask(task ITask, providerName, server string) {
	started := time.Now()
	var onCompleteCalled bool

	taskID := task.GetID()
	provider := task.GetProvider()
	pd, providerExists := tm.providers[providerName]

	// 1) Acquire concurrency FIRST (non-blocking with retry).
	semaphore, hasLimit := tm.getServerSemaphore(server)
	if hasLimit {
		// Try to acquire non-blocking first
		select {
		case semaphore <- struct{}{}:
			tm.logger.Debug().
				Str("server", server).
				Str("taskID", taskID).
				Msg("[tms|processTask] Acquired concurrency slot immediately!")
		default:
			// If blocked, return server and re-queue task
			tm.logger.Debug().
				Str("server", server).
				Str("taskID", taskID).
				Msg("[tms|processTask] Concurrency limit reached, re-queuing task")
			
			// Return server to pool
			tm.returnServerToPool(providerExists, pd, server)
			
			// Re-queue without incrementing retry counter
			// Concurrency limits shouldn't count as failures
			tm.delTaskInQueue(task)
			
			// Add small backoff to prevent tight loop
			time.Sleep(5 * time.Millisecond)
			
			tm.AddTask(task)
			return
		}
	}

	defer tm.wg.Done()
	defer func() {
		// 4) Always release concurrency when done
		if hasLimit {
			<-semaphore
			tm.logger.Debug().
				Str("server", server).
				Str("taskID", taskID).
				Msg("[tms|processTask] Released concurrency slot!")
		}
		// Return server to the pool
		tm.returnServerToPool(providerExists, pd, server)
	}()

	// Recover from panic ...
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic occurred: %v\n%s", r, string(debug.Stack()))
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic", providerName, taskID, server)
			now := time.Now()
			elapsed := now.Sub(started)
			task.MarkAsFailed(elapsed.Milliseconds(), err)
			if !onCompleteCalled {
				task.OnComplete()
				onCompleteCalled = true
			}
			tm.delTaskInQueue(task)
		}
	}()

	// Validate provider
	if provider == nil {
		err := fmt.Errorf("task '%s' has no provider", taskID)
		tm.logger.Error().Err(err).
			Msgf("[tms|%s|%s|%s] Task has no provider", providerName, taskID, server)
		now := time.Now()
		task.MarkAsFailed(now.Sub(started).Milliseconds(), err)
		if !onCompleteCalled {
			task.OnComplete()
			onCompleteCalled = true
		}
		tm.delTaskInQueue(task)
		return
	}

	// 2) Now that concurrency is acquired, we start the actual timed work:
	err, totalTime := tm.HandleWithTimeout(providerName, task, server, tm.HandleTask)
	if err != nil {
		retries := task.GetRetries()
		maxRetries := task.GetMaxRetries()
		if retries >= maxRetries || err == sql.ErrNoRows {
			// Final failure
			tm.logger.Error().Err(err).
				Msgf("[tms|%s|%s|%s] max retries reached or no rows", providerName, taskID, server)
			task.MarkAsFailed(totalTime, err)
			if !onCompleteCalled {
				task.OnComplete()
				onCompleteCalled = true
			}
			tm.delTaskInQueue(task)
		} else {
			// Retry scenario
			if level := tm.logger.GetLevel(); level <= zerolog.DebugLevel {
				tm.logger.Debug().
					Err(err).
					Str("provider", providerName).
					Str("taskID", taskID).
					Str("server", server).
					Int("retry", retries+1).
					Int("maxRetries", maxRetries).
					Msg("[tms] retrying task")
			}
			task.UpdateRetries(retries + 1)
			tm.delTaskInQueue(task) // remove so it can be re-added
			tm.AddTask(task)
		}
	} else {
		// Success
		task.MarkAsSuccess(totalTime)
		if !onCompleteCalled {
			task.OnComplete()
			onCompleteCalled = true
		}
		tm.delTaskInQueue(task)
	}
}

// HandleTask calls provider.Handle(task, server) directly
func (tm *TaskManagerSimple) HandleTask(task ITask, server string) error {
	provider := task.GetProvider()
	if provider == nil {
		return fmt.Errorf("task '%s' has no provider", task.GetID())
	}
	return provider.Handle(task, server)
}
func (tm *TaskManagerSimple) returnServerToPool(providerExists bool, pd *ProviderData, server string) {
	if !providerExists {
		return
	}
	// Always block to ensure server is returned
	pd.availableServers <- server
}

// getServerSemaphore checks if the server has a concurrency limit.
func (tm *TaskManagerSimple) getServerSemaphore(server string) (chan struct{}, bool) {
	// Optional: parse out query, to unify concurrency for any query string
	if u, err := url.Parse(server); err == nil {
		// Remove query and fragment, so "https://foo?x=1" => "https://foo"
		u.RawQuery = ""
		u.Fragment = ""
		server = u.String()
	}

	tm.serverConcurrencyMu.RLock()
	defer tm.serverConcurrencyMu.RUnlock()

	// If the server URL or name starts with a known prefix, return that semaphore
	for prefix, sem := range tm.serverConcurrencyMap {
		if strings.HasPrefix(server, prefix) {
			return sem, true
		}
	}
	return nil, false
}

// Shutdown signals and waits for all provider dispatchers
func (tm *TaskManagerSimple) Shutdown() {
	if !tm.IsRunning() {
		if tm.logger.GetLevel() <= zerolog.DebugLevel {
			tm.logger.Debug().Msg("[tms] Task manager shutdown [ALREADY STOPPED]")
		}
		return
	}
	atomic.StoreInt32(&tm.shutdownRequest, 1)
	close(tm.shutdownCh)

	// Wake dispatchers one by one to avoid thundering herd
	for _, pd := range tm.providers {
		pd.taskQueueLock.Lock()
		// Signal instead of Broadcast to wake one at a time
		pd.taskQueueCond.Signal()
		pd.taskQueueLock.Unlock()
		// Small delay to stagger wakeups
		time.Sleep(1 * time.Millisecond)
	}

	tm.wg.Wait()
	atomic.StoreInt32(&tm.isRunning, 0)

	if tm.logger.GetLevel() <= zerolog.DebugLevel {
		tm.logger.Debug().Msg("[tms] Task manager shutdown [FINISHED]")
	}
}

// HandleWithTimeout wraps the provider handler in a context timeout
func (tm *TaskManagerSimple) HandleWithTimeout(
	pn string,
	task ITask,
	server string,
	handler func(ITask, string) error,
) (error, int64) {

	var err error
	taskID := task.GetID()
	callbackName := task.GetCallbackName()

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic occurred: %v", r)
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic in task", pn, taskID, server)
		}
	}()

	maxTimeout := tm.getTimeout(callbackName, pn)

	ctx := context.WithValue(context.Background(), taskIDKey, taskID)
	ctx = context.WithValue(ctx, providerNameKey, pn)
	ctx = context.WithValue(ctx, serverNameKey, server)
	ctx, cancel := context.WithTimeout(ctx, maxTimeout)
	defer cancel()

	done := make(chan error, 1)
	startTime := time.Now()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				e := fmt.Errorf("panic occurred in handler: %v\n%s", r, string(debug.Stack()))
				tm.logger.Error().Err(e).
					Msgf("[tms|%s|%s|%s] panic in handler", pn, taskID, server)
				done <- e
			}
		}()

		if tm.logger.GetLevel() <= zerolog.DebugLevel {
			tm.logger.Debug().
				Str("provider", pn).
				Str("taskID", taskID).
				Str("server", server).
				Msg("[tms|HandleWithTimeout] Task STARTED")
		}

		// Actually run the provider's handle
		done <- handler(task, server)
	}()

	var elapsed time.Duration
	select {
	case <-ctx.Done():
		now := time.Now()
		elapsed = now.Sub(startTime)
		err = fmt.Errorf(errTaskTimeout, pn, taskID, server)
		tm.logger.Error().
			Err(err).
			Msgf("[%s|%s] Task FAILED-TIMEOUT on server %s, took %s", pn, taskID, server, elapsed)
	case e := <-done:
		now := time.Now()
		elapsed = now.Sub(startTime)
		err = e
		if err == nil {
			if level := tm.logger.GetLevel(); level <= zerolog.DebugLevel {
				tm.logger.Debug().
					Str("provider", pn).
					Str("taskID", taskID).
					Str("server", server).
					Dur("duration", elapsed).
					Msg("[tms] Task COMPLETED")
			}
		} else {
			tm.logger.Error().
				Err(err).
				Str("provider", pn).
				Str("taskID", taskID).
				Str("server", server).
				Dur("duration", elapsed).
				Msg("[tms] Task FAILED")
		}
	}
	return err, elapsed.Milliseconds()
}

var (
	TaskQueueManagerInstance *TaskManagerSimple
	// Only used to guard TaskQueueManagerInstance creation/usage:
	taskManagerMutex sync.Mutex
	taskManagerCond  = sync.NewCond(&taskManagerMutex)
	addMaxRetries    = 3

	// Pool for TaskWithPriority objects
	taskWithPriorityPool = sync.Pool{
		New: func() interface{} {
			return &TaskWithPriority{}
		},
	}

	// Common error strings
	errTaskNoProvider   = "task '%s' has no provider"
	errProviderNotFound = "provider '%s' not found"
	errTaskTimeout      = "[tms|%s|%s] Task timed out on server %s"
	errPanicOccurred    = "panic occurred: %v\n%s"
	errPanicInHandler   = "panic occurred in handler: %v\n%s"
)

// InitTaskQueueManager creates and starts the global manager
func InitTaskQueueManager(
	logger *zerolog.Logger,
	providers *[]IProvider,
	tasks []ITask,
	servers map[string][]string,
	getTimeout func(string, string) time.Duration,
) {
	taskManagerMutex.Lock()
	defer taskManagerMutex.Unlock()

	logger.Info().Msg("[tms] Task manager initialization")

	tm := NewTaskManagerSimple(providers, servers, logger, getTimeout)
	tm.Start()
	
	// Use atomic store for lock-free access
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&TaskQueueManagerInstance)), unsafe.Pointer(tm))

	logger.Info().Msg("[tms] Task manager started")
	taskManagerCond.Broadcast()

	// Requeue any uncompleted tasks
	RequeueTaskIfNeeded(logger, tasks)
}

// RequeueTaskIfNeeded re-injects tasks that were incomplete
func RequeueTaskIfNeeded(logger *zerolog.Logger, tasks []ITask) {
	count, _ := TaskQueueManagerInstance.AddTasks(tasks)
	logger.Info().Msgf("[tms] Requeued %d. (%d tasks in queue)", count, len(tasks))
}

// AddTask is the global helper for adding tasks
func AddTask(task ITask, logger *zerolog.Logger) {
	// Fast path: check if manager exists without lock
	tm := (*TaskManagerSimple)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&TaskQueueManagerInstance))))
	if tm == nil {
		return
	}
	
	if tm.HasShutdownRequest() {
		return
	}
	
	// Direct add without retries for duplicates
	if !tm.AddTask(task) {
		logger.Debug().Str("taskID", task.GetID()).Msg("[tms|add-task] Task not added (duplicate)")
	}
}
