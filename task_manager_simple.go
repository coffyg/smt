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
	availableServers    chan string
	availableServerList []string
	availableServerLock sync.RWMutex

	commandQueue   *CommandQueue
	commandSet     map[uuid.UUID]struct{}
	commandSetLock sync.Mutex
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
func (pd *ProviderData) IsServerAvailable(serverName string) bool {
	pd.availableServerLock.RLock()
	defer pd.availableServerLock.RUnlock()

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

// delTaskInQueue removes a task ID from the map. We now use this **only**
// when we specifically intend to re-queue a task after partial failure.
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

// AddTask attempts to add a task once. If it is already known to the manager,
// the function returns false. Otherwise it enqueues the task and returns true.
func (tm *TaskManagerSimple) AddTask(task ITask) bool {
	// Fast check to see if manager is running
	if atomic.LoadInt32(&tm.isRunning) != 1 {
		return false
	}

	// Validate the task
	taskID := task.GetID()
	provider := task.GetProvider()
	if provider == nil {
		err := fmt.Errorf("task '%s' has no provider", taskID)
		tm.logger.Error().Err(err).Msg("[tms|nil_provider|error] error")
		task.MarkAsFailed(0, err)
		task.OnComplete()
		return false
	}

	providerName := provider.Name()
	pd, ok := tm.providers[providerName]
	if !ok {
		err := fmt.Errorf(errProviderNotFound, providerName)
		tm.logger.Error().
			Err(err).
			Str("provider", providerName).
			Str("taskID", taskID).
			Msg("[tms] provider not found error")
		task.MarkAsFailed(0, err)
		task.OnComplete()
		return false
	}

	// Atomically ensure we only accept this task once
	if _, loaded := tm.taskInQueue.LoadOrStore(taskID, struct{}{}); loaded {
		// Already known, skip
		return false
	}

	// Insert into provider's priority queue
	priority := task.GetPriority()
	twp := taskWithPriorityPool.Get().(*TaskWithPriority)
	twp.task = task
	twp.priority = priority
	twp.index = 0

	pd.taskQueueLock.Lock()
	heap.Push(&pd.taskQueue, twp)
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

// HasAvailableServer checks if the provider has any available servers without blocking
func (pd *ProviderData) HasAvailableServer() bool {
	pd.availableServerLock.RLock()
	defer pd.availableServerLock.RUnlock()
	return len(pd.availableServerList) > 0
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
	var hasWork bool
	var taskWithPriority *TaskWithPriority
	var server string
	var serverCount int

	for {
		// If we have leftover servers in the local batch, use them first
		if len(serverBatch) > 0 {
			server = serverBatch[0]
			serverBatch = serverBatch[1:]

			isCommand = false
			command = Command{}
			task = nil
			hasWork = false

			pd.taskQueueLock.Lock()
			if pd.taskQueue.Len() > 0 {
				// Pop a task
				taskWithPriority = heap.Pop(&pd.taskQueue).(*TaskWithPriority)
				task = taskWithPriority.task
				hasWork = true
			} else if pd.commandQueue.Len() > 0 {
				// Dequeue a command
				command, _ = pd.commandQueue.Dequeue()
				isCommand = true
				hasWork = true
			} else {
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
					go tm.processTask(task, providerName, server)
				}
			}
			continue
		}

		// Otherwise, wait for work
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
			taskWithPriority = heap.Pop(&pd.taskQueue).(*TaskWithPriority)
			hasWork = true
		} else if pd.commandQueue.Len() > 0 {
			command, _ = pd.commandQueue.Dequeue()
			isCommand = true
			hasWork = true
		} else {
			pd.taskQueueLock.Unlock()
			continue
		}
		pd.taskQueueLock.Unlock()

		if !hasWork {
			continue
		}

		// Grab a server
		select {
		case <-shutdownCh:
			if !isCommand && taskWithPriority != nil {
				// Return it to pool
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
				// Return the struct to the pool
				taskWithPriority.task = nil
				taskWithPriorityPool.Put(taskWithPriority)

				go tm.processTask(task, providerName, server)
			}

			// Try to grab more servers for batch usage
			serverCount = 0
			serverBatch = serverBatch[:0]
			for serverCount < batchSize-1 {
				select {
				case s := <-pd.availableServers:
					serverBatch = append(serverBatch, s)
					serverCount++
				default:
					goto doneBatch
				}
			}
		doneBatch:
		}
	}
}

// For caching time so we don’t call time.Now() too frequently
var (
	cachedTime     atomic.Value
	cachedTimeOnce sync.Once
)

type contextKey string

const (
	taskIDKey       contextKey = "taskID"
	providerNameKey contextKey = "providerName"
	serverNameKey   contextKey = "serverName"
)

func getCachedTime() time.Time {
	cachedTimeOnce.Do(func() {
		t := time.Now()
		cachedTime.Store(t)

		go func() {
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()

			for now := range ticker.C {
				cachedTime.Store(now)
			}
		}()
	})
	return cachedTime.Load().(time.Time)
}

func (tm *TaskManagerSimple) processTask(task ITask, providerName, server string) {
	started := getCachedTime()
	var onCompleteCalled bool

	taskID := task.GetID()
	provider := task.GetProvider()

	pd, providerExists := tm.providers[providerName]

	// Check concurrency limits
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
		// Return the server to the pool
		if providerExists {
			select {
			case pd.availableServers <- server:
			default:
				go func(s string) {
					pd.availableServers <- s
				}(server)
			}
		}
	}()

	// Recover from panics
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic occurred: %v\n%s", r, string(debug.Stack()))
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic", providerName, taskID, server)
			now := getCachedTime()
			elapsed := now.Sub(started)
			task.MarkAsFailed(elapsed.Milliseconds(), err)
			if !onCompleteCalled {
				task.OnComplete()
				onCompleteCalled = true
			}
			// IMPORTANT: do NOT remove from taskInQueue if final, so duplicates don't re-add
			// (No re-try if we panicked)
		}
	}()

	// Ensure provider is valid
	if provider == nil {
		err := fmt.Errorf("task '%s' has no provider", taskID)
		tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] Task has no provider", providerName, taskID, server)
		now := getCachedTime()
		task.MarkAsFailed(now.Sub(started).Milliseconds(), err)
		if !onCompleteCalled {
			task.OnComplete()
			onCompleteCalled = true
		}
		// do NOT remove from map if final
		return
	}

	// Handle with timeout
	err, totalTime := tm.HandleWithTimeout(providerName, task, server, tm.HandleTask)
	if err != nil {
		retries := task.GetRetries()
		maxRetries := task.GetMaxRetries()
		if retries >= maxRetries || err == sql.ErrNoRows {
			// Final error
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] max retries reached", providerName, taskID, server)
			task.MarkAsFailed(totalTime, err)
			if !onCompleteCalled {
				task.OnComplete()
				onCompleteCalled = true
			}
			// do NOT remove from map => ensures no duplicates
		} else {
			// Re-try scenario
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
			// We remove from map so that re-AddTask can succeed:
			tm.delTaskInQueue(task)
			tm.AddTask(task)
		}
	} else {
		// Success
		task.MarkAsSuccess(totalTime)
		if !onCompleteCalled {
			task.OnComplete()
			onCompleteCalled = true
		}
		// do NOT remove => ensures we never re-add
	}
}

// HandleTask is just the direct call to provider.Handle
func (tm *TaskManagerSimple) HandleTask(task ITask, server string) error {
	provider := task.GetProvider()
	if provider == nil {
		return fmt.Errorf("task '%s' has no provider", task.GetID())
	}
	return provider.Handle(task, server)
}

// Shutdown stops all dispatchers and waits for them to exit
func (tm *TaskManagerSimple) Shutdown() {
	if !tm.IsRunning() {
		if tm.logger.GetLevel() <= zerolog.DebugLevel {
			tm.logger.Debug().Msg("[tms] Task manager shutdown [ALREADY STOPPED]")
		}
		return
	}
	atomic.StoreInt32(&tm.shutdownRequest, 1)
	close(tm.shutdownCh)

	// Wake all dispatchers
	for _, pd := range tm.providers {
		pd.taskQueueLock.Lock()
		pd.taskQueueCond.Broadcast()
		pd.taskQueueLock.Unlock()
	}

	tm.wg.Wait()
	atomic.StoreInt32(&tm.isRunning, 0)

	if tm.logger.GetLevel() <= zerolog.DebugLevel {
		tm.logger.Debug().Msg("[tms] Task manager shutdown [FINISHED]")
	}
}

// HandleWithTimeout wraps the actual provider handling in a timeout context
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
	startTime := getCachedTime()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				e := fmt.Errorf("panic occurred in handler: %v\n%s", r, string(debug.Stack()))
				tm.logger.Error().Err(e).Msgf("[tms|%s|%s|%s] panic in handler", pn, taskID, server)
				done <- e
			}
		}()

		if tm.logger.GetLevel() <= zerolog.DebugLevel {
			tm.logger.Debug().Msgf("[tms|%s|%s] Task STARTED on server %s", pn, taskID, server)
		}
		done <- handler(task, server)
	}()

	var elapsed time.Duration
	select {
	case <-ctx.Done():
		now := getCachedTime()
		elapsed = now.Sub(startTime)
		err = fmt.Errorf(errTaskTimeout, pn, taskID, server)
		tm.logger.Error().
			Err(err).
			Msgf("[%s|%s] Task FAILED-TIMEOUT on server %s, took %s", pn, taskID, server, elapsed)
	case e := <-done:
		now := getCachedTime()
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
	taskManagerMutex         sync.Mutex
	taskManagerCond          = sync.NewCond(&taskManagerMutex)
	addMaxRetries            = 3

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

	TaskQueueManagerInstance = NewTaskManagerSimple(providers, servers, logger, getTimeout)
	TaskQueueManagerInstance.Start()
	logger.Info().Msg("[tms] Task manager started")

	// Signal that the TaskManager is ready
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

		if added := tmInstance.AddTask(task); !added {
			logger.Debug().Msg("[tms|add-task] Task not added, retrying")
			time.Sleep(250 * time.Millisecond)
			tries++
			continue
		}
		break
	}
}
