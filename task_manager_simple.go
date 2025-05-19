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

func NewTaskManagerSimple(
	providers *[]IProvider,
	servers map[string][]string,
	logger *zerolog.Logger,
	getTimeout func(string, string) time.Duration,
) *TaskManagerSimple {
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
			commandQueue:     NewCommandQueue(32), // user code for CommandQueue (assumed present)
			commandSet:       make(map[uuid.UUID]struct{}),
			commandSetLock:   sync.Mutex{},
			servers:          serverList,
			availableServers: make(chan string, len(serverList)),
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

// IsServerAvailable checks if a server is available without blocking
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

// Helper methods for thread-safe taskInQueue operations
func (tm *TaskManagerSimple) isTaskInQueue(taskID string) bool {
	_, exists := tm.taskInQueue.Load(taskID)
	return exists
}

func (tm *TaskManagerSimple) addTaskToQueue(taskID string) bool {
	_, loaded := tm.taskInQueue.LoadOrStore(taskID, struct{}{})
	return !loaded // Returns true if task was added, false if it was already there
}

// delTaskInQueue removes a task ID from the map
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
	var taskWithPriority *TaskWithPriority
	var hasWork bool
	var server string
	var serverCount int

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
				task = taskWithPriority.task
				hasWork = true
			} else if pd.commandQueue.Len() > 0 {
				command, _ = pd.commandQueue.Dequeue()
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

			// Collect more servers into batch
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

// Basic time-caching to avoid frequent time.Now() calls
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

	// 1) Acquire concurrency FIRST (this can block until there's space).
	semaphore, hasLimit := tm.getServerSemaphore(server)
	if hasLimit {
		tm.logger.Debug().
			Str("server", server).
			Str("taskID", taskID).
			Msg("[tms|processTask] Waiting for concurrency slot...")

		semaphore <- struct{}{} // blocks if no slot free

		tm.logger.Debug().
			Str("server", server).
			Str("taskID", taskID).
			Msg("[tms|processTask] Acquired concurrency slot!")
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

	// Validate provider
	if provider == nil {
		err := fmt.Errorf("task '%s' has no provider", taskID)
		tm.logger.Error().Err(err).
			Msgf("[tms|%s|%s|%s] Task has no provider", providerName, taskID, server)
		now := getCachedTime()
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
	select {
	case pd.availableServers <- server:
	default:
		// fallback to a goroutine if the channel is full
		go func(s string) {
			pd.availableServers <- s
		}(server)
	}
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
	startTime := getCachedTime()

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
				Msgf("[tms|%s|%s] Task STARTED on server %s", pn, taskID, server)
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

	TaskQueueManagerInstance = NewTaskManagerSimple(providers, servers, logger, getTimeout)
	TaskQueueManagerInstance.Start()

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
