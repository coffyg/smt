package smt

import (
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"runtime"
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
	isRunning            atomic.Bool
	shutdownRequest      atomic.Bool
	shutdownCh           chan struct{}
	wg                   sync.WaitGroup
	logger               *zerolog.Logger
	getTimeout           func(string, string) time.Duration
	serverConcurrencyMap map[string]chan struct{} // Map of servers to semaphores
	serverConcurrencyMu  sync.RWMutex
}

type ProviderData struct {
	taskQueue        TaskQueuePrio
	taskQueueLock    sync.Mutex
	taskQueueCond    *sync.Cond
	servers          []string
	availableServers chan string
	commandQueue     *CommandQueue
	commandSet       map[uuid.UUID]struct{}
	commandSetLock   sync.Mutex
}

func NewTaskManagerSimple(providers *[]IProvider, servers map[string][]string, logger *zerolog.Logger, getTimeout func(string, string) time.Duration) *TaskManagerSimple {
	tm := &TaskManagerSimple{
		providers:            make(map[string]*ProviderData, len(*providers)),
		shutdownCh:           make(chan struct{}),
		logger:               logger,
		getTimeout:           getTimeout,
		serverConcurrencyMap: make(map[string]chan struct{}),
		serverConcurrencyMu:  sync.RWMutex{},
	}

	// Initialize providers - this is done only once, so optimize for clarity
	for _, provider := range *providers {
		providerName := provider.Name()
		serverList, ok := servers[providerName]
		if !ok {
			serverList = []string{}
		}
		
		// Pre-allocate slice capacity
		serverCount := len(serverList)
		pd := &ProviderData{
			taskQueue:        TaskQueuePrio{},
			taskQueueLock:    sync.Mutex{},
			commandQueue:     NewCommandQueue(32), // Initialize with a reasonable capacity
			commandSet:       make(map[uuid.UUID]struct{}, 64),
			commandSetLock:   sync.Mutex{},
			servers:          serverList,
			availableServers: make(chan string, serverCount),
		}
		
		// Buffer the server channel to avoid blocking
		for _, server := range serverList {
			pd.availableServers <- server
		}
		
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
	return tm.shutdownRequest.Load()
}

func (tm *TaskManagerSimple) IsRunning() bool {
	return tm.isRunning.Load()
}

func (tm *TaskManagerSimple) setTaskInQueue(taskID string) {
	tm.taskInQueue.Store(taskID, struct{}{})
}

func (tm *TaskManagerSimple) isTaskInQueue(taskID string) bool {
	_, ok := tm.taskInQueue.Load(taskID)
	return ok
}

func (tm *TaskManagerSimple) delTaskInQueue(taskID string) {
	tm.taskInQueue.Delete(taskID)
}

func (tm *TaskManagerSimple) AddTasks(tasks []ITask) (count int, err error) {
	// Fast path: If not running, return early
	if !tm.IsRunning() {
		return 0, nil
	}
	
	// Pre-allocate taskIDs to reduce allocs
	taskIDs := make(map[string]struct{}, len(tasks))
	
	// First pass: check if tasks are already in queue (fast reject)
	for _, task := range tasks {
		taskID := task.GetID()
		if tm.isTaskInQueue(taskID) {
			continue
		}
		taskIDs[taskID] = struct{}{}
	}
	
	// If no valid tasks, return early
	if len(taskIDs) == 0 {
		return 0, nil
	}
	
	// Second pass: add valid tasks
	for _, task := range tasks {
		taskID := task.GetID()
		if _, ok := taskIDs[taskID]; !ok {
			continue
		}
		
		// Add task and increment count if successful
		if tm.AddTask(task) {
			count++
		}
	}
	
	return count, nil
}

func (tm *TaskManagerSimple) AddTask(task ITask) bool {
	// Fail fast if not running
	if !tm.IsRunning() {
		return false
	}

	taskID := task.GetID()
	if tm.isTaskInQueue(taskID) {
		return false
	}

	provider := task.GetProvider()
	if provider == nil {
		err := newTaskError("AddTask", taskID, "nil", "", ErrTaskHasNoProvider)
		tm.logger.Error().Err(err).Msg("[tms|nil_provider|error] error")
		task.MarkAsFailed(0, err)
		task.OnComplete() // Must call this for initial failure
		err.Release() // Return to pool
		return false
	}

	providerName := provider.Name()
	pd, ok := tm.providers[providerName]
	if !ok {
		err := newTaskError("AddTask", taskID, providerName, "", ErrProviderNotFound)
		tm.logger.Error().Err(err).Msgf("[tms|%s|%s] error", providerName, taskID)
		task.MarkAsFailed(0, err)
		task.OnComplete() // Must call this for initial failure
		err.Release() // Return to pool
		return false
	}

	// Now that the task is confirmed valid and the provider exists, set it as in queue
	tm.setTaskInQueue(taskID)

	pd.taskQueueLock.Lock()
	heap.Push(&pd.taskQueue, &TaskWithPriority{
		task:     task,
		priority: task.GetPriority(),
	})
	pd.taskQueueCond.Signal() // Signal that a new task is available
	pd.taskQueueLock.Unlock()

	return true
}

func (tm *TaskManagerSimple) Start() {
	// Use atomic to ensure we only start once
	if !tm.isRunning.CompareAndSwap(false, true) {
		return
	}

	// Calculate number of worker goroutines based on providers and available cores
	numWorkers := 0
	for providerName, pd := range tm.providers {
		// Use min(numServers*2, availableCores/numProviders) for each provider
		numServers := len(pd.servers)
		if numServers > 0 {
			tm.wg.Add(1)
			go tm.providerDispatcher(providerName)
			numWorkers++
		}
	}
	
	// If we have no workers (no servers), log a warning
	if numWorkers == 0 {
		tm.logger.Warn().Msg("[tms] Task manager started with no servers available")
	}
}

func (tm *TaskManagerSimple) providerDispatcher(providerName string) {
	defer tm.wg.Done()
	pd := tm.providers[providerName]

	// Local variables to avoid repeated map lookups
	taskQueue := &pd.taskQueue
	commandQueue := pd.commandQueue
	availableServers := pd.availableServers
	
	for {
		pd.taskQueueLock.Lock()
		// Wait for tasks, commands, or shutdown
		for taskQueue.Len() == 0 && commandQueue.Len() == 0 && !tm.HasShutdownRequest() {
			pd.taskQueueCond.Wait()
		}
		
		// Check shutdown first to avoid unnecessary work
		if tm.HasShutdownRequest() {
			pd.taskQueueLock.Unlock()
			return
		}

		var isCommand bool
		var command Command
		var taskWithPriority *TaskWithPriority

		if taskQueue.Len() > 0 {
			// Get the next task
			taskWithPriority = heap.Pop(taskQueue).(*TaskWithPriority)
			isCommand = false
		} else if commandQueue.Len() > 0 {
			// Dequeue the command
			command, _ = commandQueue.Dequeue()
			isCommand = true
		}
		pd.taskQueueLock.Unlock()

		// Wait for an available server using select
		select {
		case <-tm.shutdownCh:
			return
		case server := <-availableServers:
			// We got a server, process the task or command
			tm.wg.Add(1)
			if isCommand {
				go tm.processCommand(command, providerName, server)
			} else {
				go tm.processTask(taskWithPriority.task, providerName, server)
			}
		}
	}
}

func (tm *TaskManagerSimple) processTask(task ITask, providerName, server string) {
	started := time.Now()
	var onCompleteCalled bool
	taskID := task.GetID()

	// Try to acquire semaphore if server URL matches any prefix with concurrency limit
	var semaphore chan struct{}
	var hasLimit bool

	// Critical path optimization: Use RLock for concurrent reads
	tm.serverConcurrencyMu.RLock()
	for prefix, sem := range tm.serverConcurrencyMap {
		if strings.HasPrefix(server, prefix) {
			semaphore = sem
			hasLimit = true
			break
		}
	}
	tm.serverConcurrencyMu.RUnlock()
	
	// Acquire semaphore if needed
	if hasLimit {
		semaphore <- struct{}{}
	}

	defer tm.wg.Done()
	defer func() {
		// Release semaphore if needed
		if hasLimit {
			<-semaphore
		}
		
		// Return the server to the available servers pool
		pd, ok := tm.providers[providerName]
		if ok {
			pd.availableServers <- server
		}
	}()
	
	// Handle panics - IMPORTANT: This must be the last defer to execute first
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic occurred: %v\n%s", r, string(debug.Stack()))
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic", providerName, taskID, server)
			task.MarkAsFailed(time.Since(started).Milliseconds(), err)
			if !onCompleteCalled {
				task.OnComplete()
				onCompleteCalled = true
			}
			tm.delTaskInQueue(taskID)
		}
	}()

	// Ensure the task has a valid provider
	if task.GetProvider() == nil {
		err := fmt.Errorf("task '%s' has no provider", taskID)
		tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] Task has no provider", providerName, taskID, server)
		task.MarkAsFailed(time.Since(started).Milliseconds(), err)
		if !onCompleteCalled {
			task.OnComplete()
			onCompleteCalled = true
		}
		tm.delTaskInQueue(taskID)
		return
	}

	// Handle the task
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
			tm.delTaskInQueue(taskID)
		} else {
			// For retry, don't call OnComplete as the task will be requeued
			// Just update retries and remove from current queue
			tm.logger.Debug().Err(err).Msgf("[tms|%s|%s|%s] retrying (%d/%d)", providerName, taskID, server, retries+1, maxRetries)
			task.UpdateRetries(retries + 1)
			
			// First remove from queue to avoid duplicates
			tm.delTaskInQueue(taskID)
			
			// Add back to queue with new retry count
			tm.AddTask(task)
		}
	} else {
		task.MarkAsSuccess(totalTime)
		if !onCompleteCalled {
			task.OnComplete()
			onCompleteCalled = true
		}
		tm.delTaskInQueue(taskID)
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
	// Avoid double shutdown using atomic
	if !tm.isRunning.Load() {
		tm.logger.Debug().Msg("[tms] Task manager shutdown [ALREADY STOPPED]")
		return
	}
	
	// Signal shutdown request
	if !tm.shutdownRequest.CompareAndSwap(false, true) {
		// Someone else already requested shutdown
		tm.wg.Wait()
		return
	}

	// Close shutdown channel (protected by CompareAndSwap above)
	close(tm.shutdownCh)

	// Signal all provider dispatchers to wake up
	for _, pd := range tm.providers {
		pd.taskQueueLock.Lock()
		pd.taskQueueCond.Broadcast()
		pd.taskQueueLock.Unlock()
	}

	// Wait for all workers to finish
	tm.wg.Wait()
	tm.isRunning.Store(false)
	tm.logger.Debug().Msg("[tms] Task manager shutdown [FINISHED]")
}

func (tm *TaskManagerSimple) HandleWithTimeout(pn string, task ITask, server string, handler func(ITask, string) error) (error, int64) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			stack := make([]byte, 4096)
			stack = stack[:runtime.Stack(stack, false)]
			err = fmt.Errorf("panic occurred: %v\n%s", r, stack)
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic in task", pn, task.GetID(), server)
		}
	}()

	// Get timeout from task or provider
	maxTimeout := task.GetTimeout()
	if maxTimeout <= 0 {
		maxTimeout = tm.getTimeout(task.GetCallbackName(), pn)
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), maxTimeout)
	defer cancel()

	// Use buffered channel to avoid goroutine leaks
	done := make(chan error, 1)
	startTime := time.Now()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				stack := make([]byte, 4096)
				stack = stack[:runtime.Stack(stack, false)]
				err := fmt.Errorf("panic occurred in handler: %v\n%s", r, stack)
				tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic in handler", pn, task.GetID(), server)
				done <- err
			}
		}()

		// Start task and handle it
		task.OnStart()
		tm.logger.Debug().Msgf("[tms|%s|%s] Task STARTED on server %s", pn, task.GetID(), server)
		done <- handler(task, server)
	}()

	// Wait for done or timeout
	select {
	case <-ctx.Done():
		err = newTaskError("HandleWithTimeout", task.GetID(), pn, server, ErrTaskTimeout)
		tm.logger.Error().Err(err).Msgf("[%s|%s] Task FAILED-TIMEOUT on server %s, took %s", pn, task.GetID(), server, time.Since(startTime))
	case err = <-done:
		if err == nil {
			tm.logger.Debug().Msgf("[tms|%s|%s] Task COMPLETED on server %s, took %s", pn, task.GetID(), server, time.Since(startTime))
		} else {
			// Wrap error if it's not already a TaskError
			if _, ok := err.(*TaskError); !ok {
				wrappedErr := newTaskError("HandleTask", task.GetID(), pn, server, err)
				tm.logger.Error().Err(wrappedErr).Msgf("[tms|%s|%s] Task FAILED on server %s, took %s", pn, task.GetID(), server, time.Since(startTime))
				err = wrappedErr
			} else {
				tm.logger.Error().Err(err).Msgf("[tms|%s|%s] Task FAILED on server %s, took %s", pn, task.GetID(), server, time.Since(startTime))
			}
		}
	}

	return err, time.Since(startTime).Milliseconds()
}

var (
	TaskQueueManagerInstance *TaskManagerSimple
	taskManagerMutex         sync.Mutex
	taskManagerCond          = sync.NewCond(&taskManagerMutex)
	addMaxRetries            = 3
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
		
		// Check if task manager is running
		taskManagerMutex.Lock()
		for TaskQueueManagerInstance == nil || !TaskQueueManagerInstance.IsRunning() {
			taskManagerCond.Wait()
		}
		
		// Get a local reference to avoid race conditions
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