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

	// Removed background task compaction code based on benchmark results

	// Adaptive timeout settings
	adaptiveTimeout     *AdaptiveTimeoutManager            // Manages adaptive timeouts
	originalTimeoutFunc func(string, string) time.Duration // Original timeout function
	adaptiveTimeoutStop chan struct{}                      // Channel to signal adaptive timeout worker to stop

	// Two-level dispatching settings
	twoLevelDispatch *TwoLevelDispatchManager // Manages two-level dispatching
	enableTwoLevel   bool                     // Whether two-level dispatching is enabled
	
	// Memory pooling settings
	taskPool       *TaskWithPriorityPool     // Memory pool for TaskWithPriority objects
	enablePooling  bool                      // Whether memory pooling is enabled
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

// Basic constructor - default options with optimal performance configuration
func NewTaskManagerSimple(providers *[]IProvider, servers map[string][]string, logger *zerolog.Logger, getTimeout func(string, string) time.Duration) *TaskManagerSimple {
	return NewTaskManagerWithOptions(providers, servers, logger, getTimeout, &TaskManagerOptions{
		// Memory pooling settings - significantly reduces GC pressure (enabled by benchmarks)
		EnablePooling: true, 
		PoolConfig: &PoolConfig{
			PreWarmSize:  2000, // Pre-allocate objects for better startup performance
			TrackStats:   false, // Disable stats tracking in production for max performance
			PreWarmAsync: true,  // Pre-warm in background to avoid startup delay
		},

		// Adaptive timeout settings - enabled based on benchmarks for variable workloads
		// BenchmarkAdaptiveTimeoutTimeoutScenarios/WithAdaptiveTimeout_FrequentTimeouts shows ~14% improvement
		EnableAdaptiveTimeout: true,

		// Two-level dispatching settings - enabled based on benchmarks for better scalability
		EnableTwoLevel: true,
	})
}

// TaskManagerOptions provides configuration options for TaskManagerSimple
type TaskManagerOptions struct {
	// Memory pooling settings
	EnablePooling   bool       // Whether to enable memory pooling for task wrappers
	PoolConfig      *PoolConfig // Configuration options for the memory pool

	// Adaptive timeout settings
	EnableAdaptiveTimeout bool // Whether to enable adaptive timeouts

	// Two-level dispatching settings
	EnableTwoLevel bool // Whether to enable two-level dispatching
}

// NewTaskManagerWithPool creates a task manager with optimized memory pooling for task wrappers
// It's recommended to use NewTaskManagerSimple instead for optimal performance configuration.
func NewTaskManagerWithPool(providers *[]IProvider, servers map[string][]string, logger *zerolog.Logger, getTimeout func(string, string) time.Duration) *TaskManagerSimple {
	return NewTaskManagerWithOptions(providers, servers, logger, getTimeout, &TaskManagerOptions{
		EnablePooling: true,
		PoolConfig: &PoolConfig{
			PreWarmSize:  2000,
			TrackStats:   false,
			PreWarmAsync: true,
		},
		// Enable proven optimizations based on benchmarks
		EnableTwoLevel: true,
		EnableAdaptiveTimeout: true,
	})
}

// DEPRECATED: Use NewTaskManagerSimple instead. Benchmarks showed compaction adds overhead.
// This constructor is kept for backward compatibility only.
func NewTaskManagerWithCompaction(providers *[]IProvider, servers map[string][]string, logger *zerolog.Logger, getTimeout func(string, string) time.Duration) *TaskManagerSimple {
	logger.Warn().Msg("[tms] NewTaskManagerWithCompaction is deprecated, use NewTaskManagerSimple instead")
	return NewTaskManagerSimple(providers, servers, logger, getTimeout)
}

// NewTaskManagerWithAdaptiveTimeout creates a task manager with adaptive timeout support
// It's recommended to use NewTaskManagerSimple instead for optimal performance configuration.
func NewTaskManagerWithAdaptiveTimeout(providers *[]IProvider, servers map[string][]string, logger *zerolog.Logger, getTimeout func(string, string) time.Duration) *TaskManagerSimple {
	return NewTaskManagerWithOptions(providers, servers, logger, getTimeout, &TaskManagerOptions{
		EnablePooling: true,
		PoolConfig: &PoolConfig{
			PreWarmSize:  2000,
			TrackStats:   false,
			PreWarmAsync: true,
		},
		EnableTwoLevel: true,
		EnableAdaptiveTimeout: true,
	})
}

// NewTaskManagerWithOptions creates a new task manager with the specified options
func NewTaskManagerWithOptions(providers *[]IProvider, servers map[string][]string, logger *zerolog.Logger, getTimeout func(string, string) time.Duration, options *TaskManagerOptions) *TaskManagerSimple {
	// Set default options if not provided
	if options == nil {
		options = &TaskManagerOptions{}
	}

	// Removed batch and compaction settings configuration

	// Create adaptive timeout manager if enabled
	adaptiveTimeoutMgr := NewAdaptiveTimeoutManager(options.EnableAdaptiveTimeout)

	// Create a wrapper for the timeout function to use adaptive timeouts
	var timeoutFunc func(string, string) time.Duration
	if options.EnableAdaptiveTimeout {
		// Store the original function and create a new one that uses adaptive timeouts
		timeoutFunc = func(callback, provider string) time.Duration {
			// Get the base timeout from the original function
			baseTimeout := getTimeout(callback, provider)

			// Register this timeout with the adaptive manager
			adaptiveTimeoutMgr.RegisterBaseTimeout(provider, callback, baseTimeout)

			// Get the adjusted timeout
			return adaptiveTimeoutMgr.GetTimeout(callback, provider)
		}
	} else {
		// Use the original timeout function
		timeoutFunc = getTimeout
	}
	
	// Initialize the memory pool if enabled
	var taskPool *TaskWithPriorityPool
	if options.EnablePooling {
		taskPool = NewTaskWithPriorityPoolConfig(options.PoolConfig)
	}

	tm := &TaskManagerSimple{
		providers:            make(map[string]*ProviderData, len(*providers)),
		shutdownCh:           make(chan struct{}),
		logger:               logger,
		getTimeout:           timeoutFunc,
		originalTimeoutFunc:  getTimeout,
		serverConcurrencyMap: make(map[string]chan struct{}),
		serverConcurrencyMu:  sync.RWMutex{},
		adaptiveTimeout:      adaptiveTimeoutMgr,
		adaptiveTimeoutStop:  make(chan struct{}),
		enableTwoLevel:       options.EnableTwoLevel,
		taskPool:             taskPool,
		enablePooling:        options.EnablePooling,
	}

	// Initialize two-level dispatch manager if enabled
	if options.EnableTwoLevel {
		tm.twoLevelDispatch = NewTwoLevelDispatchManager(tm, true)
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
		err.Release()     // Return to pool
		return false
	}

	providerName := provider.Name()
	pd, ok := tm.providers[providerName]
	if !ok {
		err := newTaskError("AddTask", taskID, providerName, "", ErrProviderNotFound)
		tm.logger.Error().Err(err).Msgf("[tms|%s|%s] error", providerName, taskID)
		task.MarkAsFailed(0, err)
		task.OnComplete() // Must call this for initial failure
		err.Release()     // Return to pool
		return false
	}

	// Now that the task is confirmed valid and the provider exists, set it as in queue
	tm.setTaskInQueue(taskID)

	// Try to use two-level dispatch if enabled
	if tm.enableTwoLevel && tm.twoLevelDispatch != nil {
		// Attempt to get a server from the available servers
		var server string
		select {
		case server = <-pd.availableServers:
			// Got a server, use it
			if tm.twoLevelDispatch.EnqueueTask(task, server) {
				// Successfully enqueued in two-level dispatch
				return true
			}
			// Return the server if two-level dispatch failed
			pd.availableServers <- server
		default:
			// No servers available, use two-level dispatch's server selection
			if tm.twoLevelDispatch.EnqueueTaskWithServerSelection(task, func(servers []string) string {
				// Simple round-robin selection
				if len(servers) > 0 {
					return servers[0]
				}
				return ""
			}) {
				// Successfully enqueued in two-level dispatch
				return true
			}
		}
	}

	// Fallback to traditional queue if two-level dispatch failed or is disabled
	pd.taskQueueLock.Lock()
	
	// Use memory pool if enabled, otherwise create a new TaskWithPriority
	var taskPriority *TaskWithPriority
	if tm.enablePooling && tm.taskPool != nil {
		taskPriority = tm.taskPool.GetWithTask(task, task.GetPriority())
	} else {
		taskPriority = &TaskWithPriority{
			task:     task,
			priority: task.GetPriority(),
		}
	}
	
	heap.Push(&pd.taskQueue, taskPriority)
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

	// Removed background task compaction worker

	// Start background adaptive timeout worker if enabled
	if tm.adaptiveTimeout != nil && tm.adaptiveTimeout.IsEnabled() {
		tm.wg.Add(1)
		go tm.startAdaptiveTimeoutWorker()
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
				go tm.processTask(taskWithPriority.task, providerName, server, taskWithPriority)
			}
		}
	}
}

func (tm *TaskManagerSimple) processTask(task ITask, providerName, server string, taskWrapper *TaskWithPriority) {
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
		
		// Return the task wrapper to the pool if pooling is enabled
		if tm.enablePooling && tm.taskPool != nil && taskWrapper != nil {
			tm.taskPool.Put(taskWrapper)
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

// processTaskTwoLevel processes a task using the two-level dispatch mechanism
func (tm *TaskManagerSimple) processTaskTwoLevel(task ITask, providerName, server string, taskWrapper *TaskWithPriority) {
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
		defer func() {
			<-semaphore
		}()
	}
	
	// Return the task wrapper to the pool when done
	defer func() {
		if tm.enablePooling && tm.taskPool != nil && taskWrapper != nil {
			tm.taskPool.Put(taskWrapper)
		}
	}()

	// Handle panics
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic occurred: %v\n%s", r, string(debug.Stack()))
			tm.logger.Error().Err(err).Msgf("[tms-twolevel|%s|%s|%s] panic", providerName, taskID, server)
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
		tm.logger.Error().Err(err).Msgf("[tms-twolevel|%s|%s|%s] Task has no provider", providerName, taskID, server)
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
			tm.logger.Error().Err(err).Msgf("[tms-twolevel|%s|%s|%s] max retries reached", providerName, taskID, server)
			task.MarkAsFailed(totalTime, err)
			if !onCompleteCalled {
				task.OnComplete()
				onCompleteCalled = true
			}
			tm.delTaskInQueue(taskID)
		} else {
			// For retry, don't call OnComplete as the task will be requeued
			// Just update retries and remove from current queue
			tm.logger.Debug().Err(err).Msgf("[tms-twolevel|%s|%s|%s] retrying (%d/%d)", providerName, taskID, server, retries+1, maxRetries)
			task.UpdateRetries(retries + 1)

			// Remove from queue to avoid duplicates
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

	// Removed compaction worker stop code

	// Signal adaptive timeout worker to stop if enabled
	if tm.adaptiveTimeout != nil && tm.adaptiveTimeout.IsEnabled() {
		close(tm.adaptiveTimeoutStop)
	}

	// Shutdown two-level dispatch if enabled
	if tm.enableTwoLevel && tm.twoLevelDispatch != nil {
		tm.twoLevelDispatch.Shutdown()
	}

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
	var execTimeMs int64
	select {
	case <-ctx.Done():
		err = newTaskError("HandleWithTimeout", task.GetID(), pn, server, ErrTaskTimeout)
		execTimeMs = time.Since(startTime).Milliseconds()
		tm.logger.Error().Err(err).Msgf("[%s|%s] Task FAILED-TIMEOUT on server %s, took %s", pn, task.GetID(), server, time.Since(startTime))
	case err = <-done:
		execTimeMs = time.Since(startTime).Milliseconds()
		if err == nil {
			tm.logger.Debug().Msgf("[tms|%s|%s] Task COMPLETED on server %s, took %s", pn, task.GetID(), server, time.Since(startTime))

			// Record successful execution in adaptive timeout manager
			if tm.adaptiveTimeout != nil && tm.adaptiveTimeout.IsEnabled() {
				tm.adaptiveTimeout.RecordExecution(pn, task.GetCallbackName(), execTimeMs, maxTimeout)
			}
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

	return err, execTimeMs
}

// Background task compaction worker and related functions have been removed
// based on benchmark results showing minimal performance benefit

// startAdaptiveTimeoutWorker starts a background worker that periodically decays old timeout statistics
func (tm *TaskManagerSimple) startAdaptiveTimeoutWorker() {
	defer tm.wg.Done()

	// Use ticker to schedule decay at regular intervals (daily)
	decayInterval := 24 * time.Hour
	ticker := time.NewTicker(decayInterval)
	defer ticker.Stop()

	tm.logger.Info().
		Msg("[tms|adaptive-timeout] Adaptive timeout worker started")

	for {
		select {
		case <-ticker.C:
			// Decay old statistics to give more weight to recent task executions
			if tm.adaptiveTimeout != nil {
				tm.adaptiveTimeout.DecayOldStats()
				tm.logger.Debug().Msg("[tms|adaptive-timeout] Decayed old execution statistics")
			}
		case <-tm.adaptiveTimeoutStop:
			tm.logger.Debug().Msg("[tms|adaptive-timeout] Adaptive timeout worker stopping")
			return
		case <-tm.shutdownCh:
			tm.logger.Debug().Msg("[tms|adaptive-timeout] Adaptive timeout worker stopping due to shutdown")
			return
		}
	}
}

// Task queue compaction functions have been removed based on benchmark results

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

	// Create a new task manager with benchmarked optimal configuration
	TaskQueueManagerInstance = NewTaskManagerSimple(providers, servers, logger, getTimeout)
	TaskQueueManagerInstance.Start()
	logger.Info().Msg("[tms] Task manager started with optimal configuration (memory pooling, adaptive timeouts, and two-level dispatching)")

	// Signal that the TaskManager is ready
	taskManagerCond.Broadcast()

	// Add uncompleted tasks
	RequeueTaskIfNeeded(logger, tasks)
}
func InitTaskQueueManagerFromTM(tm *TaskManagerSimple, logger *zerolog.Logger, tasks []ITask) {
	taskManagerMutex.Lock()
	defer taskManagerMutex.Unlock()
	logger.Info().Msg("[tms] Task manager initialization from existing instance")

	// Check if the provided task manager is nil
	if TaskQueueManagerInstance != nil {
		logger.Error().Msg("[tms] Task manager already initialized")
		return
	}
	if tm == nil {
		logger.Error().Msg("[tms] Task manager instance is nil")
		return
	}

	// Use the provided task manager instance
	TaskQueueManagerInstance = tm

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
