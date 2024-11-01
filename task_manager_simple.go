package smt

import (
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// TaskManagerSimple interface (optimized)
type TaskManagerSimple struct {
	providers       map[string]*ProviderData
	taskInQueue     sync.Map
	isRunning       int32
	shutdownRequest int32
	shutdownCh      chan struct{}
	wg              sync.WaitGroup
	logger          *zerolog.Logger
	getTimeout      func(string) time.Duration
}

type ProviderData struct {
	taskQueue        TaskQueuePrio
	taskQueueLock    sync.Mutex
	taskQueueCond    *sync.Cond
	servers          []string
	availableServers chan string
	commandQueue     []Command
}

func NewTaskManagerSimple(providers *[]IProvider, servers map[string][]string, logger *zerolog.Logger, getTimeout func(string) time.Duration) *TaskManagerSimple {
	tm := &TaskManagerSimple{
		providers:       make(map[string]*ProviderData),
		isRunning:       0,
		shutdownRequest: 0,
		shutdownCh:      make(chan struct{}),
		logger:          logger,
		getTimeout:      getTimeout,
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
			commandQueue:     []Command{},
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
	if !tm.IsRunning() {
		return false
	}

	if tm.isTaskInQueue(task) {
		return false
	}

	provider := task.GetProvider()
	if provider == nil {
		err := fmt.Errorf("task '%s' has no provider", task.GetID())
		tm.logger.Error().Err(err).Msg("[tms|nil_provider|error] error")
		task.MarkAsFailed(0, err)
		task.OnComplete()
		// Do not set the task as in queue
		return false
	}

	providerName := provider.Name()
	pd, ok := tm.providers[providerName]
	if !ok {
		err := fmt.Errorf("provider '%s' not found", providerName)
		tm.logger.Error().Err(err).Msgf("[tms|%s|%s] error", providerName, task.GetID())
		task.MarkAsFailed(0, err)
		task.OnComplete()
		return false
	}

	// Now that the task is confirmed valid and the provider exists, set it as in queue
	tm.setTaskInQueue(task)

	pd.taskQueueLock.Lock()
	defer pd.taskQueueLock.Unlock()
	heap.Push(&pd.taskQueue, &TaskWithPriority{
		task:     task,
		priority: task.GetPriority(),
	})
	pd.taskQueueCond.Signal() // Signal that a new task is available

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

	for {
		pd.taskQueueLock.Lock()
		for len(pd.commandQueue) == 0 && pd.taskQueue.Len() == 0 && !tm.HasShutdownRequest() {
			pd.taskQueueCond.Wait()
		}
		if tm.HasShutdownRequest() {
			pd.taskQueueLock.Unlock()
			return
		}

		var isCommand bool
		var command Command
		var taskWithPriority *TaskWithPriority

		if len(pd.commandQueue) > 0 {
			// Dequeue the command
			command = pd.commandQueue[0]
			pd.commandQueue = pd.commandQueue[1:]
			isCommand = true
		} else if pd.taskQueue.Len() > 0 {
			// Get the next task
			taskWithPriority = heap.Pop(&pd.taskQueue).(*TaskWithPriority)
			isCommand = false
		}
		pd.taskQueueLock.Unlock()

		// Wait for an available server
		select {
		case <-tm.shutdownCh:
			return
		case server := <-pd.availableServers:
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

	defer tm.wg.Done()
	defer func() {
		// Return the server to the available servers pool
		pd, ok := tm.providers[providerName]
		if ok {
			pd.availableServers <- server
		}
	}()
	// Handle panics
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic occurred: %v\n%s", r, string(debug.Stack()))
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic", providerName, task.GetID(), server)
			task.MarkAsFailed(time.Since(started).Milliseconds(), err)
			if !onCompleteCalled {
				task.OnComplete()
				onCompleteCalled = true
			}
			tm.delTaskInQueue(task)
		}
	}()

	// Ensure the task has a valid provider
	if task.GetProvider() == nil {
		err := fmt.Errorf("task '%s' has no provider", task.GetID())
		tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] Task has no provider", providerName, task.GetID(), server)
		task.MarkAsFailed(time.Since(started).Milliseconds(), err)
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
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] max retries reached", providerName, task.GetID(), server)
			task.MarkAsFailed(totalTime, err)
			if !onCompleteCalled {
				task.OnComplete()
				onCompleteCalled = true
			}
			tm.delTaskInQueue(task)
		} else {
			tm.logger.Debug().Err(err).Msgf("[tms|%s|%s|%s] retrying (%d/%d)", providerName, task.GetID(), server, retries+1, maxRetries)
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
		tm.logger.Debug().Msg("[tms] Task manager shutdown [ALREADY STOPPED]")
		return
	}
	atomic.StoreInt32(&tm.shutdownRequest, 1)
	close(tm.shutdownCh)

	// Signal all provider dispatchers to wake up
	for _, pd := range tm.providers {
		pd.taskQueueLock.Lock()
		pd.taskQueueCond.Broadcast()
		pd.taskQueueLock.Unlock()
	}

	tm.wg.Wait()
	atomic.StoreInt32(&tm.isRunning, 0)
	tm.logger.Debug().Msg("[tms] Task manager shutdown [FINISHED]")
}

func (tm *TaskManagerSimple) HandleWithTimeout(pn string, task ITask, server string, handler func(ITask, string) error) (error, int64) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic occurred: %v", r)
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic in task", pn, task.GetID(), server)
		}
	}()

	maxTimeout := tm.getTimeout(pn)
	ctx, cancel := context.WithTimeout(context.Background(), maxTimeout)
	defer cancel()

	done := make(chan error, 1)
	startTime := time.Now()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("panic occurred in handler: %v\n%s", r, string(debug.Stack()))
				tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic in handler", pn, task.GetID(), server)
				done <- err
			}
		}()

		tm.logger.Debug().Msgf("[tms|%s|%s] Task STARTED on server %s", pn, task.GetID(), server)
		done <- handler(task, server)
	}()

	select {
	case <-ctx.Done():
		err = fmt.Errorf("[tms|%s|%s] Task timed out on server %s", pn, task.GetID(), server)
		tm.logger.Error().Err(err).Msgf("[%s|%s] Task FAILED-TIMEOUT on server %s, took %s", pn, task.GetID(), server, time.Since(startTime))
	case err = <-done:
		if err == nil {
			tm.logger.Debug().Msgf("[tms|%s|%s] Task COMPLETED on server %s, took %s", pn, task.GetID(), server, time.Since(startTime))
		} else {
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s] Task FAILED on server %s, took %s", pn, task.GetID(), server, time.Since(startTime))
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
func InitTaskQueueManager(logger *zerolog.Logger, providers *[]IProvider, tasks []ITask, servers map[string][]string, getTimeout func(string) time.Duration) {
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
			logger.Error().Msg("[tms|add-task] Task not added, max retries reached")
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
