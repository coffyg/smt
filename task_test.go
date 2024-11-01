package smt

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Mock implementations for testing interfaces

type MockTask struct {
	id             string
	priority       int
	retries        int
	maxRetries     int
	createdAt      time.Time
	taskGroup      ITaskGroup
	provider       IProvider
	timeout        time.Duration
	lastError      string
	success        bool
	failed         bool
	startCalled    bool
	completeCalled bool
	successTime    int64
	failedTime     int64
	done           chan struct{}
	doneOnce       sync.Once
}

func (t *MockTask) MarkAsSuccess(time int64) {
	t.success = true
	t.successTime = time
}

func (t *MockTask) MarkAsFailed(time int64, err error) {
	t.failed = true
	t.failedTime = time
	t.lastError = err.Error()
}

func (t *MockTask) GetPriority() int {
	return t.priority
}

func (t *MockTask) GetID() string {
	return t.id
}

func (t *MockTask) GetMaxRetries() int {
	return t.maxRetries
}

func (t *MockTask) GetRetries() int {
	return t.retries
}

func (t *MockTask) GetCreatedAt() time.Time {
	return t.createdAt
}

func (t *MockTask) GetTaskGroup() ITaskGroup {
	return t.taskGroup
}

func (t *MockTask) GetProvider() IProvider {
	return t.provider
}

func (t *MockTask) UpdateRetries(retries int) error {
	t.retries = retries
	return nil
}

func (t *MockTask) GetTimeout() time.Duration {
	return t.timeout
}

func (t *MockTask) UpdateLastError(errStr string) error {
	t.lastError = errStr
	return nil
}

func (t *MockTask) OnComplete() {
	t.completeCalled = true
	if t.done != nil {
		t.doneOnce.Do(func() {
			close(t.done)
		})
	}
}

func (t *MockTask) OnStart() {
	t.startCalled = true
}

type MockTaskGroup struct {
	taskCount          int
	taskCompletedCount int
}

func (tg *MockTaskGroup) MarkComplete() error {
	return nil
}

func (tg *MockTaskGroup) GetTaskCount() int {
	return tg.taskCount
}

func (tg *MockTaskGroup) GetTaskCompletedCount() int {
	return tg.taskCompletedCount
}

func (tg *MockTaskGroup) UpdateTaskCompletedCount(count int) error {
	tg.taskCompletedCount = count
	return nil
}

type MockProvider struct {
	name       string
	handleFunc func(task ITask, server string) error
}

func (p *MockProvider) Handle(task ITask, server string) error {
	if p.handleFunc != nil {
		return p.handleFunc(task, server)
	}
	return nil
}

func (p *MockProvider) Name() string {
	return p.name
}

// Tests for MockTask
func TestMockTask(t *testing.T) {
	taskGroup := &MockTaskGroup{}
	provider := &MockProvider{name: "mockProvider"}
	task := &MockTask{
		id:         "task1",
		priority:   1,
		maxRetries: 3,
		createdAt:  time.Now(),
		taskGroup:  taskGroup,
		provider:   provider,
		timeout:    time.Second * 10,
	}

	if task.GetID() != "task1" {
		t.Errorf("Expected task ID 'task1', got '%s'", task.GetID())
	}
	if task.GetPriority() != 1 {
		t.Errorf("Expected priority 1, got %d", task.GetPriority())
	}
	if task.GetMaxRetries() != 3 {
		t.Errorf("Expected max retries 3, got %d", task.GetMaxRetries())
	}
	if task.GetRetries() != 0 {
		t.Errorf("Expected retries 0, got %d", task.GetRetries())
	}
	if task.GetProvider() != provider {
		t.Error("Expected provider to be the mock provider")
	}

	task.UpdateRetries(1)
	if task.GetRetries() != 1 {
		t.Errorf("Expected retries 1, got %d", task.GetRetries())
	}

	task.MarkAsSuccess(0)
	if !task.success {
		t.Error("Expected task to be marked as success")
	}

	task.MarkAsFailed(0, errors.New("some error"))
	if !task.failed {
		t.Error("Expected task to be marked as failed")
	}
	if task.lastError != "some error" {
		t.Errorf("Expected last error 'some error', got '%s'", task.lastError)
	}

	task.OnStart()
	if !task.startCalled {
		t.Error("Expected OnStart to be called")
	}

	task.OnComplete()
	if !task.completeCalled {
		t.Error("Expected OnComplete to be called")
	}
}

// Tests for MockTaskGroup
func TestMockTaskGroup(t *testing.T) {
	tg := &MockTaskGroup{taskCount: 10}

	if tg.GetTaskCount() != 10 {
		t.Errorf("Expected task count 10, got %d", tg.GetTaskCount())
	}
	if tg.GetTaskCompletedCount() != 0 {
		t.Errorf("Expected completed task count 0, got %d", tg.GetTaskCompletedCount())
	}

	tg.UpdateTaskCompletedCount(5)
	if tg.GetTaskCompletedCount() != 5 {
		t.Errorf("Expected completed task count 5, got %d", tg.GetTaskCompletedCount())
	}

	if err := tg.MarkComplete(); err != nil {
		t.Errorf("Expected MarkComplete to not return error, got %v", err)
	}
}

// Tests for MockProvider
func TestMockProvider(t *testing.T) {
	provider := &MockProvider{name: "testProvider"}

	if provider.Name() != "testProvider" {
		t.Errorf("Expected provider name 'testProvider', got '%s'", provider.Name())
	}

	task := &MockTask{id: "task1"}
	server := "testServer"

	called := false
	provider.handleFunc = func(task ITask, server string) error {
		called = true
		return nil
	}

	if err := provider.Handle(task, server); err != nil {
		t.Errorf("Expected Handle to not return error, got %v", err)
	}
	if !called {
		t.Error("Expected provider Handle function to be called")
	}
}
func TestTaskManagerSimple_ExecuteCommand(t *testing.T) {
	// Setup TaskManagerSimple
	providerName := "testProvider"
	provider := &MockProvider{name: providerName}

	servers := map[string][]string{
		providerName: {"server1"},
	}

	logger := zerolog.New(nil)
	getTimeout := func(string) time.Duration {
		return time.Second * 5
	}

	tm := NewTaskManagerSimple(&[]IProvider{provider}, servers, &logger, getTimeout)
	tm.Start()
	defer tm.Shutdown()

	// Create a normal task with low priority
	task := &MockTask{
		id:         "task1",
		priority:   1,
		maxRetries: 3,
		provider:   provider,
		done:       make(chan struct{}),
	}

	executionOrder := []string{}
	var executionOrderLock sync.Mutex

	provider.handleFunc = func(task ITask, server string) error {
		executionOrderLock.Lock()
		executionOrder = append(executionOrder, "task")
		executionOrderLock.Unlock()
		if mt, ok := task.(*MockTask); ok {
			mt.startCalled = true
			if mt.done != nil {
				close(mt.done)
			}
		}
		return nil
	}

	tm.AddTask(task)

	// Create a command function
	commandExecuted := false
	commandDone := make(chan struct{})
	commandFunc := func(server string) error {
		executionOrderLock.Lock()
		executionOrder = append(executionOrder, "command")
		executionOrderLock.Unlock()
		commandExecuted = true
		close(commandDone)
		return nil
	}

	// Execute the command with higher priority
	err := tm.ExecuteCommand(providerName, commandFunc)
	if err != nil {
		t.Errorf("ExecuteCommand returned error: %v", err)
	}

	// Wait for command and task to complete
	select {
	case <-time.After(time.Second * 1):
		t.Error("Timeout waiting for command and task to complete")
	case <-commandDone:
		// Wait for task
		select {
		case <-task.done:
			// Both command and task completed
		case <-time.After(time.Second * 1):
			t.Error("Timeout waiting for task to complete")
		}
	}

	// Check that the command was executed before the task
	if !commandExecuted {
		t.Error("Command was not executed")
	}
	if !task.startCalled {
		t.Error("Task was not executed after the command")
	}

	executionOrderLock.Lock()
	if len(executionOrder) != 2 || executionOrder[0] != "command" || executionOrder[1] != "task" {
		t.Errorf("Execution order incorrect, expected ['command', 'task'], got %v", executionOrder)
	}
	executionOrderLock.Unlock()
}
