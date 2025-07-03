package smt

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// CommandTask wraps a command function to implement ITask interface
type CommandTask struct {
	id           string
	commandFunc  func(server string) error
	provider     IProvider
	priority     int
	createdAt    time.Time
	retries      int
	maxRetries   int
	lastError    string
	completed    bool
	providerName string
	sequence     int64
}

var commandSequence int64 = 0

// NewCommandTask creates a new CommandTask
func NewCommandTask(providerName string, provider IProvider, commandFunc func(server string) error, priority int) *CommandTask {
	// Atomically increment sequence for each new command
	seq := atomic.AddInt64(&commandSequence, 1) - 1
	
	return &CommandTask{
		id:           uuid.New().String(),
		commandFunc:  commandFunc,
		provider:     provider,
		priority:     priority,
		createdAt:    time.Now(),
		retries:      0,
		maxRetries:   1, // Commands typically don't retry
		providerName: providerName,
		sequence:     seq,
	}
}

// MarkAsSuccess implements ITask
func (ct *CommandTask) MarkAsSuccess(t int64) {
	ct.completed = true
}

// MarkAsFailed implements ITask
func (ct *CommandTask) MarkAsFailed(t int64, err error) {
	ct.completed = true
	if err != nil {
		ct.lastError = err.Error()
	}
}

// GetPriority implements ITask
func (ct *CommandTask) GetPriority() int {
	// Commands have negative priority to execute after regular tasks
	// Use negative sequence to maintain FIFO order (earlier commands have less negative values)
	return -int(ct.sequence) - 1
}

// GetID implements ITask
func (ct *CommandTask) GetID() string {
	return ct.id
}

// GetMaxRetries implements ITask
func (ct *CommandTask) GetMaxRetries() int {
	return ct.maxRetries
}

// GetRetries implements ITask
func (ct *CommandTask) GetRetries() int {
	return ct.retries
}

// GetCreatedAt implements ITask
func (ct *CommandTask) GetCreatedAt() time.Time {
	return ct.createdAt
}

// GetTaskGroup implements ITask
func (ct *CommandTask) GetTaskGroup() ITaskGroup {
	return nil // Commands don't belong to task groups
}

// GetProvider implements ITask
func (ct *CommandTask) GetProvider() IProvider {
	return ct.provider
}

// UpdateRetries implements ITask
func (ct *CommandTask) UpdateRetries(retries int) error {
	ct.retries = retries
	return nil
}

// GetTimeout implements ITask
func (ct *CommandTask) GetTimeout() time.Duration {
	return 0 // Use default timeout
}

// UpdateLastError implements ITask
func (ct *CommandTask) UpdateLastError(error string) error {
	ct.lastError = error
	return nil
}

// GetCallbackName implements ITask
func (ct *CommandTask) GetCallbackName() string {
	return ""
}

// OnComplete implements ITask
func (ct *CommandTask) OnComplete() {
	// No-op for commands
}

// OnStart implements ITask
func (ct *CommandTask) OnStart() {
	// No-op for commands
}

// CommandProvider wraps command execution to implement IProvider interface
type CommandProvider struct {
	name        string
}

// NewCommandProvider creates a new CommandProvider
func NewCommandProvider(name string) *CommandProvider {
	return &CommandProvider{
		name:         name,
	}
}

// Handle implements IProvider for command execution
func (cp *CommandProvider) Handle(task ITask, server string) error {
	if ct, ok := task.(*CommandTask); ok {
		return ct.commandFunc(server)
	}
	return fmt.Errorf("invalid task type for CommandProvider")
}

// Name implements IProvider
func (cp *CommandProvider) Name() string {
	return cp.name
}