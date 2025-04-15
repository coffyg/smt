package smt

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// Pre-allocated common errors
	ErrTaskManagerNotRunning = errors.New("TaskManager is not running")
	ErrTaskHasNoProvider     = errors.New("task has no provider")
	ErrProviderNotFound      = errors.New("provider not found")
	ErrTaskTimeout           = errors.New("task timed out")
	ErrCommandTimeout        = errors.New("command timed out")
)

// TaskError represents an error that occurred during task processing
type TaskError struct {
	Op         string // Operation that failed
	TaskID     string // ID of the task that failed
	ProviderID string // ID of the provider
	ServerID   string // ID of the server
	Err        error  // Underlying error
}

// Small pool of error buffers to reduce allocations - disabled in race detection mode
var taskErrorPool = &sync.Pool{
	New: func() interface{} {
		return &TaskError{}
	},
}

// newTaskError creates a new TaskError
func newTaskError(op, taskID, providerID, serverID string, err error) *TaskError {
	// Don't use pool for now to avoid potential race issues
	return &TaskError{
		Op:         op,
		TaskID:     taskID,
		ProviderID: providerID,
		ServerID:   serverID,
		Err:        err,
	}
}

// Release returns the error to the pool (disabled for now)
func (e *TaskError) Release() {
	// No-op as we aren't using the pool to avoid race conditions
}

// Error returns the error message
func (e *TaskError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("smt.%s: operation failed for task '%s'", e.Op, e.TaskID)
	}
	return fmt.Sprintf("smt.%s: %v [task=%s provider=%s server=%s]", 
		e.Op, e.Err, e.TaskID, e.ProviderID, e.ServerID)
}

// Unwrap returns the underlying error
func (e *TaskError) Unwrap() error {
	return e.Err
}