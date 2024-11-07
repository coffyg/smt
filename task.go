package smt

import (
	"time"
)

// Task interfaces (task.go)
type ITask interface {
	MarkAsSuccess(t int64)
	MarkAsFailed(t int64, err error)
	GetPriority() int
	GetID() string
	GetMaxRetries() int
	GetRetries() int
	GetCreatedAt() time.Time
	GetTaskGroup() ITaskGroup
	GetProvider() IProvider
	UpdateRetries(int) error
	GetTimeout() time.Duration
	UpdateLastError(string) error
	GetCallbackName() string
	OnComplete()
	OnStart()
}

type ITaskGroup interface {
	MarkComplete() error
	GetTaskCount() int
	GetTaskCompletedCount() int
	UpdateTaskCompletedCount(int) error
}

type IProvider interface {
	Handle(task ITask, server string) error
	Name() string
}
