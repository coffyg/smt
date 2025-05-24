package smt

import (
	"sync"
	"sync/atomic"
)

// ExtendedMockTask extends MockTask to track execution status
type ExtendedMockTask struct {
	MockTask
	statusMutex      sync.Mutex
	succeeded        bool
	failed           bool
	completed        bool
	taskCompletionCh chan<- string
	tasksSucceeded   *int32
	tasksFailed      *int32
	tasksCompleted   *int32
}

func (t *ExtendedMockTask) MarkAsSuccess(time int64) {
	t.statusMutex.Lock()
	t.succeeded = true
	t.statusMutex.Unlock()
	atomic.AddInt32(t.tasksSucceeded, 1)
	t.MockTask.MarkAsSuccess(time)
}

func (t *ExtendedMockTask) MarkAsFailed(time int64, err error) {
	t.statusMutex.Lock()
	t.failed = true
	t.statusMutex.Unlock()
	atomic.AddInt32(t.tasksFailed, 1)
	t.MockTask.MarkAsFailed(time, err)
}

func (t *ExtendedMockTask) OnComplete() {
	t.statusMutex.Lock()
	t.completed = true
	t.statusMutex.Unlock()
	atomic.AddInt32(t.tasksCompleted, 1)

	// only signal the channel if it was set
	if t.taskCompletionCh != nil {
		t.taskCompletionCh <- t.id
	}

	t.MockTask.OnComplete()
}

func (t *ExtendedMockTask) GetStatus() (bool, bool, bool) {
	t.statusMutex.Lock()
	defer t.statusMutex.Unlock()
	return t.succeeded, t.failed, t.completed
}
