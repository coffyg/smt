package smt

import (
	"bytes"
	"sync"
)

// Thread-safe logger for race detection tests
type ThreadSafeLogBuffer struct {
	buf   bytes.Buffer
	mutex sync.Mutex
}

func (t *ThreadSafeLogBuffer) Write(p []byte) (n int, err error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.buf.Write(p)
}

func (t *ThreadSafeLogBuffer) String() string {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.buf.String()
}