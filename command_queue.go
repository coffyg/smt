package smt

import (
	"sync"
	"sync/atomic"
)

// ringBuffer is a fixed-size ring buffer that avoids slice resizing
type ringBuffer struct {
	buffer    []Command
	size      int
	head      int
	tail      int
	count     int32 // Using atomic operations for faster concurrent access
	capacity  int
}

// CommandQueue is a thread-safe FIFO queue for Commands using a ring buffer
type CommandQueue struct {
	rb       *ringBuffer
	lock     sync.Mutex
	// Cache padding to prevent false sharing
	_        [64]byte
}

// newRingBuffer creates a new ring buffer with the given capacity
func newRingBuffer(capacity int) *ringBuffer {
	// Default minimum capacity to prevent frequent resizing
	if capacity < 16 {
		capacity = 16
	}
	return &ringBuffer{
		buffer:   make([]Command, capacity),
		size:     0,
		head:     0,
		tail:     0,
		count:    0,
		capacity: capacity,
	}
}

// resize increases the capacity of the ring buffer
func (rb *ringBuffer) resize() {
	newCapacity := rb.capacity * 2
	newBuffer := make([]Command, newCapacity)
	
	// Copy existing items in order
	for i := 0; i < int(rb.count); i++ {
		newBuffer[i] = rb.buffer[(rb.head+i)%rb.capacity]
	}
	
	// Update buffer and indices
	rb.buffer = newBuffer
	rb.head = 0
	rb.tail = int(rb.count)
	rb.capacity = newCapacity
}

// NewCommandQueue creates a new command queue with an initial capacity
func NewCommandQueue(initialCapacity int) *CommandQueue {
	return &CommandQueue{
		rb: newRingBuffer(initialCapacity),
	}
}

// Enqueue adds a command to the queue
func (cq *CommandQueue) Enqueue(cmd Command) {
	cq.lock.Lock()
	defer cq.lock.Unlock()
	
	// Check if buffer is full and resize if needed
	if int(cq.rb.count) == cq.rb.capacity {
		cq.rb.resize()
	}
	
	// Add item to tail
	cq.rb.buffer[cq.rb.tail] = cmd
	cq.rb.tail = (cq.rb.tail + 1) % cq.rb.capacity
	atomic.AddInt32(&cq.rb.count, 1)
}

// Dequeue removes and returns the oldest command from the queue
func (cq *CommandQueue) Dequeue() (Command, bool) {
	cq.lock.Lock()
	defer cq.lock.Unlock()
	
	// Check if buffer is empty
	if atomic.LoadInt32(&cq.rb.count) == 0 {
		return Command{}, false
	}
	
	// Get item from head
	cmd := cq.rb.buffer[cq.rb.head]
	// Clear reference to help GC
	cq.rb.buffer[cq.rb.head] = Command{}
	cq.rb.head = (cq.rb.head + 1) % cq.rb.capacity
	atomic.AddInt32(&cq.rb.count, -1)
	
	return cmd, true
}

// Len returns the number of commands in the queue
func (cq *CommandQueue) Len() int {
	// Use atomic access for better concurrency
	return int(atomic.LoadInt32(&cq.rb.count))
}
