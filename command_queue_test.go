package smt

import (
	"testing"
	"errors"
	"github.com/google/uuid"
)

func TestCommandQueue(t *testing.T) {
	queue := NewCommandQueue(0)

	// Test initial state
	if queue.Len() != 0 {
		t.Errorf("Expected empty queue, got length %d", queue.Len())
	}

	// Test Dequeue on empty queue
	_, ok := queue.Dequeue()
	if ok {
		t.Error("Expected Dequeue on empty queue to return false")
	}

	// Test Enqueue and Dequeue
	cmd1 := Command{
		id: uuid.New(),
		commandFunc: func(server string) error {
			return nil
		},
	}
	queue.Enqueue(cmd1)
	
	if queue.Len() != 1 {
		t.Errorf("Expected queue length 1, got %d", queue.Len())
	}

	cmd2, ok := queue.Dequeue()
	if !ok {
		t.Error("Expected Dequeue to succeed")
	}
	if cmd2.id != cmd1.id {
		t.Errorf("Expected command id %s, got %s", cmd1.id, cmd2.id)
	}

	// Test queue is empty after dequeue
	if queue.Len() != 0 {
		t.Errorf("Expected empty queue after dequeue, got length %d", queue.Len())
	}
}

func TestCommandQueueMultiple(t *testing.T) {
	queue := NewCommandQueue(2)

	// Add several items
	for i := 0; i < 10; i++ {
		cmdID := uuid.New()
		cmd := Command{
			id: cmdID,
			commandFunc: func(server string) error {
				return nil
			},
		}
		queue.Enqueue(cmd)
	}

	// Check length
	if queue.Len() != 10 {
		t.Errorf("Expected queue length 10, got %d", queue.Len())
	}

	// Dequeue all items
	for i := 0; i < 10; i++ {
		_, ok := queue.Dequeue()
		if !ok {
			t.Errorf("Failed to dequeue item %d", i)
			break
		}
	}

	// Queue should be empty
	if queue.Len() != 0 {
		t.Errorf("Expected empty queue, got length %d", queue.Len())
	}
}

func TestCommandQueueResize(t *testing.T) {
	// Start with a small queue
	queue := NewCommandQueue(1)

	// Add more items than initial capacity
	for i := 0; i < 20; i++ {
		cmdID := uuid.New()
		var err error
		if i%2 == 0 {
			err = errors.New("test error")
		}
		
		cmd := Command{
			id: cmdID,
			commandFunc: func(server string) error {
				return err
			},
		}
		queue.Enqueue(cmd)
	}

	// Check that all items were added
	if queue.Len() != 20 {
		t.Errorf("Expected queue length 20, got %d", queue.Len())
	}

	// Dequeue and verify all items can be retrieved
	for i := 0; i < 20; i++ {
		_, ok := queue.Dequeue()
		if !ok {
			t.Errorf("Failed to dequeue item %d", i)
			break
		}
	}
}