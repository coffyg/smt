package smt

import (
	"container/heap"
	"testing"
)

// Test the priority queue ordering
func TestTaskPriorityQueue(t *testing.T) {
	pq := &TaskQueuePrio{}
	heap.Init(pq)

	tasks := []*TaskWithPriority{
		{task: &MockTask{id: "task1"}, priority: 3},
		{task: &MockTask{id: "task2"}, priority: 1},
		{task: &MockTask{id: "task3"}, priority: 5},
		{task: &MockTask{id: "task4"}, priority: 2},
		{task: &MockTask{id: "task5"}, priority: 4},
	}
	
	// Initialize sort keys
	for i, task := range tasks {
		task.sortKey = ComputeSortKey(task.priority, i)
	}

	for _, task := range tasks {
		heap.Push(pq, task)
	}

	expectedOrder := []string{"task3", "task5", "task1", "task4", "task2"}

	for i, expectedID := range expectedOrder {
		item := heap.Pop(pq).(*TaskWithPriority)
		if item.task.GetID() != expectedID {
			t.Errorf("Expected task ID '%s' at position %d, got '%s'", expectedID, i, item.task.GetID())
		}
	}
}

// Test updating priority of a task in the queue
func TestTaskPriorityQueue_UpdatePriority(t *testing.T) {
	pq := &TaskQueuePrio{}
	heap.Init(pq)

	task1 := &TaskWithPriority{task: &MockTask{id: "task1"}, priority: 3}
	task1.sortKey = ComputeSortKey(task1.priority, 0)
	heap.Push(pq, task1)
	
	task2 := &TaskWithPriority{task: &MockTask{id: "task2"}, priority: 2}
	task2.sortKey = ComputeSortKey(task2.priority, 1)
	heap.Push(pq, task2)
	
	task3 := &TaskWithPriority{task: &MockTask{id: "task3"}, priority: 1} 
	task3.sortKey = ComputeSortKey(task3.priority, 2)
	heap.Push(pq, task3)

	// Increase priority of task3
	task3.priority = 5
	task3.sortKey = ComputeSortKey(task3.priority, task3.index)
	heap.Fix(pq, task3.index)

	item := heap.Pop(pq).(*TaskWithPriority)
	if item.task.GetID() != "task3" {
		t.Errorf("Expected task 'task3' to be popped first after priority update, got '%s'", item.task.GetID())
	}
}