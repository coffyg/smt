package smt

// Task priority queue (task_priority.go)
type TaskWithPriority struct {
	task     ITask
	priority int
	index    int // The index is needed by the heap.Interface methods.
}

type TaskQueuePrio []*TaskWithPriority

func (tq TaskQueuePrio) Len() int { return len(tq) }

func (tq TaskQueuePrio) Less(i, j int) bool {
	// Higher priority tasks come first
	return tq[i].priority > tq[j].priority
}

func (tq TaskQueuePrio) Swap(i, j int) {
	tq[i], tq[j] = tq[j], tq[i]
	tq[i].index = i
	tq[j].index = j
}

func (tq *TaskQueuePrio) Push(x interface{}) {
	n := len(*tq)
	item := x.(*TaskWithPriority)
	item.index = n
	*tq = append(*tq, item)
}

func (tq *TaskQueuePrio) Pop() interface{} {
	old := *tq
	n := len(old)
	item := old[n-1]
	item.index = -1 // For safety
	*tq = old[0 : n-1]
	return item
}
