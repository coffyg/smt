package smt

import (
	"sync"
)

// CommandQueue is a thread-safe FIFO queue for Commands
type CommandQueue struct {
	commands []Command
	lock     sync.Mutex
}

func (cq *CommandQueue) Enqueue(cmd Command) {
	cq.lock.Lock()
	defer cq.lock.Unlock()
	cq.commands = append(cq.commands, cmd)
}

func (cq *CommandQueue) Dequeue() (Command, bool) {
	cq.lock.Lock()
	defer cq.lock.Unlock()
	if len(cq.commands) == 0 {
		return Command{}, false
	}
	cmd := cq.commands[0]
	cq.commands = cq.commands[1:]
	return cmd, true
}

func (cq *CommandQueue) Len() int {
	cq.lock.Lock()
	defer cq.lock.Unlock()
	return len(cq.commands)
}
