package smt

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
)

type Command struct {
	commandFunc func(server string) error
	id          uuid.UUID
}

func (tm *TaskManagerSimple) ExecuteCommand(providerName string, commandFunc func(server string) error) error {
	if !tm.IsRunning() {
		return errors.New("TaskManager is not running")
	}
	pd, ok := tm.providers[providerName]
	if !ok {
		return fmt.Errorf("provider '%s' not found", providerName)
	}

	// Generate a UUID for the command
	cmdID := uuid.New()

	// Check if the command is already being processed
	pd.commandSetLock.Lock()
	if _, exists := pd.commandSet[cmdID]; exists {
		pd.commandSetLock.Unlock()
		return fmt.Errorf("command with UUID '%s' is already being processed", cmdID)
	}
	pd.commandSet[cmdID] = struct{}{}
	pd.commandSetLock.Unlock()

	// Add the command to the queue
	pd.taskQueueLock.Lock()
	defer pd.taskQueueLock.Unlock()
	pd.commandQueue.Enqueue(Command{
		id:          cmdID,
		commandFunc: commandFunc,
	})
	pd.taskQueueCond.Signal()
	return nil
}

func (tm *TaskManagerSimple) processCommand(command Command, providerName, server string) {
	defer tm.wg.Done()
	defer func() {
		// Return the server to the available servers pool
		pd, ok := tm.providers[providerName]
		if ok {
			pd.availableServers <- server
			// Remove the command from the tracking set
			pd.commandSetLock.Lock()
			delete(pd.commandSet, command.id)
			pd.commandSetLock.Unlock()
		}
	}()
	// Handle panics
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic occurred: %v\n%s", r, string(debug.Stack()))
			tm.logger.Error().Err(err).Msgf("[tms|%s|command|%s] panic", providerName, server)
		}
	}()
	// Handle command with timeout
	err, totalTime := tm.HandleCommandWithTimeout(providerName, command, server)
	if err != nil {
		tm.logger.Error().Err(err).Msgf("[tms|%s|command|%s] error", providerName, server)
	} else {
		tm.logger.Debug().Msgf("[tms|%s|command|%s] Command COMPLETED, took %d ms", providerName, server, totalTime)
	}
}

func (tm *TaskManagerSimple) HandleCommandWithTimeout(providerName string, command Command, server string) (error, int64) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic occurred: %v", r)
			tm.logger.Error().Err(err).Msgf("[tms|%s|command|%s] panic in command", providerName, server)
		}
	}()

	maxTimeout := tm.getTimeout(providerName)
	ctx, cancel := context.WithTimeout(context.Background(), maxTimeout)
	defer cancel()

	done := make(chan error, 1)
	startTime := time.Now()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("panic occurred in command: %v\n%s", r, string(debug.Stack()))
				tm.logger.Error().Err(err).Msgf("[tms|%s|command|%s] panic in command function", providerName, server)
				done <- err
			}
		}()

		tm.logger.Debug().Msgf("[tms|%s|command|%s] Command STARTED on server %s", providerName, command.id, server)
		done <- command.commandFunc(server)
	}()

	select {
	case <-ctx.Done():
		err = fmt.Errorf("[tms|%s|command|%s] Command timed out on server %s", providerName, command.id, server)
		tm.logger.Error().Err(err).Msgf("[%s|command|%s] Command FAILED-TIMEOUT on server %s, took %s", providerName, command.id, server, time.Since(startTime))
	case err = <-done:
		if err == nil {
			tm.logger.Debug().Msgf("[tms|%s|command|%s] Command COMPLETED on server %s, took %s", providerName, command.id, server, time.Since(startTime))
		} else {
			tm.logger.Error().Err(err).Msgf("[tms|%s|command|%s] Command FAILED on server %s, took %s", providerName, command.id, server, time.Since(startTime))
		}
	}

	return err, time.Since(startTime).Milliseconds()
}
