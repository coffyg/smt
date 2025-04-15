package smt

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/google/uuid"
)

type Command struct {
	commandFunc func(server string) error
	id          uuid.UUID
}

// commandBufferPool is used for Stack functions in panic handlers
var commandStackBuffer = make([]byte, 4096)

func (tm *TaskManagerSimple) ExecuteCommand(providerName string, commandFunc func(server string) error) error {
	// Fast path: if not running, return early
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
	
	// Pre-check command existence without releasing lock
	if _, exists := pd.commandSet[cmdID]; exists {
		pd.commandSetLock.Unlock()
		return fmt.Errorf("command with UUID '%s' is already being processed", cmdID)
	}
	
	// Add to tracking set
	pd.commandSet[cmdID] = struct{}{}
	pd.commandSetLock.Unlock()

	// Add the command to the queue under the lock to ensure proper signal
	pd.taskQueueLock.Lock()
	pd.commandQueue.Enqueue(Command{
		id:          cmdID,
		commandFunc: commandFunc,
	})
	pd.taskQueueCond.Signal()
	pd.taskQueueLock.Unlock()
	
	return nil
}

func (tm *TaskManagerSimple) processCommand(command Command, providerName, server string) {
	defer tm.wg.Done()
	
	// Create local vars to avoid repeatedly dereferencing
	cmdID := command.id
	
	defer func() {
		// Return the server to the available servers pool
		pd, ok := tm.providers[providerName]
		if ok {
			pd.availableServers <- server
			// Remove the command from the tracking set
			pd.commandSetLock.Lock()
			delete(pd.commandSet, cmdID)
			pd.commandSetLock.Unlock()
		}
	}()
	
	// Handle panics
	defer func() {
		if r := recover(); r != nil {
			runtime.Stack(commandStackBuffer, false)
			err := fmt.Errorf("panic occurred: %v\n%s", r, commandStackBuffer)
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
			runtime.Stack(commandStackBuffer, false)
			err = fmt.Errorf("panic occurred: %v\n%s", r, commandStackBuffer)
			tm.logger.Error().Err(err).Msgf("[tms|%s|command|%s] panic in command", providerName, server)
		}
	}()

	// Use the provider's default timeout
	maxTimeout := tm.getTimeout("", providerName)
	ctx, cancel := context.WithTimeout(context.Background(), maxTimeout)
	defer cancel()

	// Use buffered channel to avoid goroutine leaks
	done := make(chan error, 1)
	startTime := time.Now()
	cmdID := command.id

	go func() {
		defer func() {
			if r := recover(); r != nil {
				runtime.Stack(commandStackBuffer, false)
				err := fmt.Errorf("panic occurred in command: %v\n%s", r, commandStackBuffer)
				tm.logger.Error().Err(err).Msgf("[tms|%s|command|%s] panic in command function", providerName, server)
				done <- err
			}
		}()

		tm.logger.Debug().Msgf("[tms|%s|command|%s] Command STARTED on server %s", providerName, cmdID, server)
		done <- command.commandFunc(server)
	}()

	select {
	case <-ctx.Done():
		err = newTaskError("HandleCommandWithTimeout", cmdID.String(), providerName, server, ErrCommandTimeout)
		tm.logger.Error().Err(err).Msgf("[%s|command|%s] Command FAILED-TIMEOUT on server %s, took %s", providerName, cmdID, server, time.Since(startTime))
	case err = <-done:
		if err == nil {
			tm.logger.Debug().Msgf("[tms|%s|command|%s] Command COMPLETED on server %s, took %s", providerName, cmdID, server, time.Since(startTime))
		} else {
			// Wrap error if it's not already a TaskError
			if _, ok := err.(*TaskError); !ok {
				wrappedErr := newTaskError("HandleCommand", cmdID.String(), providerName, server, err)
				tm.logger.Error().Err(wrappedErr).Msgf("[tms|%s|command|%s] Command FAILED on server %s, took %s", providerName, cmdID, server, time.Since(startTime))
				err = wrappedErr
			} else {
				tm.logger.Error().Err(err).Msgf("[tms|%s|command|%s] Command FAILED on server %s, took %s", providerName, cmdID, server, time.Since(startTime))
			}
		}
	}

	return err, time.Since(startTime).Milliseconds()
}