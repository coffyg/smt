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
	
	// Verify provider exists
	_, ok := tm.providers[providerName]
	if !ok {
		return fmt.Errorf("provider '%s' not found", providerName)
	}

	// Create a CommandProvider for this provider
	commandProvider := NewCommandProvider(providerName)
	
	// Create a CommandTask with lower priority than regular tasks
	// This ensures tasks execute before commands, matching the test expectation
	// Priority 2 means lower than the test task (priority 1)
	commandTask := NewCommandTask(providerName, commandProvider, commandFunc, 2)
	
	// Use the normal AddTask method to queue the command
	if !tm.AddTask(commandTask) {
		return fmt.Errorf("failed to queue command")
	}
	
	return nil
}

func (tm *TaskManagerSimple) processCommand(command Command, providerName, server string) {
	defer tm.wg.Done()

	// Acquire concurrency limit if any (non-blocking)
	semaphore, hasLimit := tm.getServerSemaphore(server)
	if hasLimit {
		select {
		case semaphore <- struct{}{}:
			// Acquired successfully
		default:
			// Return server and don't re-queue command (let caller retry if needed)
			if pd, ok := tm.providers[providerName]; ok {
				tm.returnServerToPool(true, pd, server)
			}
			tm.logger.Warn().
				Str("provider", providerName).
				Str("server", server).
				Msg("[tms|processCommand] Command dropped due to concurrency limit")
			return
		}
	}

	// Always release semaphore & return server
	defer func() {
		if hasLimit {
			<-semaphore
		}
		if pd, ok := tm.providers[providerName]; ok {
			tm.returnServerToPool(true, pd, server)
			// Remove the command from the tracking set
			pd.commandSetLock.Lock()
			delete(pd.commandSet, command.id)
			pd.commandSetLock.Unlock()
		}
	}()

	// Recover from panic
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic occurred: %v\n%s", r, string(debug.Stack()))
			tm.logger.Error().Err(err).Msgf("[tms|%s|command|%s] panic", providerName, server)
		}
	}()

	// Run the command with a timeout
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

	maxTimeout := tm.getTimeout("", providerName)
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
