package smt

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"
)

type Command struct {
	commandFunc func(server string) error
}

func (tm *TaskManagerSimple) ExecuteCommand(providerName string, commandFunc func(server string) error) error {
	if !tm.IsRunning() {
		return errors.New("TaskManager is not running")
	}
	pd, ok := tm.providers[providerName]
	if !ok {
		return fmt.Errorf("provider '%s' not found", providerName)
	}
	pd.taskQueueLock.Lock()
	defer pd.taskQueueLock.Unlock()
	pd.commandQueue = append(pd.commandQueue, Command{commandFunc: commandFunc})
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

		tm.logger.Debug().Msgf("[tms|%s|command] Command STARTED on server %s", providerName, server)
		done <- command.commandFunc(server)
	}()

	select {
	case <-ctx.Done():
		err = fmt.Errorf("[tms|%s|command] Command timed out on server %s", providerName, server)
		tm.logger.Error().Err(err).Msgf("[%s|command] Command FAILED-TIMEOUT on server %s, took %s", providerName, server, time.Since(startTime))
	case err = <-done:
		if err == nil {
			tm.logger.Debug().Msgf("[tms|%s|command] Command COMPLETED on server %s, took %s", providerName, server, time.Since(startTime))
		} else {
			tm.logger.Error().Err(err).Msgf("[tms|%s|command] Command FAILED on server %s, took %s", providerName, server, time.Since(startTime))
		}
	}

	return err, time.Since(startTime).Milliseconds()
}
