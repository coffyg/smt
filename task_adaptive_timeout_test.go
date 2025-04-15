package smt

import (
	"testing"
	"time"
	
	"github.com/rs/zerolog"
)

// TestAdaptiveTimeoutManager tests the functionality of the adaptive timeout manager
func TestAdaptiveTimeoutManager(t *testing.T) {
	// Create a new adaptive timeout manager
	manager := NewAdaptiveTimeoutManager(true)
	
	// Register some base timeouts
	provider := "test-provider"
	callback := "test-callback"
	baseTimeout := 1 * time.Second
	
	manager.RegisterBaseTimeout(provider, callback, baseTimeout)
	
	// Test initial timeout (should be base timeout)
	timeout := manager.GetTimeout(callback, provider)
	if timeout != baseTimeout {
		t.Errorf("Expected initial timeout to be %v, got %v", baseTimeout, timeout)
	}
	
	// Record some executions
	// First few executions should not change timeout (not enough samples)
	execTime := int64(100) // 100 ms
	for i := 0; i < 4; i++ {
		manager.RecordExecution(provider, callback, execTime, baseTimeout)
	}
	
	// Timeout should still be base timeout (not enough samples)
	timeout = manager.GetTimeout(callback, provider)
	if timeout != baseTimeout {
		t.Errorf("Expected timeout to still be %v, got %v", baseTimeout, timeout)
	}
	
	// Record more executions to meet threshold
	for i := 0; i < 5; i++ {
		manager.RecordExecution(provider, callback, execTime, baseTimeout)
	}
	
	// Now timeout should be adapted (something less than base timeout)
	timeout = manager.GetTimeout(callback, provider)
	expectedMin := time.Duration(float64(baseTimeout) * manager.minMultiplier)
	expectedMax := time.Duration(float64(baseTimeout) * manager.maxMultiplier)
	
	if timeout < expectedMin || timeout > expectedMax {
		t.Errorf("Expected timeout to be between %v and %v, got %v", expectedMin, expectedMax, timeout)
	}
	
	// Ensure we can disable adaptive timeouts
	manager.SetEnabled(false)
	timeout = manager.GetTimeout(callback, provider)
	if timeout != baseTimeout {
		t.Errorf("Expected timeout to be base timeout %v when disabled, got %v", baseTimeout, timeout)
	}
}

// TestAdaptiveTimeoutWithTaskManager tests integration with TaskManagerSimple
func TestAdaptiveTimeoutWithTaskManager(t *testing.T) {
	// Create a logger that won't output during tests
	logger := zerolog.Nop()
	
	// Create mock provider with fixed execution time
	fixedDelay := 50 * time.Millisecond
	provider := &MockProvider{
		name: "test-provider",
		handleFunc: func(task ITask, server string) error {
			time.Sleep(fixedDelay)
			return nil
		},
	}
	
	providers := []IProvider{provider}
	servers := map[string][]string{
		"test-provider": {"server1", "server2"},
	}
	
	// Set a high base timeout
	baseTimeout := 500 * time.Millisecond
	getTimeout := func(callback, provider string) time.Duration {
		return baseTimeout
	}
	
	// Create task manager with adaptive timeouts enabled
	tm := NewTaskManagerWithOptions(&providers, servers, &logger, getTimeout, &TaskManagerOptions{
		EnableAdaptiveTimeout: true,
	})
	
	// Start the task manager
	tm.Start()
	defer tm.Shutdown()
	
	// Run several tasks to build up statistics
	callbackName := "test-callback"
	taskCount := 10
	for i := 0; i < taskCount; i++ {
		task := &MockTask{
			id:           "test-task",
			callbackName: callbackName,
			provider:     provider,
		}
		
		// Run the task directly through HandleWithTimeout
		err, _ := tm.HandleWithTimeout("test-provider", task, "server1", tm.HandleTask)
		if err != nil {
			t.Errorf("Expected task to succeed, got error: %v", err)
		}
	}
	
	// Get the current adaptive timeout after training
	// This is testing implementation details, but it's necessary to verify the feature
	adaptiveTimeout := tm.adaptiveTimeout.GetTimeout(callbackName, "test-provider")
	
	// Verify the timeout has been adapted (should be significantly less than base timeout)
	if adaptiveTimeout >= baseTimeout {
		t.Errorf("Expected adaptive timeout to be less than base timeout, got %v", adaptiveTimeout)
	}
	
	// Verify it's within a reasonable range based on the fixed delay
	minExpected := time.Duration(float64(fixedDelay) * 1.1) // At least 10% more than execution time
	// Upper bound is checked against baseTimeout below
	
	if adaptiveTimeout < minExpected {
		t.Errorf("Adaptive timeout too small: %v, expected at least %v", adaptiveTimeout, minExpected)
	}
	
	if adaptiveTimeout > baseTimeout {
		t.Errorf("Adaptive timeout exceeds base timeout: %v, base was %v", adaptiveTimeout, baseTimeout)
	}
}