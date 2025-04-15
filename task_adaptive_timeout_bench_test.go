package smt

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
	
	"github.com/rs/zerolog"
)

// BenchmarkAdaptiveTimeoutFeature benchmarks the performance of adaptive timeout feature
func BenchmarkAdaptiveTimeoutFeature(b *testing.B) {
	// Create a logger that won't output during benchmarks
	logger := zerolog.Nop()
	
	// Define test scenarios
	scenarios := []struct {
		name                string
		enableAdaptiveTimeout bool
		variableExecutionTime bool
	}{
		{"WithoutAdaptiveTimeout_ConstantTime", false, false},
		{"WithAdaptiveTimeout_ConstantTime", true, false},
		{"WithoutAdaptiveTimeout_VariableTime", false, true},
		{"WithAdaptiveTimeout_VariableTime", true, true},
	}
	
	// Prepare random number generator
	rand.Seed(time.Now().UnixNano())
	
	// Setup timeouts to be generous initially
	baseTimeout := 200 * time.Millisecond
	
	// Run benchmarks
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			// Create provider with execution time behavior based on scenario
			provider := &MockProvider{
				name: "test-provider",
				handleFunc: func(task ITask, server string) error {
					// Simulate task execution
					var executionTime time.Duration
					
					if scenario.variableExecutionTime {
						// Random execution time between 10-50ms
						executionTime = time.Duration(10+rand.Intn(40)) * time.Millisecond
					} else {
						// Fixed execution time
						executionTime = 30 * time.Millisecond
					}
					
					time.Sleep(executionTime)
					return nil
				},
			}
			
			providers := []IProvider{provider}
			servers := map[string][]string{
				"test-provider": {"server1", "server2", "server3", "server4"},
			}
			
			// Setup timeout function
			getTimeout := func(callback, provider string) time.Duration {
				return baseTimeout
			}
			
			// Create task manager with adaptive timeouts based on scenario
			tm := NewTaskManagerWithOptions(&providers, servers, &logger, getTimeout, &TaskManagerOptions{
				EnableAdaptiveTimeout: scenario.enableAdaptiveTimeout,
			})
			
			// Start the task manager
			tm.Start()
			defer tm.Shutdown()
			
			// Mix of different callback names to simulate different task types
			callbackNames := []string{"callback1", "callback2", "callback3"}
			
			// Pre-populate adaptive timeout statistics if enabled
			if scenario.enableAdaptiveTimeout {
				// Train the adaptive timeout system with some samples
				for i := 0; i < 20; i++ {
					for _, callback := range callbackNames {
						task := &MockTask{
							id:           fmt.Sprintf("train-task-%d", i),
							callbackName: callback,
							provider:     provider,
							maxRetries:   1,
						}
						tm.HandleWithTimeout("test-provider", task, "server1", tm.HandleTask)
					}
				}
			}
			
			// Reset timer after setup
			b.ResetTimer()
			
			// Run the benchmark
			for i := 0; i < b.N; i++ {
				// Select a random callback
				callback := callbackNames[i%len(callbackNames)]
				
				// Create task
				task := &MockTask{
					id:           fmt.Sprintf("bench-task-%d", i),
					callbackName: callback,
					provider:     provider,
					maxRetries:   1,
				}
				
				// Process task
				tm.HandleWithTimeout("test-provider", task, fmt.Sprintf("server%d", (i%4)+1), tm.HandleTask)
			}
			
			// Stop timer to exclude cleanup
			b.StopTimer()
		})
	}
}

// BenchmarkAdaptiveTimeoutTimeoutScenarios benchmarks how adaptive timeouts
// perform in scenarios with occasional slow tasks and timeouts
func BenchmarkAdaptiveTimeoutTimeoutScenarios(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping timeout scenario benchmark in short mode")
	}
	
	// Create a logger that won't output during benchmarks
	logger := zerolog.Nop()
	
	// Define test scenarios
	scenarios := []struct {
		name                string
		enableAdaptiveTimeout bool
		timeoutProbability  float64 // Probability of a task timing out
	}{
		{"WithoutAdaptiveTimeout_RareTimeouts", false, 0.05},
		{"WithAdaptiveTimeout_RareTimeouts", true, 0.05},
		{"WithoutAdaptiveTimeout_FrequentTimeouts", false, 0.20},
		{"WithAdaptiveTimeout_FrequentTimeouts", true, 0.20},
	}
	
	// Prepare random number generator
	rand.Seed(time.Now().UnixNano())
	
	// Setup a tight initial timeout
	baseTimeout := 50 * time.Millisecond
	
	// Run benchmarks
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			// Create provider that sometimes runs slowly (causing timeouts)
			provider := &MockProvider{
				name: "test-provider",
				handleFunc: func(task ITask, server string) error {
					// Determine if this task should "timeout" by running slowly
					if rand.Float64() < scenario.timeoutProbability {
						// Run slower than the timeout
						time.Sleep(baseTimeout * 2)
					} else {
						// Run at normal speed (less than timeout)
						time.Sleep(baseTimeout / 2)
					}
					return nil
				},
			}
			
			providers := []IProvider{provider}
			servers := map[string][]string{
				"test-provider": {"server1"},
			}
			
			// Setup timeout function
			getTimeout := func(callback, provider string) time.Duration {
				return baseTimeout
			}
			
			// Create task manager with adaptive timeouts based on scenario
			tm := NewTaskManagerWithOptions(&providers, servers, &logger, getTimeout, &TaskManagerOptions{
				EnableAdaptiveTimeout: scenario.enableAdaptiveTimeout,
			})
			
			// Start the task manager
			tm.Start()
			defer tm.Shutdown()
			
			// Define different task types
			// Different callback types with different timeout behaviors:
			// - fast-callback: Never times out (0% probability)
			// - normal-callback: Occasionally times out (5% probability)
			// - slow-callback: Times out more frequently (varies by scenario)
			
			// For consistent ordering in the loop
			callbackNames := []string{"fast-callback", "normal-callback", "slow-callback"}
			
			// Pre-populate adaptive timeout statistics if enabled
			if scenario.enableAdaptiveTimeout {
				// Train the adaptive timeout system
				for _, callback := range callbackNames {
					for i := 0; i < 20; i++ {
						task := &MockTask{
							id:           fmt.Sprintf("train-task-%s-%d", callback, i),
							callbackName: callback,
							provider:     provider,
							timeout:      0, // Use default
						}
						tm.HandleWithTimeout("test-provider", task, "server1", tm.HandleTask)
					}
				}
			}
			
			// Reset timer after setup
			b.ResetTimer()
			
			// Run the benchmark
			for i := 0; i < b.N; i++ {
				// Cycle through callback types
				callback := callbackNames[i%len(callbackNames)]
				
				// Create task
				task := &MockTask{
					id:           fmt.Sprintf("bench-task-%d", i),
					callbackName: callback,
					provider:     provider,
					timeout:      0, // Use the adaptive timeout
				}
				
				// Process task
				_, _ = tm.HandleWithTimeout("test-provider", task, "server1", tm.HandleTask)
			}
			
			// Stop timer to exclude cleanup
			b.StopTimer()
		})
	}
}