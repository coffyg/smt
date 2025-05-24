package smt

import (
	"testing"
	"time"
	"github.com/rs/zerolog"
)

func BenchmarkTaskManagerIdleLoad(b *testing.B) {
	logger := zerolog.New(zerolog.NewConsoleWriter()).Level(zerolog.ErrorLevel)
	
	// Create providers
	var providers []IProvider
	servers := make(map[string][]string)
	
	for i := 0; i < 5; i++ {
		providerName := "provider" + string(rune('A' + i))
		provider := &MockProvider{
			name: providerName,
			handleFunc: func(task ITask, server string) error {
				time.Sleep(10 * time.Millisecond)
				return nil
			},
		}
		providers = append(providers, provider)
		
		// Add 10 servers per provider
		serverList := make([]string, 10)
		for j := 0; j < 10; j++ {
			serverList[j] = "server_" + providerName + "_" + string(rune('0' + j))
		}
		servers[providerName] = serverList
	}
	
	getTimeout := func(string, string) time.Duration {
		return 30 * time.Second
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		// Initialize and run empty
		InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)
		
		// Let it run idle
		time.Sleep(100 * time.Millisecond)
		
		// Shutdown
		TaskQueueManagerInstance.Shutdown()
	}
}

func BenchmarkTaskManagerLightLoad(b *testing.B) {
	logger := zerolog.New(zerolog.NewConsoleWriter()).Level(zerolog.ErrorLevel)
	
	// Create providers
	var providers []IProvider
	servers := make(map[string][]string)
	
	for i := 0; i < 5; i++ {
		providerName := "provider" + string(rune('A' + i))
		provider := &MockProvider{
			name: providerName,
			handleFunc: func(task ITask, server string) error {
				time.Sleep(5 * time.Millisecond)
				return nil
			},
		}
		providers = append(providers, provider)
		
		serverList := make([]string, 10)
		for j := 0; j < 10; j++ {
			serverList[j] = "server_" + providerName + "_" + string(rune('0' + j))
		}
		servers[providerName] = serverList
	}
	
	getTimeout := func(string, string) time.Duration {
		return 30 * time.Second
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)
		
		// Add some light load
		for j := 0; j < 50; j++ {
			task := &MockTask{
				id:         "task" + string(rune(j)),
				priority:   j % 10,
				maxRetries: 1,
				provider:   providers[j%len(providers)],
				timeout:    100 * time.Millisecond,
				done:       make(chan struct{}),
			}
			AddTask(task, &logger)
		}
		
		// Wait for completion
		time.Sleep(200 * time.Millisecond)
		
		TaskQueueManagerInstance.Shutdown()
	}
}