package main

import (
	"fmt"
	"runtime"
	"time"

	smt "github.com/coffyg/smt"
	"github.com/rs/zerolog"
)

// MockProvider for testing
type MockProvider struct {
	name       string
	handleFunc func(task smt.ITask, server string) error
}

func (p *MockProvider) Handle(task smt.ITask, server string) error {
	if p.handleFunc != nil {
		return p.handleFunc(task, server)
	}
	return nil
}

func (p *MockProvider) Name() string {
	return p.name
}

func main() {
	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create empty provider and server setup
	var providers []smt.IProvider
	servers := make(map[string][]string)
	
	// Create a minimal server setup
	for i := 0; i < 3; i++ {
		providerName := fmt.Sprintf("idle_provider_%d", i)
		provider := &MockProvider{name: providerName}
		providers = append(providers, provider)
		
		// Add 5 servers
		serverList := make([]string, 5)
		for j := 0; j < 5; j++ {
			serverList[j] = fmt.Sprintf("idle_server_%s_%s", providerName, string(rune('A'+j)))
		}
		servers[providerName] = serverList
	}
	
	// Define getTimeout function
	getTimeout := func(callbackName string, providerName string) time.Duration {
		return time.Second
	}

	// Set GOMAXPROCS to match available cores for accurate measurement
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)
	fmt.Printf("Running with GOMAXPROCS=%d\n", numCPU)

	// Measure memory usage before initialization
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	
	// Initialize TaskManager with zero tasks
	smt.InitTaskQueueManager(&logger, &providers, nil, servers, getTimeout)
	
	// Verify task manager is running
	if !smt.TaskQueueManagerInstance.IsRunning() {
		fmt.Println("Task manager failed to start")
		return
	}
	
	// Allow system to stabilize
	time.Sleep(time.Second)

	// Run empty system for a few seconds with no tasks
	fmt.Println("Measuring zero-load CPU and memory usage...")
	runtime.GC() // Force garbage collection before measurement
	
	// Let it run idle for 10 seconds
	time.Sleep(10 * time.Second)
	
	// Check memory usage after running
	runtime.ReadMemStats(&m2)
	
	// Calculate memory growth during idle period
	heapDiff := int64(m2.HeapAlloc - m1.HeapAlloc)
	objectsDiff := int64(m2.HeapObjects - m1.HeapObjects)
	
	fmt.Println("Idle Memory Stats:")
	fmt.Printf("  Heap Growth: %d bytes\n", heapDiff)
	fmt.Printf("  Object Count Growth: %d objects\n", objectsDiff)
	fmt.Printf("  Heap Alloc: %d bytes\n", m2.HeapAlloc)
	fmt.Printf("  System Memory: %d bytes\n", m2.Sys)
	
	// Shutdown TaskManager
	smt.TaskQueueManagerInstance.Shutdown()
	fmt.Println("Task manager shut down successfully")
}