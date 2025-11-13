# Go SMT (Swift Multiplexing Tasker)

SMT is a high-performance, concurrent task management system written in Go. It is designed to manage and execute a large number of tasks efficiently across a pool of workers, with fine-grained control over prioritization, concurrency, and error handling.

## Core Concepts

The SMT architecture is built around a few key concepts:

-   **Tasks (`ITask`)**: The fundamental unit of work. A task is any operation that needs to be executed, such as an API call, a database query, or a file transformation. Tasks are defined by the `ITask` interface, which allows the system to manage their state, priority, and lifecycle.

-   **Providers (`IProvider`)**: The workers responsible for executing tasks. A provider contains the actual logic for how a task should be handled. For example, you might have an `HttpProvider` for making web requests or a `DbProvider` for database operations.

-   **Servers**: The execution targets for a provider. A server is simply a string (like a URL or a database connection string) that tells a provider where to execute its task. Each provider has its own pool of available servers.

-   **Commands**: Special, high-priority administrative actions that can be sent to the task manager. Unlike tasks, which represent business logic, commands are used for control-plane operations within the SMT system itself.

## Architecture Deep Dive

### The `TaskManagerSimple`

This is the central orchestrator of the entire system. It manages a map of `ProviderData`, where each provider has its own dedicated task queue, command queue, and server pool.

The manager runs a dispatcher goroutine for each provider, which is responsible for pulling work from the queues and assigning it to an available server.

### Task Flow and Lifecycle

A task's journey through the system follows a clear path:

1.  **Submission**: A task is submitted via `AddTask()`. The system performs a quick check to prevent duplicate task IDs from being enqueued.
2.  **Queuing**: The task is wrapped in a `TaskWithPriority` object and pushed into the appropriate provider's **priority queue**.
3.  **Dispatching**: A dispatcher goroutine, waiting on a condition variable, is signaled that new work is available. It pops the highest-priority task from the queue.
4.  **Server Allocation**: The dispatcher acquires an available server from the provider's server channel.
5.  **Execution**: The task is passed to `processTask()`, which is responsible for the execution phase.
6.  **Concurrency Control**: Before running, `processTask` attempts to acquire a concurrency slot for the target server. If all slots are busy, the task is immediately re-queued, and an exponential backoff with jitter is applied to prevent a thundering herd problem.
7.  **Timeout-Wrapped Execution**: The task's `Handle()` method is executed within a `HandleWithTimeout` wrapper, ensuring that no task can run indefinitely.
8.  **Completion & Cleanup**:
    *   On **success**, the task is marked as such and removed from the system.
    *   On **failure**, the system checks the task's retry policy. If more retries are allowed, the retry counter is incremented, and the task is re-queued. Otherwise, it is marked as permanently failed and removed.
    *   The server and concurrency slots are always released, and the task is unregistered from the `runningTasks` map.

### Dual-Queue System

SMT uses a sophisticated dual-queue system for each provider to separate business logic from administrative actions.

#### 1. Task Priority Queue (`TaskQueuePrio`)

This is the primary queue for `ITask` objects. It is implemented as a **min-heap** based on priority.

-   **Prioritization**: Higher integer values have higher priority.
-   **Fairness**: If two tasks have the same priority, the one that was added earlier is executed first (FIFO for equal priority).

#### 2. Command Queue (`CommandQueue`)

This is a standard, thread-safe **FIFO queue** implemented with an efficient, resizing ring buffer. It holds `Command` objects.

-   **Execution**: Commands are wrapped in a special `CommandTask` that gives them a negative priority, ensuring they are almost always processed before regular tasks in the main dispatch loop. This allows administrative actions to be handled swiftly.

### Concurrency and Backoff

SMT provides robust protection against overloading your servers.

-   **Semaphore-Based Limiting**: Concurrency is managed by a map of semaphores (`serverConcurrencyMap`), where each key is a server URL or prefix. You can set a specific parallel limit for any server or group of servers (e.g., limit all requests to `https://api.example.com` to 10).
-   **Exponential Backoff**: When a task fails to acquire a concurrency slot, it doesn't busy-wait. It is re-queued, and the system calculates a delay using an exponential backoff algorithm with added jitter. This gracefully handles contention and prevents retry storms.

### Graceful Shutdown

A clean shutdown is initiated by calling `Shutdown()`. This closes a `shutdownCh`, which signals all dispatcher goroutines to stop accepting new work. The manager then waits on a `sync.WaitGroup` for all currently running tasks to complete before it fully stops.

## How to Use

To integrate SMT, you need to follow these steps:

1.  **Implement the Core Interfaces**:
    *   Create your own task structs that satisfy the `ITask` interface.
    *   Create provider structs that satisfy the `IProvider` interface, containing the logic to handle your tasks.

2.  **Initialize the Manager**:
    *   Create a list of your providers.
    *   Create a `map[string][]string` that associates each provider's name with a slice of its available server strings.
    *   Instantiate `TaskManagerSimple` using `NewTaskManagerSimple()`, providing the providers, servers, a logger, and a timeout function.

3.  **Start the Manager**:
    *   Call `tm.Start()` to begin the dispatcher goroutines.

4.  **Add Tasks**:
    *   Call `tm.AddTask(yourTask)` to submit work to the system.

The task manager will then handle the scheduling, execution, and lifecycle of your tasks automatically.

## Example: Coordinating AI and SSR Tasks

Here is a practical example demonstrating how to use SMT to manage two different kinds of workloads: AI inference tasks for GPU servers and Server-Side Rendering (SSR) tasks for Node.js servers.

### 1. Define the Providers

First, we define our two providers. The `AIProvider` will handle AI tasks, and the `SsrProvider` will handle rendering tasks.

```go
package main

import (
	"fmt"
	"time"
	"math/rand"

	"github.com/coffyg/smt" // Assuming smt is the module name
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"os"
)

// AIProvider handles tasks for AI model inference.
type AIProvider struct {}

func (p *AIProvider) Name() string { return "ai_provider" }

func (p *AIProvider) Handle(task smt.ITask, server string) error {
	aiTask, ok := task.(*AITask)
	if !ok {
		return fmt.Errorf("invalid task type for AIProvider")
	}
	fmt.Printf("AI WORKER: Running AI task '%s' with prompt '%s' on GPU server '%s'\n", aiTask.GetID(), aiTask.Prompt, server)
	// Simulate work
	time.Sleep(time.Duration(100+rand.Intn(100)) * time.Millisecond)
	return nil
}

// SsrProvider handles tasks for server-side rendering.
type SsrProvider struct {}

func (p *SsrProvider) Name() string { return "ssr_provider" }

func (p *SsrProvider) Handle(task smt.ITask, server string) error {
	ssrTask, ok := task.(*SsrTask)
	if !ok {
		return fmt.Errorf("invalid task type for SsrProvider")
	}
	fmt.Printf("SSR WORKER: Rendering page '%s' on Node.js server '%s'\n", ssrTask.PageURL, server)
	// Simulate work
	time.Sleep(time.Duration(50+rand.Intn(50)) * time.Millisecond)
	return nil
}
```

### 2. Define the Tasks

Next, we define the structs for `AITask` and `SsrTask`. They must both implement the `smt.ITask` interface. For simplicity, many methods in this example return default values.

```go
package main

// AITask represents a job for a GPU worker.
type AITask struct {
	id       string
	provider smt.IProvider
	Prompt   string
}

func NewAITask(provider smt.IProvider, prompt string) *AITask {
	return &AITask{id: uuid.New().String(), provider: provider, Prompt: prompt}
}

// Implement smt.ITask for AITask
func (t *AITask) GetID() string { return t.id }
func (t *AITask) GetProvider() smt.IProvider { return t.provider }
func (t *AITask) GetPriority() int { return 1 } // Normal priority
func (t *AITask) MarkAsSuccess(d int64) { fmt.Printf("AI task %s completed in %dms\n", t.id, d) }
func (t *AITask) MarkAsFailed(d int64, err error) { fmt.Printf("AI task %s failed: %v\n", t.id, err) }
// Default implementations for other interface methods
func (t *AITask) GetMaxRetries() int { return 3 }
func (t *AITask) GetRetries() int { return 0 }
func (t *AITask) UpdateRetries(r int) error { return nil }
func (t *AITask) GetCreatedAt() time.Time { return time.Now() }
func (t *AITask) GetTaskGroup() smt.ITaskGroup { return nil }
func (t *AITask) GetTimeout() time.Duration { return 5 * time.Second }
func (t *AITask) UpdateLastError(e string) error { return nil }
func (t *AITask) GetCallbackName() string { return "" }
func (t *AITask) OnComplete() {}
func (t *AITask) OnStart() {}


// SsrTask represents a job for a Node.js renderer.
type SsrTask struct {
	id       string
	provider smt.IProvider
	PageURL  string
}

func NewSsrTask(provider smt.IProvider, pageUrl string) *SsrTask {
	return &SsrTask{id: uuid.New().String(), provider: provider, PageURL: pageUrl}
}

// Implement smt.ITask for SsrTask
func (t *SsrTask) GetID() string { return t.id }
func (t *SsrTask) GetProvider() smt.IProvider { return t.provider }
func (t *SsrTask) GetPriority() int { return 10 } // Higher priority for user-facing tasks
func (t *SsrTask) MarkAsSuccess(d int64) { fmt.Printf("SSR task %s completed in %dms\n", t.id, d) }
func (t *SsrTask) MarkAsFailed(d int64, err error) { fmt.Printf("SSR task %s failed: %v\n", t.id, err) }
// Default implementations for other interface methods
func (t *SsrTask) GetMaxRetries() int { return 2 }
func (t *SsrTask) GetRetries() int { return 0 }
func (t *SsrTask) UpdateRetries(r int) error { return nil }
func (t *SsrTask) GetCreatedAt() time.Time { return time.Now() }
func (t *SsrTask) GetTaskGroup() smt.ITaskGroup { return nil }
func (t *SsrTask) GetTimeout() time.Duration { return 2 * time.Second }
func (t *SsrTask) UpdateLastError(e string) error { return nil }
func (t *SsrTask) GetCallbackName() string { return "" }
func (t *SsrTask) OnComplete() {}
func (t *SsrTask) OnStart() {}
```

### 3. Set Up and Run the Task Manager

Finally, the `main` function wires everything together. It initializes the providers, defines the server pools, creates the task manager, and submits tasks.

```go
package main

func main() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// 1. Initialize Providers
	aiProvider := &AIProvider{}
	ssrProvider := &SsrProvider{}
	providers := []smt.IProvider{aiProvider, ssrProvider}

	// 2. Define Server Pools for each Provider
	servers := map[string][]string{
		aiProvider.Name():  {"gpu-server-1:8080", "gpu-server-2:8080"},
		ssrProvider.Name(): {"ssr-node-1:3000", "ssr-node-2:3000", "ssr-node-3:3000"},
	}

	// 3. Create and Start the Task Manager
	getTimeout := func(callbackName string, providerName string) time.Duration {
		return 10 * time.Second // Default timeout
	}
	taskManager := smt.NewTaskManagerSimple(&providers, servers, &logger, getTimeout)
	taskManager.Start()
	fmt.Println("Task Manager Started.")

	// 4. Add Tasks to the Manager
	// SsrTasks have higher priority (10) and will be picked up first.
	taskManager.AddTask(NewSsrTask(ssrProvider, "/home"))
	taskManager.AddTask(NewAITask(aiProvider, "A photo of a gopher writing Go code"))
	taskManager.AddTask(NewSsrTask(ssrProvider, "/pricing"))
	taskManager.AddTask(NewSsrTask(ssrProvider, "/about"))
	taskManager.AddTask(NewAITask(aiProvider, "A futuristic city skyline at dusk"))

	// Let the tasks run for a bit
	fmt.Println("Waiting for tasks to be processed...")
	time.Sleep(2 * time.Second)

	// 5. Gracefully Shut Down the Manager
	fmt.Println("Shutting down Task Manager...")
	taskManager.Shutdown()
	fmt.Println("Task Manager Shutdown Complete.")
}