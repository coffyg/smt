# Simple Task Management (SMT)

A Go library for task queue management with high performance optimizations.

## Features

- Task queue management with priority support
- Provider-based task dispatching
- Server-based execution with configurable timeout
- Memory pooling for task objects
- Task batching for improved throughput
- Background task compaction for memory optimization
- Thread-safe operation with proper synchronization

## Usage

```go
// Create a task manager with default options (memory pooling and compaction enabled)
tm := NewTaskManagerSimple(&providers, servers, &logger, timeoutFunc)

// Start the task manager
tm.Start()

// Add tasks
tm.AddTask(task)

// Shutdown when done
tm.Shutdown()
```

## Configuration Options

The task manager can be configured with various options:

```go
tm := NewTaskManagerWithOptions(&providers, servers, &logger, timeoutFunc, &TaskManagerOptions{
    // Memory pooling settings
    EnablePooling: true,

    // Task batching settings
    EnableBatching: true,
    BatchMaxSize: 100,
    BatchMaxWait: 100 * time.Millisecond,

    // Background task compaction settings
    EnableCompaction: true,
    CompactionInterval: 1 * time.Minute,
    CompactionThreshold: 50,
})
```

## Optimizations

See detailed information about the optimizations in:
- [MEMORY_POOL_SUMMARY.md](MEMORY_POOL_SUMMARY.md)
- [COMPACTION_SUMMARY.md](COMPACTION_SUMMARY.md)
- [BATCH_PERFORMANCE.md](BATCH_PERFORMANCE.md)
