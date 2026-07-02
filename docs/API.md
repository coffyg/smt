# SMT Public API

The surface actually consumed in production (Soulkyn) is marked ★. Everything listed
here is covered by the backward-compatibility guarantee; internal identifiers not
listed may change.

## Interfaces you implement

### ★ `ITask`

```go
type ITask interface {
    MarkAsSuccess(t int64)            // called once on success; t = elapsed ms
    MarkAsFailed(t int64, err error)  // called once on terminal failure
    GetPriority() int                 // higher = scheduled first (use ≥ 0)
    GetID() string                    // must be unique among live tasks (dedup key)
    GetMaxRetries() int
    GetRetries() int
    GetCreatedAt() time.Time
    GetTaskGroup() ITaskGroup         // may return nil
    GetProvider() IProvider           // must not return nil
    UpdateRetries(int) error
    GetTimeout() time.Duration        // ⚠ NOT used by the manager (see below)
    UpdateLastError(string) error
    GetCallbackName() string          // fed to the getTimeout function
    OnComplete()                      // called once after success OR terminal failure
    OnStart()                         // ⚠ never called by the manager (implement as no-op)
    GetCtx() *context.Context
    SetCtx(ctx context.Context)       // manager stores the timeout context here
}
```

Contract details the signatures don't tell you:

- **Timeouts**: the manager calls the `getTimeout(callbackName, providerName)`
  function you passed at init. `ITask.GetTimeout()` is dead weight required by the
  interface.
- **`SetCtx`** receives a context that carries `taskID`, `providerName`, `serverName`
  values and is cancelled at the timeout deadline. Long-running handlers should watch
  it: the manager gives up at the deadline but cannot kill your goroutine.
- **Lifecycle calls**: exactly one of `MarkAsSuccess` / `MarkAsFailed` per completed
  life, followed by `OnComplete`. A task that is retried gets neither until its final
  attempt resolves. `UpdateRetries` and `UpdateLastError` may be called between
  attempts. All of these can arrive on different goroutines — implementations must be
  concurrency-safe if the same task object is visible elsewhere.
- **Returning an error from `Handle` equal to `sql.ErrNoRows`** short-circuits all
  remaining retries (terminal failure).

### ★ `IProvider`

```go
type IProvider interface {
    Handle(task ITask, server string) error  // the actual work
    Name() string                            // stable key; must match servers map
}
```

`Handle` runs on a dedicated goroutine, may panic safely (recovered → task failure),
and should honor the context set via `task.SetCtx` for cooperative cancellation.

### `ITaskGroup`

Bookkeeping hook (`MarkComplete`, task counts). The manager itself never calls it;
it exists for task implementations that coordinate group completion themselves.

## Initialization

### ★ `InitTaskQueueManagerMaster(cfg ConfigOptions, logger, providers, tasks, servers, getTimeout) error`
### ★ `InitTaskQueueSlave(cfg ConfigOptions, logger, providers, servers, tasks, getTimeout) error`
### `InitTaskQueueManager(logger, providers, tasks, servers, getTimeout)` — local-only

All three create the manager, start it, store it in the global singleton, then
requeue `tasks` (your persisted incomplete work). Master/slave add Redis coordination
— see `DISTRIBUTED.md`. Note the argument-order difference between master
(`tasks, servers`) and slave (`servers, tasks`); it's historical, both are kept as-is.

```go
type ConfigOptions struct {
    RedisAddr    string // "" → local-only mode even via the master/slave inits
    RedisPwd     string
    RedisDB      int
    InstanceName string // "" → hostname-PID. Use a FIXED name per deployment slot —
                        // a restarting instance cleans up its own Redis leftovers
                        // only if the name matches the previous run.
}
```

`getTimeout func(callbackName, providerName string) time.Duration` must return a
positive duration for every combination it can be asked about.

### `NewTaskManagerSimple(providers, servers, logger, getTimeout) *TaskManagerSimple`

Direct construction without the singleton (tests / dependency injection). `Start()`
it yourself; distributed features stay off.

## Runtime operations

### ★ `AddTask(task ITask, logger *zerolog.Logger)` (package-level)
### ★ `(*TaskManagerSimple).AddTask(task ITask) bool`

`false` (or a debug log for the package helper) means: duplicate ID, unknown/nil
provider (task is failed + completed in that case), manager not running, or shutdown
in progress. Duplicate submission is **silent dedup by design** — the second object
is dropped, no lifecycle methods are invoked on it.

### `(*TaskManagerSimple).AddTasks(tasks []ITask) (count int, err error)`

Loop over `AddTask`; `count` = how many were accepted. `err` is always nil (kept for
signature compatibility).

### ★ `DelTask(taskID string, interruptFn func(ITask, string) error, logger) DelTaskResult` (package-level)
### `(*TaskManagerSimple).DelTask(taskID string, interruptFn ...) DelTaskResult`

```go
const (
    DelTaskNotFound            // not queued, not running (see backoff blind spot below)
    DelTaskRemovedFromQueue    // removed before execution; interruptFn(task, "")
    DelTaskInterruptedRunning  // cancel signalled; interruptFn(task, server)
    DelTaskErrorNotRunning
    DelTaskErrorShuttingDown   // package-level helper only
)
```

- `interruptFn` runs synchronously on the caller's goroutine. It is your channel for
  telling the remote server to abort; SMT itself cannot stop a running handler.
- Calling `DelTask` again for the same running task is safe and idempotent
  (returns `DelTaskInterruptedRunning` again, re-invokes `interruptFn`).
- Blind spots: a task in concurrency-backoff sleep (max ~1.2s) or held by a
  dispatcher waiting for a free server reports `DelTaskNotFound` but will still
  execute. If you must prevent execution, make your `Handle` check an external
  kill-flag — Soulkyn's `InteruptedDontRequeueEver` cache is exactly that.

### ★ `(*TaskManagerSimple).SetTaskManagerServerMaxParallel(prefix string, maxParallel int)`

Sets the concurrency budget for a normalized server URL (exact key). `maxParallel <= 0`
deletes the limit (the next task on that server re-creates the default of 1).
In distributed mode this also writes the limit to Redis; on slaves it is ignored for
master-controlled servers. Call it identically on every instance after init — limits
set only on the master do not propagate config state to slaves' local views
(see `DISTRIBUTED.md` § deployment pattern).

⚠ Replacing a limit while tasks hold slots momentarily over-admits in local mode
(fresh semaphore starts empty; holders release into the old one). Set limits at
startup, not under load, or accept the transient.

### ★ `GetTaskQueueManagerInstance() *TaskManagerSimple`

Atomic read of the global singleton; nil before init.

### ★ `(*TaskManagerSimple).Shutdown()`

Blocks until in-flight tasks finish. Queued tasks are dropped from memory (requeue
from your persistence at next boot). Safe to call once; subsequent calls no-op.

### `(*TaskManagerSimple).ExecuteCommand(providerName string, fn func(server string) error) error`

Queues an administrative closure that runs like a task on the next free server, at a
priority **below all normal tasks** (negative, FIFO among commands). One attempt, no
retry, errors are logged not returned. No production callers today.

### `RequeueTaskIfNeeded(logger, tasks []ITask)`

`AddTasks` against the singleton with a log line. Called by the init functions.

### `IsRunning() / HasShutdownRequest() bool`

Atomic state probes.

## Priority conventions

Priorities are raw ints, higher first. Suggested bands (SMT does not enforce them):
business tasks 0–100, urgent/user-facing above background, never negative (that range
belongs to `ExecuteCommand` internals).

## Compatibility guarantee

Everything above keeps its signature and observable semantics. Fixes documented in
`KNOWN-ISSUES.md` change behavior only where the previous behavior was a crash, a
leak, or a documented-vs-actual mismatch being realigned.
