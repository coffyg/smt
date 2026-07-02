# SMT Architecture

*Written from a full read of the source (2026-07-02). This describes what the code
actually does, not what it was once intended to do.*

SMT (Swift Multiplexing Tasker) is a provider-based task scheduler. One
`TaskManagerSimple` orchestrates N independent **providers**; each provider has its
own priority queue, its own pool of **server** strings, and one dedicated dispatcher
goroutine. Tasks are user-defined objects implementing `ITask`; execution logic lives
in the provider's `Handle(task, server)`.

It runs in two modes, chosen at init:

- **Local mode** (`InitTaskQueueManager`): concurrency limits enforced with in-process
  channel semaphores.
- **Distributed mode** (`InitTaskQueueManagerMaster` / `InitTaskQueueSlave`): several
  processes share per-server concurrency budgets through Redis (Lua-scripted slot
  counters), with a master instance owning config publication. See `DISTRIBUTED.md`.

## Core objects

```
TaskManagerSimple
├── providers map[string]*ProviderData     one entry per provider name
│   ├── taskQueue   TaskQueuePrio          binary heap of *TaskWithPriority
│   ├── taskQueueCond sync.Cond            dispatcher sleeps here when idle
│   ├── availableServers chan string       the server pool (buffered, cap = 2×len)
│   ├── commandQueue *CommandQueue         legacy ring buffer (see "Command path")
│   └── taskCount / commandCount int32     atomic fast-path counters
├── taskInQueue map[string]struct{}        task-ID dedup set (RWMutex)
├── runningTasks map[string]*RunningTaskInfo   currently executing tasks (RWMutex)
├── taskToProvider sync.Map                taskID → providerName, O(1) DelTask lookup
├── serverConcurrencyMap map[string]chan struct{}   local semaphores (RWMutex)
├── concurrencyRetryMap sync.Map           taskID → backoff attempt count
└── slotCoordinator *RedisSlotCoordinator  nil in local mode
```

## Task lifecycle

```
AddTask(task)
  │  reject if shutting down / not running
  │  reject if provider nil or unknown        → MarkAsFailed + OnComplete
  │  reject if task ID already tracked        → return false (silent dedup)
  │  store taskID→provider, push onto heap, Signal dispatcher
  ▼
providerDispatcher (one goroutine per provider)
  │  sleeps on taskQueueCond when both queues empty (atomic counters checked first)
  │  pops highest-priority task
  │  blocks on <-availableServers  (or exits on shutdown)
  ▼
processTask (new goroutine per execution)
  │  1. acquire concurrency slot for the *normalized* server URL
  │     - local: non-blocking semaphore try
  │     - distributed: fairness check + Lua acquire in Redis
  │     on rejection: server returned to pool, task leaves the dedup set,
  │     goroutine sleeps an exponential backoff (5ms·2^n + jitter, cap 800ms),
  │     then re-AddTask. Does NOT consume a task retry.
  │  2. registerRunningTask (makes DelTask interruption possible)
  │  3. cancelled-before-start check (DelTask may have closed cancelCh)
  │  4. HandleWithTimeout → provider.Handle(task, server)
  ▼
completion
  │  success        → MarkAsSuccess + OnComplete
  │  error, retries left → UpdateRetries(n+1), immediate re-AddTask (no delay)
  │  error, retries exhausted OR err == sql.ErrNoRows → MarkAsFailed + OnComplete
  │  always: unregister running task, release slot, return server to pool
```

Notes that matter in production:

- **The task timeout comes from the manager's `getTimeout(callbackName, providerName)`
  function, not from `ITask.GetTimeout()`.** The interface method exists but the
  manager never calls it.
- **`sql.ErrNoRows` is a terminal error**: a provider returning it fails the task
  immediately regardless of remaining retries (convention: the task's DB row is gone,
  retrying is pointless).
- **Failure retries are immediate.** There is no delay between a task failing and its
  re-execution; only *concurrency-slot* rejections get exponential backoff. A reliably
  failing endpoint will absorb `maxRetries` attempts back-to-back.
- **Timeout does not kill the handler.** `HandleWithTimeout` returns at the deadline
  and the task is marked failed/retried, but the `Handle` goroutine keeps running
  until it returns on its own. The context stored via `task.SetCtx()` is cancelled at
  the deadline — providers must honor it if they want real cancellation. Until the
  orphaned handler returns, it does NOT hold the concurrency slot (released at
  timeout), so a slow endpoint can transiently see more in-flight work than its limit.
- **Panic isolation**: panics in `Handle` are recovered in two layers (handler
  goroutine + processTask) and converted to task failure with a stack trace in the log.

## Priority semantics

`TaskQueuePrio` is a binary heap ordered by **higher `GetPriority()` first**. Ties
break on heap array index, which approximates — but does not guarantee — FIFO among
equal priorities. Treat it as "roughly insertion-ordered within a priority class".

Priorities are plain ints with no reserved ranges. `ExecuteCommand` tasks use
negative priorities (see below), so avoid negative priorities for business tasks
unless you intend to run below commands.

## Server pool and the `?s=N` pattern

A provider's servers are plain strings, delivered once into a buffered channel that
acts as the pool. A server string is checked out per task and returned when the task
finishes (or is rejected by a concurrency limit).

Concurrency limits are keyed by the server URL **normalized** — query string and
fragment stripped (`url.Parse`, `RawQuery = ""`, `Fragment = ""`). This enables the
deployment pattern Soulkyn uses:

```
pool:  https://host/api?s=1, https://host/api?s=2, ... https://host/api?s=N
limit: SetTaskManagerServerMaxParallel("https://host/api", K)
```

N pool entries give the dispatcher N checkout tokens for that endpoint; the
normalized-key limit caps actual in-flight execution at K. The `?s=N` suffix exists
only to multiply pool entries; every variation shares one concurrency budget.

Limit resolution order in local mode (`getServerSemaphore`):
1. exact match on the normalized URL,
2. any key that is a **prefix** of the normalized URL (map iteration order — if
   several prefixes match, the winner is unspecified),
3. otherwise a default limit of **1** is created on the fly.

Every server present at init gets an explicit default limit of 1, so prefix rules set
later never apply to those exact URLs — always set limits with the exact normalized
URL (this is what Soulkyn does). In distributed mode resolution is exact-key-only in
Redis (`smt:slots:<url>:max`); prefixes don't participate at all.

## The dispatcher

One goroutine per provider. Idle path: checks the atomic `taskCount`/`commandCount`
counters, then double-checks under the queue lock and sleeps on the condition
variable. Wake-ups come from `AddTask` (Signal) and `Shutdown` (Broadcast).

After popping work it blocks on `availableServers` — so one queued task can hold the
dispatcher hostage while all of that provider's servers are busy. That is by design
(per-provider isolation: other providers have their own dispatchers), but it means a
single provider's queue drains strictly one-server-checkout at a time. A
higher-priority task submitted while the dispatcher is already blocked waits for the
*next* free server; the held task is not re-evaluated.

`processTask`/`processCommand` run in fresh goroutines, so the dispatcher returns to
its loop immediately after handing off.

## DelTask (cancellation)

`DelTask(taskID, interruptFn)` has three outcomes:

- **Running** → the task's `cancelCh` is closed, `interruptFn(task, server)` is
  invoked synchronously (your hook to tell the remote endpoint to stop), returns
  `DelTaskInterruptedRunning`. The task's handler is NOT killed — same rules as
  timeout above.
- **Queued** → removed from the heap (O(queue length) scan of the owning provider's
  queue, found via the `taskToProvider` map), `interruptFn(task, "")` invoked,
  returns `DelTaskRemovedFromQueue`.
- **Neither** → `DelTaskNotFound`.

Blind spots — two windows where a live task is in *neither* structure and DelTask
returns `DelTaskNotFound` even though the task will still run:

- **concurrency-backoff sleep** (up to ~1.2s): the task left the dedup set and
  hasn't re-entered the queue yet;
- **dispatcher's hand** (normally microseconds, but as long as *all servers are
  busy*): the dispatcher pops a task from the heap and then blocks waiting for a
  free server — the popped task is not in the queue and not yet registered as
  running.

If cancellation must be airtight, gate execution inside `Handle` on an external
kill-flag (Soulkyn's `InteruptedDontRequeueEver` cache is exactly that).

## Command path

`ExecuteCommand(providerName, fn)` wraps `fn` in a `CommandTask` and pushes it through
the **normal task queue** with priority `-(sequence)-1`. Two consequences:

- Commands are FIFO among themselves (more-negative = later).
- Commands run **after** every pending business task of priority ≥ 0, not before.
  (Historical docs claimed the opposite; the code is unambiguous.)

The separate `CommandQueue` ring buffer, `commandSet`, and the dispatcher's
command-dequeue branch are a **legacy path with no public producer** — nothing in the
codebase ever calls `commandQueue.Enqueue`. It is kept for API stability but is dead
in practice.

## Memory behavior

- `TaskWithPriority` wrappers are pooled (`sync.Pool`).
- The task heap pre-allocates 1024 slots per provider and grows 1.5×.
- `taskInQueue` is recreated when it empties, releasing map buckets after spikes.
- The command ring buffer shrinks when 4× oversized (floor 256).

## Shutdown

`Shutdown()` sets the shutdown flag, closes `shutdownCh`, broadcasts all dispatcher
condition variables, and waits on the WaitGroup covering dispatchers and in-flight
`processTask` goroutines. **Queued-but-unstarted tasks are abandoned in memory** —
persistence/requeue is the caller's job (Soulkyn re-adds incomplete tasks from
Postgres at boot via `RequeueTaskIfNeeded`). In distributed mode it then closes the
config subscription, stops heartbeat/lock renewal, releases the master lock and all
Redis slots, and closes the Redis client.
