# Known Issues & Fix Log

Findings from the 2026-07-02 full-code audit. Status: `FIXED` entries were resolved
in that pass (tests added); `OPEN` entries are documented behavior we chose not to
change (compat) or accepted trade-offs.

## FIXED

### 1. Double `DelTask` on a running task froze the whole manager — CRITICAL
`DelTask` closed the task's `cancelCh` unconditionally. A second call for the same
still-running task panicked (`close of closed channel`) **while holding
`runningTasksMu`**, so the recovered panic (e.g. by an HTTP framework) left the mutex
locked forever: every subsequent task registration blocked → total scheduler freeze
until process restart. Reachable in production via double-cancel of an image task.
Fix: idempotent cancel (guarded close); repeat calls return
`DelTaskInterruptedRunning` and re-invoke `interruptFn`. Regression test:
`TestDelTaskRunningTwice`.

### 2. Data race: `configPubSub` (slave init vs shutdown)
`subscribeConfigUpdates` (goroutine) wrote `tm.configPubSub` while `Shutdown` read it
— caught by `-race` in `TestSlaveIgnoresMasterServers` (the one red test in the
baseline). Fix: the subscription is created synchronously during slave init, before
the listener goroutine starts; the field is never written concurrently afterwards.

### 3. Dead-instance detection could never fire
The monitor scanned `smt:instances:*:heartbeat` and looked for keys with expired
TTLs — but expired keys don't survive to be scanned, so `deadInstances` was always
empty. Crashed instances leaked their slots until a same-name restart
(`cleanupGhostInstance`) rescued them; auto-generated instance names leaked forever.
The unit test masked this by calling `cleanupDeadInstance` directly.
Fix: instances register in a persistent `smt:instances:registry` set (SADD at
heartbeat start and on each beat, SREM on clean shutdown); the monitor walks the
registry and treats *missing heartbeat key* as dead. Test:
`TestDeadInstanceDetection` (detection path, not just cleanup).

### 4. Silent task loss when re-queue failed during shutdown
The concurrency-backoff and retry paths ended in `tm.AddTask(task)`; if that returned
`false` because a shutdown began during the backoff sleep, the task vanished —
no `MarkAsFailed`, no `OnComplete`, upstream state stuck at "processing" forever
(the DB-side symptom: tasks needing a manual requeue after deploys).
Fix: when re-queue fails and the manager is stopping/stopped, the task is failed
properly (`MarkAsFailed` + `OnComplete` + tracking cleanup). Duplicate-ID re-queue
failures (external re-submission won the race) log a warning and clean tracking.

### 5. `InitTaskQueueSlave` leaked a Redis connection
The temporary manager used for config-loading opened a Redis client that was never
closed (one leaked connection per slave boot). Fix: explicit close before the real
manager takes over.

### 6. Heartbeat key lingered after clean shutdown
`stopHeartbeat()` (which deletes the key) existed but was never called; a cleanly
stopped instance looked alive for up to 30s. Fix: wired into `Shutdown` alongside
registry deregistration.

### 7. Fairness valve never engaged for a persistent waiter
On every failed acquire the instance re-`ZADD`ed itself into the waiting set with a
fresh timestamp — resetting its own wait-age each retry, so it never crossed the 5ms
starvation threshold from other instances' perspective and nobody ever yielded.
Fix: `ZADDNX` — the first rejection timestamp survives until the slot is acquired
(`ZREM` on success), making wait-age real.

### 8. Dispatcher dead code removed
The `serverBatch` fast-path in `providerDispatcher` could never execute (the batch
was never filled) and its task-pop path leaked `TaskWithPriority` objects from the
pool had it ever run. Deleted; the live path is unchanged. Also collapsed a duplicate
`url.Parse` per task (processTask normalized, then `getServerSemaphore` re-parsed the
same string).

## OPEN (documented, intentionally unchanged)

### A. Commands run *after* tasks, not before
`ExecuteCommand` uses negative priorities; historical docs claimed commands preempt
tasks. Soulkyn has no callers; realigning the docs (done) was safer than flipping
runtime behavior. The legacy `CommandQueue` ring buffer has no public producer and is
retained only for API stability.

### B. DelTask blind spot during concurrency backoff
A task sleeping in slot-rejection backoff (≤ ~1.2s) is neither queued nor running;
`DelTask` reports `DelTaskNotFound` yet the task will still execute. Callers needing
hard cancellation must gate inside `Handle` (Soulkyn's `InteruptedDontRequeueEver`).
Changing this requires a third task state and touches the hot path — not worth it at
current cancel volumes.

### C. Timeout doesn't kill the handler / slot released early
At timeout the slot and server return to the pool while the abandoned handler may
still be talking to the endpoint — transient over-admission beyond the configured
limit. Mitigate with real HTTP client timeouts inside providers (see OPERATIONS.md).
A hard fix means killable handlers (context enforcement inside `Handle`), which is a
provider-side contract, not a scheduler-side one.

### D. Failure retries are immediate (no backoff between attempts)
Only slot rejections back off. With small `GetMaxRetries()` values this is
acceptable; adding retry delay would change timing behavior under load and needs its
own design (delayed re-queue infrastructure).

### E. Master limit changes don't republish config
`SetTaskManagerServerMaxParallel` on the master updates `smt:slots:<url>:max`
(effective cluster-wide immediately) but not the `smt:config:data` blob, so slaves'
"master-controlled servers" view stays init-time stale. Convention: run identical
`Set…` blocks on every instance after init (what Soulkyn does).

### F. No queue-depth/metrics API
Queue sizes are internal (`taskCount` atomics). Exposing a read-only stats snapshot
would be a nice, safe addition — parked in the improvement backlog, needs an API
decision (new method = new surface to freeze).

### H. Distributed acquire costs 2–3 Redis round-trips (parked optimization)
Fairness check (`ZRANGEWITHSCORES`) + acquire (`EVAL`) + waiting-set upkeep
(`ZREM`/`ZADDNX`) could fold into a single Lua script (~1 RTT per task, using Redis
server time for wait scores). Deliberately NOT done: at AI-task durations (seconds to
minutes) the saving is invisible, and slot accounting is the single most
prod-critical mechanism in the library — a subtle script bug is a production
incident. Revisit only if task rates grow ~100× or Redis moves off-LAN.

### G. Priority ties are approximate-FIFO
Heap-index tiebreak, not strict insertion order. Fine for production semantics
(equal-priority tasks are interchangeable); documented so nobody relies on strict
ordering.
