# SMT Operations Guide

What breaks, what it looks like, what to do. Written against the codebase as of
2026-07-02, with Soulkyn's ~500k tasks/day deployment as the reference workload.

## Sizing intuition

The local engine benches at >1000 tasks/second with 35ms tasks on a workstation
(see `benchmark_results/`). At 500k tasks/day (~6/s average, low-hundreds/s burst),
scheduler overhead is noise; the real budgets are the concurrency limits you set and,
in distributed mode, 2–4 Redis round-trips per task execution. Keep Redis close
(same host/LAN): 1ms RTT adds ~3ms per task, 50ms RTT adds ~150ms per task.

## Failure modes

### Redis down (distributed mode)
- Slot acquires fail → every task re-queues with capped backoff (≤ ~1.2s per cycle)
  and **nothing executes** until Redis returns. Queues grow in memory.
- Heartbeats and lock renewal fail (errors logged); after Redis returns, instances
  resume automatically. The master lock may briefly be absent — nothing contests it,
  the running master just re-renews.
- Symptom in logs: `[tms|redis-slots] Failed to acquire slot` spam + throughput → 0.
- Action: restore Redis; no SMT restart needed.

### An instance crashes (OOM, kill -9, panic)
- Its held slots stay counted in `smt:slots:*` until one of:
  1. the same `InstanceName` restarts (immediate self-cleanup at init),
  2. the dead-instance monitor notices the expired heartbeat via the instance
     registry (≤ ~60s: 30s heartbeat TTL + 30s monitor tick),
  3. never — if the name was auto-generated (hostname-PID) *and* the deployment slot
    is gone. Fix by hand (below) or let the ghost sweep help if holders emptied.
- Meanwhile the affected servers run under-capacity (leaked slots occupy budget).
- Action: normally none — systemd restart covers it. For a retired instance name,
  delete its holders entries and decrement `acquired` accordingly, or just
  `DEL smt:slots:<url>:acquired smt:slots:<url>:holders` during a quiet moment
  (limits re-assert on next acquire; ghost sweep clears the waiting set).

### Master down
- Slaves keep executing with current limits. Config publication and (only the
  master's copies of) monitors stop; slot coordination is unaffected.
- Action: restart the master process. Nothing else drifts in the interim.

### One endpoint is slow or dead
- Its tasks time out (per `getTimeout`), retry immediately up to `GetMaxRetries()`,
  then fail terminally. Timeout does not kill in-flight HTTP calls — an endpoint
  that hangs rather than errors can accumulate orphaned handler goroutines for as
  long as your HTTP client's own timeout allows. Give provider HTTP clients real
  timeouts; don't rely on SMT's deadline alone.
- The dispatcher for that provider keeps cycling (server checkout returns at
  timeout), so other servers of the same provider continue working.

### Queue grows without bound
- SMT has no queue cap and no backpressure signal. `AddTask` accepts everything
  (memory permitting). Watch process RSS and your own task-age metrics; SMT does not
  expose queue-depth metrics (grep debug logs, or query `taskCount` via a fork —
  see IMPROVEMENTS backlog).

## Redis hygiene

- Prod uses a dedicated logical DB (14). The test suite **flushes** its DB (15) —
  never share a DB between tests and anything you care about.
- All SMT state is reconstructible: limits are re-set at boot by every instance,
  slots re-count as tasks run, heartbeats regenerate. `FLUSHDB` on the SMT database
  during a full-fleet stop is a clean slate, not a disaster.
- Doing `FLUSHDB` while instances run is *mostly* self-healing (max keys re-created
  by the next `SetTaskManagerServerMaxParallel` or config publish; acquired counters
  restart at 0 — transient over-admission until in-flight tasks release into
  fresh counters, which the ghost sweep then corrects).

## Logging

Everything logs through the injected zerolog logger, tag `[tms…]`.

- Production level: **Info or Warn**. Debug is extremely chatty (multiple lines per
  task per stage) and meant for development.
- Error lines that matter:
  - `Task FAILED-TIMEOUT` — endpoint slow/hung or timeout budget too small.
  - `max retries reached or no rows` — terminal task failure (business visibility).
  - `Failed to acquire slot` / `Failed to release slot` — Redis trouble.
  - `Slave ignoring SetTaskManagerServerMaxParallel` — a slave tried to override a
    master-controlled limit; usually means init ordering drifted.
  - `Cleared ghost lock` / `Released slots from dead instance` — cleanup engaged;
    if frequent, some instance is crash-looping.

## Tuning

- **Per-endpoint parallelism**: `SetTaskManagerServerMaxParallel(exactNormalizedURL, N)`
  on every instance, right after init. `N` should match what the endpoint actually
  sustains (vLLM: near its `max_num_seqs`; ComfyUI-style: 1–2 per GPU).
- **Pool multiplicity (`?s=N`)**: bounds how many tasks the dispatcher can have
  *in-flight per endpoint entry* before it blocks. Keep N ≥ the concurrency limit;
  N slightly above the limit lets rejection/backoff smooth bursts.
- **Timeouts**: `getTimeout` is looked up per `(callbackName, providerName)`. Missing
  entries in Soulkyn's tables fall back to 25s — a warn is logged each time; treat
  those warns as config debt.
- **Retries**: remember they're immediate. For endpoints where a failure is likely to
  repeat within milliseconds, keep `GetMaxRetries()` small (2–3) and rely on task
  re-submission at a higher layer.

## Testing

```bash
go test ./...            # full suite; distributed tests need Redis on localhost:6379
                         # (they use DB 15 and FLUSH it), else they skip
go test -race ./...      # the suite is race-clean; keep it that way
./bench.sh               # throughput benchmarks → benchmark_results/
```
