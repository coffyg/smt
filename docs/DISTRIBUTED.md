# SMT Distributed Mode

Multiple SMT processes coordinate per-server concurrency budgets through Redis.
Each process runs its own full scheduler (queues, dispatchers, server pools); **only
the concurrency slots are shared**. Tasks are not distributed between instances —
whoever `AddTask`s a task executes it.

## Roles

- **Master** (`InitTaskQueueManagerMaster`): holds `smt:master:lock`, publishes the
  provider/server/limit config to Redis, renews the lock every 5s (TTL 10s).
  Exactly one master may run; a second master init fails with
  `another master is running` (unless the previous holder had the same
  `InstanceName` — startup self-cleanup removes a ghost lock from a crashed run).
- **Slave** (`InitTaskQueueSlave`): loads the master's config from Redis if present
  (matching providers by name — providers the slave doesn't implement locally are
  skipped), otherwise falls back to its local config. Subscribes to config updates.
- Both send heartbeats and participate in slot coordination identically.

Master death is **not failed over**: slaves keep working with their current config
and slots, but config publication stops until a master returns. In Soulkyn the master
is the main API process under systemd, so "master returns" is a restart away.

## Redis key schema (all keys created by SMT)

| Key | Type | Purpose |
|---|---|---|
| `smt:master:lock` | string (instance name), TTL 10s | master election guard |
| `smt:config:data` | string (JSON `SerializedConfig`) | providers, servers, limits, version |
| `smt:config:updates` | pub/sub channel | "config_updated" pings for hot-reload |
| `smt:instances:<name>:heartbeat` | string, TTL 30s, refreshed every 15s | liveness |
| `smt:instances:registry` | set of instance names | dead-instance detection (see below) |
| `smt:slots:<url>:max` | string int | concurrency budget for the normalized URL |
| `smt:slots:<url>:acquired` | string int | currently held slots (all instances) |
| `smt:slots:<url>:holders` | hash instance → count | who holds how many |
| `smt:slots:<url>:waiting` | zset instance → unix ts, TTL 5min | starvation fairness |

`<url>` is the query- and fragment-stripped server URL. Keys contain `:` inside the
URL; parsing splits on the fixed `smt:slots:` / `:holders` frame, not naive `:`.

## Slot acquisition path (per task, hot path)

1. **Fairness pre-check** — if another instance has been in the waiting zset longer
   than 5ms and is not us, sleep 10ms before trying (gives the starved instance a
   window). Costs one `ZRANGEWITHSCORES` round-trip.
2. **Lua acquire** — atomically: read `max`, read `acquired`; if `acquired < max`,
   `INCR acquired`, `HINCRBY holders <me> 1`, return success. If no `max` key exists
   the acquire is treated as unlimited-allowed (warn logged).
3. On success: `ZREM` ourselves from waiting. On rejection: `ZADD` ourselves into
   waiting, return the server to the pool, exponential-backoff sleep
   (5ms·2ⁿ + jitter, cap 800ms), re-queue the task. Slot rejections do **not**
   consume task retries.
4. Release is the mirrored Lua script (`DECR` + `HINCRBY -1`, holder entry removed
   at zero).

Failure semantics: **if Redis is unreachable, acquire returns false** and the task
re-queues with backoff indefinitely — task processing effectively pauses until Redis
returns. There is no automatic fallback to local semaphores (deliberate: local
fallback would over-admit against servers shared with healthy instances).

Local semaphores (`serverConcurrencyMap`) are maintained but **not consulted** for
task admission in distributed mode; they serve as the source of limit values
published to Redis.

## Liveness and cleanup

Three complementary mechanisms return slots that would otherwise leak:

1. **Startup self-cleanup** (`cleanupGhostInstance`): on init, an instance deletes
   its own stale master lock/heartbeat and releases every slot Redis still attributes
   to its `InstanceName`. This is why **fixed instance names matter**: a crashed
   `sk-api-web` cleans itself the moment systemd restarts it. With auto-generated
   `hostname-PID` names the new PID never matches, and this mechanism does nothing.
2. **Dead-instance monitor** (every 30s, all instances): reads
   `smt:instances:registry`, and for each registered name with no live heartbeat key,
   releases all slots attributed to that name and unregisters it. (Historical note:
   before 2026-07 this scanned `smt:instances:*:heartbeat` — a pattern that can only
   match *live* keys, since dead ones expire and vanish; detection therefore never
   fired and cleanup was carried entirely by mechanisms 1 and 3. The registry set
   fixes that.)
3. **Ghost-lock sweep** (same 30s tick): any `smt:slots:<url>` with `acquired > 0`
   but an empty holders hash is reset to 0 and its waiting zset cleared. This is the
   backstop for historical TTL bugs and manual key surgery.

Clean shutdown releases everything explicitly (slots via `ReleaseAllSlots`, heartbeat
key deleted, registry entry removed, master lock deleted if held).

## Config hot-reload

The master's `publishConfig` writes `smt:config:data` and publishes on
`smt:config:updates`. Slaves reload the blob, extend their master-controlled-server
set, and swap local semaphore capacities for servers they know.

Two operational caveats:

- `publishConfig` runs **once, during master init**, with whatever limits exist at
  that moment (the per-server defaults of 1). Later
  `SetTaskManagerServerMaxParallel` calls on the master update the Redis `max` keys
  directly (which is what slot acquisition reads — so limits DO take effect
  cluster-wide) but do **not** republish the config blob. Slaves' notion of
  "master-controlled servers" is therefore the init-time server list.
- Hot-reload swaps local semaphores by replacing the channel; slots held in the old
  channel are released into the old channel. Irrelevant for task admission in
  distributed mode (Redis governs), cosmetic for the local map.

## Deployment pattern (how Soulkyn runs it)

- `sk-main` — master, fixed name, systemd.
- `sk-api-<type>`, `sk-crons` — slaves, fixed names.
- All instances run the **same** `SetTaskManagerServerMaxParallel` block right after
  init, with exact normalized URLs. That makes every instance agree on limits without
  relying on config propagation, and the last writer wins in Redis with identical
  values (idempotent).
- Server pools come from Postgres (`?s=N` multiplication — see `ARCHITECTURE.md`),
  so every instance builds the same pool.
- All processes point at the same Redis DB (14 in prod, 15 in this repo's tests —
  tests `FlushDB` their database; never point tests at the prod DB).

## Fairness model

Slot competition is first-come-first-served with a starvation valve, not a fair
scheduler: an instance that fails an acquire registers in the waiting zset (`ZADDNX`
— the score is the FIRST rejection time and survives retries, so wait-age is real);
other instances yield 10ms before their *next* acquire attempt on that server if the
oldest waiter has waited >5ms. The entry is removed on successful acquire. Under
sustained saturation, throughput distributes roughly by attempt rate; the valve only
prevents total starvation of a quiet instance.
