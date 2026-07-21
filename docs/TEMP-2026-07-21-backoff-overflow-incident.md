# TEMP — 2026-07-21 backoff overflow incident + v0.0.57 changes

Temporary field note. Delete (or fold into KNOWN-ISSUES.md) once v0.0.57 has run
clean in prod for a while. Written same-day as the fix so a regression can be
debugged from THIS file alone.

## What happened (prod, sk-api, caught by the NoQueue flight recorder)

`calculateConcurrencyBackoff` used float math: `Duration(float64(5ms) * Pow(2, retries))`.
At **retries ≥ 41** the product exceeds int64:

1. float→Duration conversion pins to **minInt64** (negative, amd64 CVTTSD2SI).
2. Negative skips the `> 800ms` cap check (it's not "greater").
3. `finalDelay = minInt64 + jitter(minInt64×[0..0.5])` **underflows int64 and
   wraps POSITIVE** → `time.Sleep(4.6e18..9.2e18 ns)` = **150-290 YEARS**.
4. The task had already been `delTaskInQueue`'d before the sleep → it never
   requeues, never MarkAsFailed, never OnComplete. **Silent permanent task loss**
   (user-facing: a generation that never arrives, no error anywhere).

Evidence (flight-recorder dump `sk_suicide_1784647218.txt`): goroutines in
`[sleep, 87..544 minutes]` at the redis-slot backoff line, sleep args
6.77e18 / 7.51e18 / 8.01e18 ns — exactly the wrapped-minInt64 jitter spread for
j = 0.27 / 0.19 / 0.13. All victims were `soulkyn-image-chat` tasks whose server
had been slot-saturated long enough for 41 consecutive misses (~1-2 min at the
capped ~1.2s backoff cycle).

## v0.0.57 changes (commits 0463fc3 + 550ce7a)

1. **Overflow fix**: integer shift with bounded exponent (`baseDelay << retries`,
   exponent clamped <8; ≥8 returns the 800ms cap directly). No float math, no
   overflow possible. Cap + 0-50% jitter behavior unchanged.
2. **Slot starvation bound** (Flo's spec: keep trying at capped backoff, but
   eventually fail): after **`MaxConcurrencySlotRetries` (default 100)** total
   slot misses for one task, the task is failed FOR REAL —
   `MarkAsFailed + OnComplete + cleanupTaskTracking` — so the caller's own
   retry/failure layer owns it. Both redis and local slot paths. At ~1.2s+dispatch
   per cycle that is roughly 2-4 minutes of genuine trying.

## If something looks wrong after deploy — check in this order

- **Symptom: tasks failing with** `gave up on server '…': no concurrency slot
  after 100 attempts` (log: `[tms|processTask] Slot starvation limit reached`).
  That is the NEW bound firing = the server was genuinely saturated for 2-4 min.
  It is NOT a regression — before v0.0.57 that same task would have either
  looped forever or vanished into a century sleep. Response: fix capacity /
  raise the per-server max, or raise `smt.MaxConcurrencySlotRetries` (exported
  var, settable from sk at boot) if 2-4 min is too aggressive for that fleet.
- **Symptom: suspicion the sleeper bug is back.** Grep a goroutine dump for
  `task_manager_simple.go` + `[sleep,` with minutes — any single sleep > seconds
  at the backoff line is a regression (impossible with integer math, so suspect
  version drift: check the deployed go.sum h1 for smt against the tag).
- **Symptom: image/tts task "stuck forever" with NO starvation log line.**
  Not this mechanism — check the flight-recorder dump for where it actually
  waits (`/tmp/sk_suicide_*.txt` on the sk-api box if a suicide fired).
- Rollback: pin go.mod to v0.0.56 — but note v0.0.56 CONTAINS the century-sleep
  bug; only roll back for a regression strictly worse than silent task loss.

## Related, NOT in smt (context for the same night)

- sk-side: dead verda clusters removed from StartSTM/StartSTMAPI limits
  (sk-images → portunus sdxl registry; vllm-rtx main-text → portunus 'main').
  zimage stays on verda (credit burn-down) at 12 across ?s slots.
- The NoQueue suicide (>15 text timeouts/min → os.Exit(42)) is sk-side
  (task_queues.go), unrelated to smt — it fired from evening-peak text
  saturation. Design discussion (per-provider breaker instead of process kill)
  parked for later.
