package smt

import (
	"fmt"
)

// UpdateProviderServers replaces a provider's server pool at RUNTIME — no
// restart, no channel swap, in-flight tasks unaffected. Built 2026-07-13 for
// portunus rolling updates: a fleet controller publishes new pod URLs and the
// running process re-points its routing pool.
//
// How it preserves the pool invariant (exactly ONE token per member server,
// either idle in the channel or held by a running task/command):
//
//  1. The new membership set is published FIRST — from that instant,
//     returnServerToPool drops tokens of servers that left the fleet.
//  2. Idle tokens are drained from the channel (non-blocking).
//  3. Tokens are re-seeded for: servers NEW to the pool (never had a token)
//     plus drained servers that are still members. A still-member server
//     whose token is currently held in flight gets NOTHING here — its own
//     token returns through the (passing) membership filter when it's done.
//  4. Servers that left: their idle tokens died in the drain, their in-flight
//     tokens die at return. Tasks already running on them finish normally —
//     rolling updates keep old pods alive through the drain window anyway.
//
// The availableServers channel is NEVER swapped: the provider dispatcher
// blocks on it, and a swapped channel would strand a dispatcher holding a
// popped task forever. Growth is bounded by the channel capacity instead
// (init floors it at 32) — an update needing more errors out loudly.
//
// Constraint (documented, not enforced): re-adding a URL that was REMOVED
// while one of its tasks was still in flight can double its token (the
// re-add seeds one, the late return passes the filter and adds another).
// With unique-per-pod URLs — the portunus naming/proxy contract, where a
// retired pod's URL never comes back — this cannot occur.
//
// Concurrency limits: brand-new servers get the default limit of 1 (init
// parity) via the normal semaphore path; call SetTaskManagerServerMaxParallel
// after this to raise them. Existing limits are untouched.
//
// Distributed note: this updates the LOCAL instance only. Master and slaves
// that should follow a fleet change must each call it (e.g. each process's
// own registry ticker) — pool membership is deliberately not pushed through
// the Redis config channel.
func (tm *TaskManagerSimple) UpdateProviderServers(providerName string, servers []string) error {
	pd, ok := tm.providers[providerName] // map is init-written, read-only after Start
	if !ok {
		return fmt.Errorf("provider '%s' not found", providerName)
	}

	// Dedupe, order-preserving (duplicate URLs would mint duplicate tokens).
	deduped := make([]string, 0, len(servers))
	seen := make(map[string]struct{}, len(servers))
	for _, s := range servers {
		if _, dup := seen[s]; dup || s == "" {
			continue
		}
		seen[s] = struct{}{}
		deduped = append(deduped, s)
	}

	pd.serversUpdateMu.Lock()
	defer pd.serversUpdateMu.Unlock()

	// Same headroom rule as init (cap = len*2): refuse a fleet the channel
	// can't hold — the channel can't be swapped (dispatcher blocks on it).
	if len(deduped)*2 > cap(pd.availableServers) {
		return fmt.Errorf("provider '%s': %d servers exceed pool capacity %d/2 — restart to grow beyond this",
			providerName, len(deduped), cap(pd.availableServers))
	}

	oldSet := make(map[string]struct{}, len(pd.servers))
	for _, s := range pd.servers {
		oldSet[s] = struct{}{}
	}
	newSet := make(map[string]struct{}, len(deduped))
	for _, s := range deduped {
		newSet[s] = struct{}{}
	}

	// (1) Publish membership BEFORE draining — stale returns start dying now,
	// so nothing re-enters between the drain and the re-seed.
	pd.serverMembers.Store(&newSet)

	// (2) Drain idle tokens.
	drained := make(map[string]int)
drainLoop:
	for {
		select {
		case s := <-pd.availableServers:
			drained[s]++
		default:
			break drainLoop
		}
	}

	// (3) Re-seed. Extra drained duplicates (shouldn't exist) are dropped —
	// the pool self-heals to one token per server.
	added, kept := 0, 0
	for _, s := range deduped {
		_, wasMember := oldSet[s]
		switch {
		case !wasMember:
			pd.availableServers <- s
			added++
			// Init parity: unknown servers get the default concurrency
			// limit of 1 (prefix-matched against existing limits first).
			tm.getServerSemaphore(s)
		case drained[s] > 0:
			pd.availableServers <- s
			kept++
		}
		// wasMember && not drained → token is in flight; it returns on its
		// own through the membership filter.
	}
	removed := 0
	for s := range oldSet {
		if _, still := newSet[s]; !still {
			removed++
		}
	}

	pd.servers = append([]string(nil), deduped...)

	tm.logger.Info().
		Str("provider", providerName).
		Int("servers", len(deduped)).
		Int("added", added).
		Int("removed", removed).
		Int("keptIdle", kept).
		Str("instance", tm.instanceName).
		Msg("[tms] Provider server pool updated live")
	return nil
}
