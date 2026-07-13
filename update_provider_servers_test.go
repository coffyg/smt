package smt

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// upsTask builds a MockTask wired to a provider (test-local helper).
func upsTask(id string, provider IProvider) *MockTask {
	return &MockTask{
		id:         id,
		priority:   1,
		maxRetries: 1,
		provider:   provider,
		timeout:    10 * time.Second,
		done:       make(chan struct{}),
	}
}

// drainPool empties a provider's availableServers channel and returns the
// idle tokens (test inspection only).
func drainPool(pd *ProviderData) []string {
	out := []string{}
	for {
		select {
		case s := <-pd.availableServers:
			out = append(out, s)
		default:
			return out
		}
	}
}

// TestUpdateProviderServers_LiveSwap: [A] → [B] while a task is IN FLIGHT on
// A. The in-flight task finishes normally, its A-token dies at the return
// filter, and every subsequent task runs on B. Pool ends with exactly one
// token: B.
func TestUpdateProviderServers_LiveSwap(t *testing.T) {
	logger := zerolog.Nop()
	providerName := "swapProvider"
	provider := &MockProvider{name: providerName}
	providers := []IProvider{provider}
	servers := map[string][]string{providerName: {"serverA"}}

	var mu sync.Mutex
	served := map[string][]string{} // taskID -> servers it ran on
	var inFlight sync.WaitGroup
	block := make(chan struct{})

	provider.handleFunc = func(task ITask, server string) error {
		mu.Lock()
		served[task.GetID()] = append(served[task.GetID()], server)
		mu.Unlock()
		if task.GetID() == "task-inflight" {
			<-block // hold serverA's token until the swap happened
			inFlight.Done()
		}
		return nil
	}

	tm := NewTaskManagerSimple(&providers, servers, &logger, func(string, string) time.Duration { return 10 * time.Second })
	tm.Start()
	defer tm.Shutdown()

	// Occupy serverA.
	inFlight.Add(1)
	tm.AddTask(upsTask("task-inflight", provider))
	waitFor(t, "task-inflight to start", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(served["task-inflight"]) == 1
	})

	// Swap the pool while A's token is held.
	if err := tm.UpdateProviderServers(providerName, []string{"serverB"}); err != nil {
		t.Fatalf("UpdateProviderServers: %v", err)
	}

	// New task must run on B (A is gone; B was seeded).
	tm.AddTask(upsTask("task-after-1", provider))
	waitFor(t, "task-after-1 on serverB", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(served["task-after-1"]) == 1
	})

	// Release the in-flight task; its A-token must be dropped on return.
	close(block)
	inFlight.Wait()
	time.Sleep(100 * time.Millisecond) // let returnServerToPool run

	// Another task: still B (never A).
	tm.AddTask(upsTask("task-after-2", provider))
	waitFor(t, "task-after-2 on serverB", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(served["task-after-2"]) == 1
	})

	mu.Lock()
	for _, id := range []string{"task-after-1", "task-after-2"} {
		if len(served[id]) != 1 || served[id][0] != "serverB" {
			t.Errorf("%s ran on %v, want [serverB]", id, served[id])
		}
	}
	mu.Unlock()

	// Invariant: exactly ONE idle token, and it's B.
	waitFor(t, "pool to settle to one B token", func() bool {
		pd := tm.providers[providerName]
		tokens := drainPool(pd)
		ok := len(tokens) == 1 && tokens[0] == "serverB"
		for _, s := range tokens { // put them back regardless
			pd.availableServers <- s
		}
		return ok
	})
}

// TestUpdateProviderServers_GrowAndDistribute: [A] grows to [A,B,C] while A
// sits idle — the drained A token re-enters, B and C get fresh tokens, and 3
// concurrent tasks land on 3 distinct servers.
func TestUpdateProviderServers_GrowAndDistribute(t *testing.T) {
	logger := zerolog.Nop()
	providerName := "growProvider"
	provider := &MockProvider{name: providerName}
	providers := []IProvider{provider}
	servers := map[string][]string{providerName: {"srv1"}}

	var mu sync.Mutex
	got := map[string]int{}
	var running int32
	release := make(chan struct{})

	provider.handleFunc = func(task ITask, server string) error {
		mu.Lock()
		got[server]++
		mu.Unlock()
		atomic.AddInt32(&running, 1)
		<-release // hold so all three must use distinct tokens
		return nil
	}

	tm := NewTaskManagerSimple(&providers, servers, &logger, func(string, string) time.Duration { return 10 * time.Second })
	tm.Start()
	defer tm.Shutdown()

	if err := tm.UpdateProviderServers(providerName, []string{"srv1", "srv2", "srv3"}); err != nil {
		t.Fatalf("UpdateProviderServers: %v", err)
	}

	for i := 0; i < 3; i++ {
		tm.AddTask(upsTask(fmt.Sprintf("grow-%d", i), provider))
	}
	waitFor(t, "3 tasks running concurrently", func() bool {
		return atomic.LoadInt32(&running) == 3
	})
	close(release)

	mu.Lock()
	defer mu.Unlock()
	for _, s := range []string{"srv1", "srv2", "srv3"} {
		if got[s] != 1 {
			t.Errorf("server %s handled %d tasks, want exactly 1 (got map: %v)", s, got[s], got)
		}
	}
}

// TestUpdateProviderServers_Errors: unknown provider + over-capacity both
// error out and leave the pool untouched.
func TestUpdateProviderServers_Errors(t *testing.T) {
	logger := zerolog.Nop()
	providerName := "errProvider"
	provider := &MockProvider{name: providerName}
	providers := []IProvider{provider}
	servers := map[string][]string{providerName: {"srvE"}}

	tm := NewTaskManagerSimple(&providers, servers, &logger, func(string, string) time.Duration { return 10 * time.Second })
	tm.Start()
	defer tm.Shutdown()

	if err := tm.UpdateProviderServers("nope", []string{"x"}); err == nil {
		t.Error("unknown provider: want error, got nil")
	}

	// cap floor is 32 → 17 servers * 2 = 34 > 32 must refuse.
	big := make([]string, 17)
	for i := range big {
		big[i] = fmt.Sprintf("bulk-%d", i)
	}
	if err := tm.UpdateProviderServers(providerName, big); err == nil {
		t.Error("over-capacity update: want error, got nil")
	}

	// Pool untouched: srvE still serves.
	var mu sync.Mutex
	var lastServer string
	provider.handleFunc = func(task ITask, server string) error {
		mu.Lock()
		lastServer = server
		mu.Unlock()
		return nil
	}
	tm.AddTask(upsTask("err-check", provider))
	waitFor(t, "task on original server", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return lastServer == "srvE"
	})
}

// waitFor polls cond up to 5s; fails the test on timeout.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
