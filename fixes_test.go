package smt

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

// TestDelTaskRunningTwice: calling DelTask repeatedly for the same running task
// must be idempotent. Before the fix, the second call panicked
// (close of closed channel) while holding runningTasksMu — a recovered panic left
// the mutex locked forever and froze the entire manager.
func TestDelTaskRunningTwice(t *testing.T) {
	logger := zerolog.Nop()

	handleStarted := make(chan struct{}, 2) // buffered for both tasks in this test
	handleContinue := make(chan struct{})

	provider := &MockProvider{
		name: "test",
		handleFunc: func(task ITask, server string) error {
			handleStarted <- struct{}{}
			<-handleContinue
			return nil
		},
	}
	providers := []IProvider{provider}
	servers := map[string][]string{"test": {"server1"}}

	tm := NewTaskManagerSimple(&providers, servers, &logger, func(string, string) time.Duration {
		return 30 * time.Second
	})
	tm.Start()
	defer tm.Shutdown()

	task := &MockTask{id: uuid.New().String(), provider: provider, priority: 1, createdAt: time.Now()}
	if !tm.AddTask(task) {
		t.Fatal("Failed to add task")
	}
	<-handleStarted

	interruptCalls := 0
	interruptFn := func(task ITask, server string) error {
		interruptCalls++
		return nil
	}

	// Three cancels for the same running task (double-click + retry scenario)
	for i := 1; i <= 3; i++ {
		result := tm.DelTask(task.GetID(), interruptFn)
		if result != DelTaskInterruptedRunning {
			t.Errorf("call %d: expected DelTaskInterruptedRunning, got %s", i, result)
		}
	}
	if interruptCalls != 3 {
		t.Errorf("expected interruptFn to run on every call, got %d", interruptCalls)
	}

	// Let the handler finish and prove the manager still works: runningTasksMu must
	// not be stuck (registerRunningTask would block forever if it were).
	close(handleContinue)

	// task2's Handle sends into the buffered handleStarted, then returns instantly
	// because handleContinue is already closed.
	task2 := &MockTask{
		id: uuid.New().String(), provider: provider, priority: 1,
		createdAt: time.Now(), done: make(chan struct{}),
	}
	if !tm.AddTask(task2) {
		t.Fatal("Failed to add task2 — manager wedged after double DelTask?")
	}

	select {
	case <-task2.done:
		// manager alive and completing tasks
	case <-time.After(5 * time.Second):
		t.Fatal("task2 never completed — manager frozen after repeated DelTask")
	}
}

// TestRequeueOrFailDuringShutdown: a task whose re-queue fails because the manager
// is stopping must be marked failed + completed, not silently dropped.
func TestRequeueOrFailDuringShutdown(t *testing.T) {
	logger := zerolog.Nop()

	provider := &MockProvider{name: "test"}
	providers := []IProvider{provider}
	servers := map[string][]string{"test": {"server1"}}

	tm := NewTaskManagerSimple(&providers, servers, &logger, func(string, string) time.Duration {
		return 5 * time.Second
	})
	tm.Start()
	tm.Shutdown() // manager now stopped: AddTask will refuse

	task := &MockTask{
		id: uuid.New().String(), provider: provider, priority: 1,
		createdAt: time.Now(), done: make(chan struct{}),
	}

	tm.requeueOrFail(task, "test scenario")

	if !task.failed {
		t.Error("task should have been marked failed when re-queue is impossible")
	}
	if !task.completeCalled {
		t.Error("OnComplete should have been called (lifecycle must terminate)")
	}
}

// TestRequeueOrFailDuplicate: if the same task ID re-entered the queue concurrently,
// requeueOrFail must NOT fail this instance (the queued one owns the lifecycle).
func TestRequeueOrFailDuplicate(t *testing.T) {
	logger := zerolog.Nop()

	// Provider whose handler blocks so the queued duplicate never completes during the test
	blocker := make(chan struct{})
	defer close(blocker)
	provider := &MockProvider{
		name: "test",
		handleFunc: func(task ITask, server string) error {
			<-blocker
			return nil
		},
	}
	providers := []IProvider{provider}
	servers := map[string][]string{"test": {"server1"}}

	tm := NewTaskManagerSimple(&providers, servers, &logger, func(string, string) time.Duration {
		return 30 * time.Second
	})
	tm.Start()
	defer tm.Shutdown()

	id := uuid.New().String()
	queued := &MockTask{id: id, provider: provider, priority: 1, createdAt: time.Now()}
	if !tm.AddTask(queued) {
		t.Fatal("failed to queue first instance")
	}

	// Same ID, different object — simulates external re-submission winning the race
	dup := &MockTask{id: id, provider: provider, priority: 1, createdAt: time.Now()}
	tm.requeueOrFail(dup, "test duplicate")

	if dup.failed {
		t.Error("duplicate hand-off must not be marked failed while a queued instance owns the ID")
	}
	if dup.completeCalled {
		t.Error("OnComplete must not fire for the yielded duplicate")
	}
}

// TestDeadInstanceDetection exercises the DETECTION path (registry walk), not just
// cleanupDeadInstance. Before the fix, detection scanned for heartbeat keys with
// expired TTLs — impossible to observe (expired keys vanish) — so crashed instances
// were never detected.
func TestDeadInstanceDetection(t *testing.T) {
	redisAddr := "localhost:6379"
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr, DB: 15})
	defer redisClient.Close()

	ctx := context.Background()
	if err := redisClient.FlushDB(ctx).Err(); err != nil {
		t.Skipf("Redis not available at %s: %v", redisAddr, err)
	}

	logger := zerolog.Nop()

	provider := &MockProviderDistributed{name: "provider1", processTime: 10 * time.Millisecond}
	providers := []IProvider{provider}
	servers := map[string][]string{"provider1": {"https://shared-server.com"}}

	err := InitTaskQueueManagerMaster(ConfigOptions{
		RedisAddr:    redisAddr,
		RedisDB:      15,
		InstanceName: "master-di",
	}, &logger, &providers, nil, servers, func(string, string) time.Duration { return 5 * time.Second })
	if err != nil {
		t.Fatalf("Failed to init master: %v", err)
	}
	masterTM := GetTaskQueueManagerInstance()
	defer masterTM.Shutdown()

	masterTM.SetTaskManagerServerMaxParallel("https://shared-server.com", 5)

	// Zombie instance: registered, holds 3 slots, heartbeat key ALREADY EXPIRED
	// (we simply never create it — same observable state as post-TTL).
	zombieClient := redis.NewClient(&redis.Options{Addr: redisAddr, DB: 15})
	defer zombieClient.Close()
	coordinator := NewRedisSlotCoordinator(zombieClient, "zombie-di", &logger)
	for i := 0; i < 3; i++ {
		if !coordinator.AcquireSlot(ctx, "https://shared-server.com") {
			t.Fatalf("zombie failed to acquire slot %d", i+1)
		}
	}
	if err := redisClient.SAdd(ctx, instanceRegistryKey, "zombie-di").Err(); err != nil {
		t.Fatalf("failed to register zombie: %v", err)
	}

	acquiredKey := "smt:slots:https://shared-server.com:acquired"
	if acquired, _ := redisClient.Get(ctx, acquiredKey).Int(); acquired != 3 {
		t.Fatalf("expected 3 acquired slots, got %d", acquired)
	}

	// One detection pass — the monitor's 30s tick, invoked directly
	masterTM.checkDeadInstancesOnce()

	if acquired, _ := redisClient.Get(ctx, acquiredKey).Int(); acquired != 0 {
		t.Errorf("expected 0 acquired slots after detection+cleanup, got %d", acquired)
	}
	holdersKey := "smt:slots:https://shared-server.com:holders"
	if _, err := redisClient.HGet(ctx, holdersKey, "zombie-di").Int(); err != redis.Nil {
		t.Error("zombie should have been removed from holders")
	}
	isMember, _ := redisClient.SIsMember(ctx, instanceRegistryKey, "zombie-di").Result()
	if isMember {
		t.Error("zombie should have been deregistered after cleanup")
	}

	// The live master must NOT have been cleaned (it has a heartbeat)
	isMember, _ = redisClient.SIsMember(ctx, instanceRegistryKey, "master-di").Result()
	if !isMember {
		t.Error("live master must remain registered")
	}
}

// TestCleanShutdownRemovesHeartbeat: a cleanly stopped instance must not linger as
// "alive" (heartbeat key) nor as a future false-positive "dead" (registry entry).
func TestCleanShutdownRemovesHeartbeat(t *testing.T) {
	redisAddr := "localhost:6379"
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr, DB: 15})
	defer redisClient.Close()

	ctx := context.Background()
	if err := redisClient.FlushDB(ctx).Err(); err != nil {
		t.Skipf("Redis not available at %s: %v", redisAddr, err)
	}

	logger := zerolog.Nop()
	provider := &MockProviderDistributed{name: "provider1", processTime: time.Millisecond}
	providers := []IProvider{provider}
	servers := map[string][]string{"provider1": {"https://hb-server.com"}}

	err := InitTaskQueueManagerMaster(ConfigOptions{
		RedisAddr:    redisAddr,
		RedisDB:      15,
		InstanceName: "hb-master",
	}, &logger, &providers, nil, servers, func(string, string) time.Duration { return 5 * time.Second })
	if err != nil {
		t.Fatalf("Failed to init master: %v", err)
	}
	tm := GetTaskQueueManagerInstance()

	// Wait for the initial heartbeat (sent by the heartbeat goroutine immediately)
	heartbeatKey := "smt:instances:hb-master:heartbeat"
	deadline := time.Now().Add(3 * time.Second)
	for {
		if n, _ := redisClient.Exists(ctx, heartbeatKey).Result(); n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("initial heartbeat never appeared")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ok, _ := redisClient.SIsMember(ctx, instanceRegistryKey, "hb-master").Result(); !ok {
		t.Fatal("instance should be registered after first heartbeat")
	}

	tm.Shutdown()

	if n, _ := redisClient.Exists(ctx, heartbeatKey).Result(); n != 0 {
		t.Error("heartbeat key must be deleted on clean shutdown")
	}
	if ok, _ := redisClient.SIsMember(ctx, instanceRegistryKey, "hb-master").Result(); ok {
		t.Error("registry entry must be removed on clean shutdown")
	}
}
