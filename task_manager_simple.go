package smt

import (
	"container/heap"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

// ConfigOptions holds distributed configuration for Redis coordination
type ConfigOptions struct {
	RedisAddr    string // Redis server address (e.g., "localhost:6379")
	RedisPwd     string // Redis password
	RedisDB      int    // Redis database number
	InstanceName string // Unique instance identifier (if empty, uses hostname-PID)
}

// DelTaskResult represents the result of a DelTask operation
type DelTaskResult int

const (
	// DelTaskNotFound indicates the task was not found in queue or running
	DelTaskNotFound DelTaskResult = iota
	// DelTaskRemovedFromQueue indicates the task was successfully removed from queue
	DelTaskRemovedFromQueue
	// DelTaskInterruptedRunning indicates the task was interrupted while running
	DelTaskInterruptedRunning
	// DelTaskErrorNotRunning indicates the task manager is not running
	DelTaskErrorNotRunning
	// DelTaskErrorShuttingDown indicates the task manager is shutting down
	DelTaskErrorShuttingDown
)

// String returns a human-readable description of the DelTaskResult
func (r DelTaskResult) String() string {
	switch r {
	case DelTaskNotFound:
		return "task not found"
	case DelTaskRemovedFromQueue:
		return "removed from queue"
	case DelTaskInterruptedRunning:
		return "interrupted while running"
	case DelTaskErrorNotRunning:
		return "task manager not running"
	case DelTaskErrorShuttingDown:
		return "task manager shutting down"
	default:
		return "unknown result"
	}
}

// RunningTaskInfo holds information about a currently executing task
type RunningTaskInfo struct {
	task         ITask
	interruptFn  func(task ITask, server string) error
	cancelCh     chan struct{}
	providerName string
	server       string
}

// TaskManagerSimple interface (optimized)
type TaskManagerSimple struct {
	providers            map[string]*ProviderData
	taskInQueue          map[string]struct{}
	taskInQueueMu        sync.RWMutex
	runningTasks         map[string]*RunningTaskInfo // Track currently executing tasks
	runningTasksMu       sync.RWMutex
	isRunning            int32
	shutdownRequest      int32
	shutdownCh           chan struct{}
	wg                   sync.WaitGroup
	logger               *zerolog.Logger
	getTimeout           func(string, string) time.Duration
	serverConcurrencyMap map[string]chan struct{} // Map of servers to semaphores
	serverConcurrencyMu  sync.RWMutex
	concurrencyRetryMap  sync.Map // Track concurrency retry attempts per task ID
	taskToProvider       sync.Map // Map taskID (string) -> providerName (string) for O(1) lookup

	// Distributed coordination fields
	redisClient               *redis.Client
	instanceName              string
	isMaster                  bool
	heartbeatStop             chan struct{}
	lockRenewalStop           chan struct{}
	slotCoordinator           *RedisSlotCoordinator
	masterControlledServers   map[string]bool // Servers controlled by Master (slaves can't modify these)
	masterControlledServersMu sync.RWMutex
	configPubSub              *redis.PubSub // Track pubsub for clean shutdown
}

type ProviderData struct {
	taskQueue        TaskQueuePrio
	taskQueueLock    sync.Mutex
	taskQueueCond    *sync.Cond
	servers          []string
	availableServers chan string
	taskCount        int32 // atomic counter for tasks
	commandCount     int32 // atomic counter for commands

	commandQueue   *CommandQueue
	commandSet     map[uuid.UUID]struct{}
	commandSetLock sync.Mutex
}

// ProviderConfig holds serializable provider configuration for Redis
type ProviderConfig struct {
	Name    string   `json:"name"`
	Servers []string `json:"servers"`
}

// SerializedConfig holds the complete configuration for Redis storage
type SerializedConfig struct {
	Providers    []ProviderConfig    `json:"providers"`
	Servers      map[string][]string `json:"servers"`
	ServerLimits map[string]int      `json:"server_limits"` // server URL -> max concurrency
	Version      int64               `json:"version"`
}

// RedisSlotCoordinator handles distributed slot coordination via Redis
type RedisSlotCoordinator struct {
	client       *redis.Client
	instanceName string
	localSlots   sync.Map // map[serverURL]int - tracks slots held by this instance
	logger       *zerolog.Logger
}

// Lua script for atomic slot acquisition
const luaAcquireSlot = `
local max_key = KEYS[1]
local acquired_key = KEYS[2]
local holders_key = KEYS[3]
local instance_name = ARGV[1]

local max = tonumber(redis.call('GET', max_key))
if not max then
    return -1  -- No limit set
end

local acquired = tonumber(redis.call('GET', acquired_key) or "0")
if acquired < max then
    redis.call('INCR', acquired_key)
    redis.call('HINCRBY', holders_key, instance_name, 1)
    -- No TTL on holders - dead instance monitor handles cleanup
    -- TTL caused ghost locks when tasks took >5min (holders expired but acquired stayed)
    return 1  -- Success
end
return 0  -- Limit reached
`

// Lua script for atomic slot release
const luaReleaseSlot = `
local acquired_key = KEYS[1]
local holders_key = KEYS[2]
local instance_name = ARGV[1]

local holder_count = tonumber(redis.call('HGET', holders_key, instance_name) or "0")
if holder_count > 0 then
    redis.call('DECR', acquired_key)
    redis.call('HINCRBY', holders_key, instance_name, -1)

    -- Remove holder if count reaches 0
    if holder_count == 1 then
        redis.call('HDEL', holders_key, instance_name)
    end
    return 1  -- Success
end
return 0  -- Instance didn't hold any slots
`

// NewRedisSlotCoordinator creates a new distributed slot coordinator
func NewRedisSlotCoordinator(client *redis.Client, instanceName string, logger *zerolog.Logger) *RedisSlotCoordinator {
	return &RedisSlotCoordinator{
		client:       client,
		instanceName: instanceName,
		logger:       logger,
	}
}

// AcquireSlot attempts to acquire a slot for the given server
func (rsc *RedisSlotCoordinator) AcquireSlot(ctx context.Context, serverURL string) bool {
	maxKey := fmt.Sprintf("smt:slots:%s:max", serverURL)
	acquiredKey := fmt.Sprintf("smt:slots:%s:acquired", serverURL)
	holdersKey := fmt.Sprintf("smt:slots:%s:holders", serverURL)
	waitingKey := fmt.Sprintf("smt:slots:%s:waiting", serverURL)

	result, err := rsc.client.Eval(ctx, luaAcquireSlot,
		[]string{maxKey, acquiredKey, holdersKey},
		rsc.instanceName).Int()

	if err != nil {
		rsc.logger.Error().
			Err(err).
			Str("server", serverURL).
			Str("instance", rsc.instanceName).
			Msg("[tms|redis-slots] Failed to acquire slot")
		return false
	}

	if result == -1 {
		// No limit set in Redis - shouldn't happen but treat as success
		rsc.logger.Warn().
			Str("server", serverURL).
			Msg("[tms|redis-slots] No max limit found in Redis")
		// Ensure we are not in waiting set
		_ = rsc.client.ZRem(ctx, waitingKey, rsc.instanceName).Err()
		return true
	}

	if result == 1 {
		// Success - track locally for cleanup
		count, _ := rsc.localSlots.LoadOrStore(serverURL, 0)
		rsc.localSlots.Store(serverURL, count.(int)+1)

		// Remove from waiting set since we acquired
		_ = rsc.client.ZRem(ctx, waitingKey, rsc.instanceName).Err()

		rsc.logger.Debug().
			Str("server", serverURL).
			Str("instance", rsc.instanceName).
			Msg("[tms|redis-slots] Acquired slot")
		return true
	}

	// result == 0 means limit reached - add to waiting set
	now := float64(time.Now().UnixNano()) / 1e9
	_ = rsc.client.ZAdd(ctx, waitingKey, redis.Z{
		Score:  now,
		Member: rsc.instanceName,
	}).Err()
	_ = rsc.client.Expire(ctx, waitingKey, 5*time.Minute).Err()
	return false
}

// CheckFairnessBeforeAcquire checks if other instances are starving and yields if needed
func (rsc *RedisSlotCoordinator) CheckFairnessBeforeAcquire(ctx context.Context, serverURL string) {
	waitingKey := fmt.Sprintf("smt:slots:%s:waiting", serverURL)

	// Get oldest waiter (lowest score)
	waiters, err := rsc.client.ZRangeWithScores(ctx, waitingKey, 0, 0).Result()
	if err != nil || len(waiters) == 0 {
		// No one waiting, proceed immediately
		return
	}

	oldestWaiter := waiters[0]
	oldestInstance := oldestWaiter.Member.(string)
	oldestTimestamp := oldestWaiter.Score

	// If oldest waiter is me, no need to yield
	if oldestInstance == rsc.instanceName {
		return
	}

	// Check how long they've been waiting
	now := float64(time.Now().UnixNano()) / 1e9
	waitTime := now - oldestTimestamp

	// If they've been waiting > 5ms, yield briefly to let them acquire
	if waitTime > 0.005 { // 5ms
		sleepDuration := 10 * time.Millisecond
		rsc.logger.Debug().
			Str("server", serverURL).
			Str("starvedInstance", oldestInstance).
			Float64("waitTimeMs", waitTime*1000).
			Dur("yieldDuration", sleepDuration).
			Msg("[tms|redis-slots] Yielding to starved instance")
		time.Sleep(sleepDuration)
	}
}

// ReleaseSlot releases a slot for the given server
func (rsc *RedisSlotCoordinator) ReleaseSlot(ctx context.Context, serverURL string) {
	acquiredKey := fmt.Sprintf("smt:slots:%s:acquired", serverURL)
	holdersKey := fmt.Sprintf("smt:slots:%s:holders", serverURL)

	result, err := rsc.client.Eval(ctx, luaReleaseSlot,
		[]string{acquiredKey, holdersKey},
		rsc.instanceName).Int()

	if err != nil {
		rsc.logger.Error().
			Err(err).
			Str("server", serverURL).
			Str("instance", rsc.instanceName).
			Msg("[tms|redis-slots] Failed to release slot")
		return
	}

	if result == 1 {
		// Success - update local tracking
		if count, ok := rsc.localSlots.Load(serverURL); ok {
			newCount := count.(int) - 1
			if newCount <= 0 {
				rsc.localSlots.Delete(serverURL)
			} else {
				rsc.localSlots.Store(serverURL, newCount)
			}
		}

		rsc.logger.Debug().
			Str("server", serverURL).
			Str("instance", rsc.instanceName).
			Msg("[tms|redis-slots] Released slot")
	}
}

// ReleaseAllSlots releases all slots held by this instance
// Scans Redis for slots held by this instanceName (for ghost cleanup)
// Also releases slots tracked in localSlots (for graceful shutdown)
func (rsc *RedisSlotCoordinator) ReleaseAllSlots(ctx context.Context) {
	rsc.logger.Info().
		Str("instance", rsc.instanceName).
		Msg("[tms|redis-slots] Releasing all slots")

	var totalReleased int

	// 1. Scan Redis for slots held by this instance (handles ghost cleanup)
	pattern := "smt:slots:*:holders"
	var cursor uint64

	for {
		keys, nextCursor, err := rsc.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			rsc.logger.Error().
				Err(err).
				Msg("[tms|redis-slots] Failed to scan for holders")
			break
		}

		for _, holdersKey := range keys {
			// Check if this instance is in holders hash
			count, err := rsc.client.HGet(ctx, holdersKey, rsc.instanceName).Int()
			if err == redis.Nil {
				continue
			} else if err != nil {
				continue
			}

			if count > 0 {
				// Extract server URL from key: smt:slots:{serverURL}:holders
				parts := strings.Split(holdersKey, ":")
				if len(parts) >= 3 {
					serverURL := strings.Join(parts[2:len(parts)-1], ":")

					// Release slots via Lua script (atomically decrements acquired + removes holder)
					for i := 0; i < count; i++ {
						rsc.ReleaseSlot(ctx, serverURL)
					}
					totalReleased += count
				}
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	// 2. Also release slots tracked in localSlots (for graceful shutdown)
	rsc.localSlots.Range(func(key, value interface{}) bool {
		serverURL := key.(string)
		count := value.(int)

		// Release each slot we hold
		for i := 0; i < count; i++ {
			rsc.ReleaseSlot(ctx, serverURL)
		}
		totalReleased += count

		return true
	})

	if totalReleased > 0 {
		rsc.logger.Debug().
			Str("instance", rsc.instanceName).
			Int("slotsReleased", totalReleased).
			Msg("[tms|redis-slots] Released all slots")
	}
}

// SetServerMaxConcurrency sets the maximum concurrency for a server in Redis
func (rsc *RedisSlotCoordinator) SetServerMaxConcurrency(ctx context.Context, serverURL string, maxConcurrency int) error {
	maxKey := fmt.Sprintf("smt:slots:%s:max", serverURL)

	if err := rsc.client.Set(ctx, maxKey, maxConcurrency, 0).Err(); err != nil {
		return fmt.Errorf("failed to set max concurrency: %w", err)
	}

	rsc.logger.Info().
		Str("server", serverURL).
		Int("maxConcurrency", maxConcurrency).
		Msg("[tms|redis-slots] Set server max concurrency")

	return nil
}

// generateInstanceName creates a unique instance identifier
func generateInstanceName() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	pid := os.Getpid()
	return fmt.Sprintf("%s-%d", hostname, pid)
}

// initRedisClient initializes Redis connection if config provided
func (tm *TaskManagerSimple) initRedisClient(cfg *ConfigOptions) error {
	if cfg == nil || cfg.RedisAddr == "" {
		// No Redis config - local-only mode
		tm.logger.Info().Msg("[tms|distributed] Running in local-only mode (no Redis)")
		return nil
	}

	// Set instance name (use provided or generate from hostname-PID)
	if cfg.InstanceName != "" {
		tm.instanceName = cfg.InstanceName
	} else {
		tm.instanceName = generateInstanceName()
	}

	// Create Redis client
	tm.redisClient = redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPwd,
		DB:       cfg.RedisDB,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := tm.redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis at %s: %w", cfg.RedisAddr, err)
	}

	tm.logger.Info().
		Str("instance", tm.instanceName).
		Str("redis", cfg.RedisAddr).
		Msg("[tms|distributed] Connected to Redis")

	// Initialize slot coordinator for distributed concurrency
	tm.slotCoordinator = NewRedisSlotCoordinator(tm.redisClient, tm.instanceName, tm.logger)

	return nil
}

// serializeConfig converts providers, servers, and slot limits to JSON for Redis storage
func serializeConfig(providers *[]IProvider, servers map[string][]string, serverLimits map[string]int) ([]byte, error) {
	if providers == nil || servers == nil {
		return nil, fmt.Errorf("providers and servers cannot be nil")
	}

	// Build provider configs
	providerConfigs := make([]ProviderConfig, 0, len(*providers))
	for _, provider := range *providers {
		providerName := provider.Name()
		serverList, ok := servers[providerName]
		if !ok {
			serverList = []string{}
		}
		providerConfigs = append(providerConfigs, ProviderConfig{
			Name:    providerName,
			Servers: serverList,
		})
	}

	// Create serialized config with version and server limits
	config := SerializedConfig{
		Providers:    providerConfigs,
		Servers:      servers,
		ServerLimits: serverLimits,
		Version:      time.Now().Unix(), // Use timestamp as version
	}

	// Marshal to JSON
	data, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config: %w", err)
	}

	return data, nil
}

// deserializeConfig parses JSON config from Redis
func deserializeConfig(data []byte) (*SerializedConfig, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty config data")
	}

	var config SerializedConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}

// validateConfig checks if config is well-formed
func validateConfig(config *SerializedConfig) error {
	if config == nil {
		return fmt.Errorf("config is nil")
	}

	if len(config.Providers) == 0 {
		return fmt.Errorf("config has no providers")
	}

	// Check each provider has a name
	for i, provider := range config.Providers {
		if provider.Name == "" {
			return fmt.Errorf("provider at index %d has empty name", i)
		}
	}

	// Servers map can be empty (providers without servers)
	if config.Servers == nil {
		return fmt.Errorf("servers map is nil")
	}

	return nil
}

// publishConfig stores config in Redis and notifies slaves via Pub/Sub
func (tm *TaskManagerSimple) publishConfig(providers *[]IProvider, servers map[string][]string) error {
	if tm.redisClient == nil {
		return fmt.Errorf("Redis client not initialized")
	}

	// Extract server limits from serverConcurrencyMap
	tm.serverConcurrencyMu.RLock()
	serverLimits := make(map[string]int, len(tm.serverConcurrencyMap))
	for serverURL, semaphore := range tm.serverConcurrencyMap {
		serverLimits[serverURL] = cap(semaphore)
	}
	tm.serverConcurrencyMu.RUnlock()

	// Serialize config with server limits
	configData, err := serializeConfig(providers, servers, serverLimits)
	if err != nil {
		return fmt.Errorf("failed to serialize config: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Store config in Redis
	if err := tm.redisClient.Set(ctx, "smt:config:data", configData, 0).Err(); err != nil {
		return fmt.Errorf("failed to store config: %w", err)
	}

	// Publish notification to Pub/Sub channel
	if err := tm.redisClient.Publish(ctx, "smt:config:updates", "config_updated").Err(); err != nil {
		tm.logger.Warn().
			Err(err).
			Str("instance", tm.instanceName).
			Msg("[tms|distributed] Failed to publish config update notification")
		// Non-fatal - config is stored, slaves will get it eventually
	}

	tm.logger.Info().
		Str("instance", tm.instanceName).
		Int("providers", len(*providers)).
		Int("serverLimits", len(serverLimits)).
		Msg("[tms|distributed] Published config to Redis")

	return nil
}

// loadConfigFromRedis pulls config from Redis (used by slaves)
func (tm *TaskManagerSimple) loadConfigFromRedis() (*SerializedConfig, error) {
	if tm.redisClient == nil {
		return nil, fmt.Errorf("Redis client not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get config from Redis
	configData, err := tm.redisClient.Get(ctx, "smt:config:data").Bytes()
	if err == redis.Nil {
		return nil, fmt.Errorf("no config found in Redis - master may not be initialized yet")
	} else if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Deserialize
	config, err := deserializeConfig(configData)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize config: %w", err)
	}

	// Validate
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return config, nil
}

// subscribeConfigUpdates listens for config changes via Pub/Sub
func (tm *TaskManagerSimple) subscribeConfigUpdates() {
	if tm.redisClient == nil {
		return
	}

	ctx := context.Background()
	pubsub := tm.redisClient.Subscribe(ctx, "smt:config:updates")
	tm.configPubSub = pubsub // Store for shutdown
	defer pubsub.Close()

	tm.logger.Info().
		Str("instance", tm.instanceName).
		Msg("[tms|distributed] Subscribed to config updates")

	// Wait for subscription confirmation
	_, err := pubsub.Receive(ctx)
	if err != nil {
		tm.logger.Error().
			Err(err).
			Str("instance", tm.instanceName).
			Msg("[tms|distributed] Failed to confirm subscription")
		return
	}

	// Listen for messages
	ch := pubsub.Channel()

	for {
		select {
		case <-tm.shutdownCh:
			tm.logger.Debug().
				Str("instance", tm.instanceName).
				Msg("[tms|distributed] Config subscription shutting down")
			return
		case msg, ok := <-ch:
			if !ok || msg == nil {
				// Channel closed
				return
			}

			tm.logger.Debug().
				Str("instance", tm.instanceName).
				Str("message", msg.Payload).
				Msg("[tms|distributed] Received config update notification")

			// Load new config from Redis
			config, err := tm.loadConfigFromRedis()
			if err != nil {
				tm.logger.Error().
					Err(err).
					Str("instance", tm.instanceName).
					Msg("[tms|distributed] Failed to load updated config")
				continue
			}

			// Update master-controlled servers list
			if config.ServerLimits != nil && len(config.ServerLimits) > 0 {
				tm.masterControlledServersMu.Lock()
				if tm.masterControlledServers == nil {
					tm.masterControlledServers = make(map[string]bool)
				}
				for serverURL := range config.ServerLimits {
					tm.masterControlledServers[serverURL] = true
				}
				tm.masterControlledServersMu.Unlock()

				// Apply server limit hot-reload
				tm.serverConcurrencyMu.Lock()
				updatedCount := 0
				for serverURL, newLimit := range config.ServerLimits {
					// Check if this is a Master-controlled server or slave-only server
					// If we don't have this server locally, it's Master-controlled - skip
					if _, exists := tm.serverConcurrencyMap[serverURL]; !exists {
						// This is a Master-controlled server we don't have locally
						continue
					}

					// Update local limit
					oldLimit := cap(tm.serverConcurrencyMap[serverURL])
					if oldLimit != newLimit {
						tm.serverConcurrencyMap[serverURL] = make(chan struct{}, newLimit)
						updatedCount++

						tm.logger.Debug().
							Str("server", serverURL).
							Int("oldLimit", oldLimit).
							Int("newLimit", newLimit).
							Str("instance", tm.instanceName).
							Msg("[tms|distributed] Updated server concurrency limit")
					}
				}
				tm.serverConcurrencyMu.Unlock()

				if updatedCount > 0 {
					tm.logger.Debug().
						Str("instance", tm.instanceName).
						Int("updated", updatedCount).
						Msg("[tms|distributed] Applied server limit hot-reload")
				}
			}

			tm.logger.Info().
				Str("instance", tm.instanceName).
				Int64("version", config.Version).
				Int("providers", len(config.Providers)).
				Msg("[tms|distributed] Config hot-reload received")
		}
	}
}

// acquireMasterLock attempts to acquire the master lock in Redis
func (tm *TaskManagerSimple) acquireMasterLock() error {
	if tm.redisClient == nil {
		return fmt.Errorf("Redis client not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try to acquire lock with SET NX EX (atomic set if not exists with expiry)
	lockKey := "smt:master:lock"
	success, err := tm.redisClient.SetNX(ctx, lockKey, tm.instanceName, 10*time.Second).Result()
	if err != nil {
		return fmt.Errorf("failed to acquire master lock: %w", err)
	}

	if !success {
		// Lock exists - check if it's expired
		currentHolder, err := tm.redisClient.Get(ctx, lockKey).Result()
		if err == redis.Nil {
			// Lock disappeared between check and now - retry
			return tm.acquireMasterLock()
		} else if err != nil {
			return fmt.Errorf("failed to check existing lock: %w", err)
		}
		return fmt.Errorf("another master is running: %s", currentHolder)
	}

	tm.isMaster = true
	tm.logger.Info().
		Str("instance", tm.instanceName).
		Msg("[tms|distributed] Acquired master lock")

	return nil
}

// renewMasterLock runs as goroutine to periodically renew the master lock
func (tm *TaskManagerSimple) renewMasterLock() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-tm.lockRenewalStop:
			return
		case <-ticker.C:
			if tm.redisClient == nil {
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err := tm.redisClient.Expire(ctx, "smt:master:lock", 10*time.Second).Err()
			cancel()

			if err != nil {
				tm.logger.Error().
					Err(err).
					Str("instance", tm.instanceName).
					Msg("[tms|distributed] Failed to renew master lock")
			} else {
				tm.logger.Debug().
					Str("instance", tm.instanceName).
					Msg("[tms|distributed] Renewed master lock")
			}
		}
	}
}

// releaseMasterLock releases the master lock on shutdown
func (tm *TaskManagerSimple) releaseMasterLock() {
	if tm.redisClient == nil || !tm.isMaster {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Only delete if we still hold it
	script := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`
	_, err := tm.redisClient.Eval(ctx, script, []string{"smt:master:lock"}, tm.instanceName).Result()
	if err != nil {
		tm.logger.Error().
			Err(err).
			Str("instance", tm.instanceName).
			Msg("[tms|distributed] Failed to release master lock")
	} else {
		tm.logger.Info().
			Str("instance", tm.instanceName).
			Msg("[tms|distributed] Released master lock")
	}
}

// startHeartbeat runs as goroutine to periodically update instance heartbeat
func (tm *TaskManagerSimple) startHeartbeat() {
	if tm.redisClient == nil {
		return
	}

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	// Send initial heartbeat immediately
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	heartbeatKey := fmt.Sprintf("smt:instances:%s:heartbeat", tm.instanceName)
	err := tm.redisClient.Set(ctx, heartbeatKey, time.Now().Unix(), 30*time.Second).Err()
	cancel()

	if err != nil {
		tm.logger.Error().
			Err(err).
			Str("instance", tm.instanceName).
			Msg("[tms|distributed] Failed to send initial heartbeat")
	} else {
		tm.logger.Debug().
			Str("instance", tm.instanceName).
			Msg("[tms|distributed] Sent initial heartbeat")
	}

	for {
		select {
		case <-tm.heartbeatStop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err := tm.redisClient.Set(ctx, heartbeatKey, time.Now().Unix(), 30*time.Second).Err()
			cancel()

			if err != nil {
				tm.logger.Error().
					Err(err).
					Str("instance", tm.instanceName).
					Msg("[tms|distributed] Failed to send heartbeat")
			} else {
				tm.logger.Debug().
					Str("instance", tm.instanceName).
					Msg("[tms|distributed] Heartbeat sent")
			}
		}
	}
}

// stopHeartbeat stops the heartbeat goroutine and removes heartbeat key
func (tm *TaskManagerSimple) stopHeartbeat() {
	if tm.redisClient == nil {
		return
	}

	// heartbeatStop already closed in Shutdown()

	// Remove heartbeat key
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	heartbeatKey := fmt.Sprintf("smt:instances:%s:heartbeat", tm.instanceName)
	err := tm.redisClient.Del(ctx, heartbeatKey).Err()
	if err != nil {
		tm.logger.Error().
			Err(err).
			Str("instance", tm.instanceName).
			Msg("[tms|distributed] Failed to remove heartbeat key")
	} else {
		tm.logger.Info().
			Str("instance", tm.instanceName).
			Msg("[tms|distributed] Removed heartbeat key")
	}
}

// monitorDeadInstances runs as goroutine to detect and cleanup crashed instances
func (tm *TaskManagerSimple) monitorDeadInstances() {
	if tm.redisClient == nil {
		return
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-tm.shutdownCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

			// Scan for all instance heartbeat keys
			pattern := "smt:instances:*:heartbeat"
			var cursor uint64
			var deadInstances []string

			for {
				keys, nextCursor, err := tm.redisClient.Scan(ctx, cursor, pattern, 100).Result()
				if err != nil {
					tm.logger.Error().
						Err(err).
						Msg("[tms|distributed] Failed to scan for instance heartbeats")
					break
				}

				// Check TTL for each key - if expired (TTL -2), instance is dead
				for _, key := range keys {
					ttl, err := tm.redisClient.TTL(ctx, key).Result()
					if err != nil {
						continue
					}

					// TTL -2 means key doesn't exist (expired)
					// TTL -1 means key exists but has no expiry (shouldn't happen)
					if ttl == -2*time.Second {
						// Extract instance name from key: smt:instances:{name}:heartbeat
						parts := strings.Split(key, ":")
						if len(parts) >= 3 {
							instanceName := parts[2]
							deadInstances = append(deadInstances, instanceName)
						}
					}
				}

				cursor = nextCursor
				if cursor == 0 {
					break
				}
			}

			cancel()

			// Cleanup dead instances
			for _, instanceName := range deadInstances {
				if instanceName != tm.instanceName {
					tm.logger.Warn().
						Str("deadInstance", instanceName).
						Msg("[tms|distributed] Detected dead instance, cleaning up")
					tm.cleanupDeadInstance(instanceName)
				}
			}

			// Cleanup ghost locks (acquired > 0 but holders hash empty/missing)
			tm.cleanupGhostLocks()
		}
	}
}

// cleanupGhostLocks finds and releases slots where acquired > 0 but no holders exist
// This can happen if holders hash expired (legacy TTL) or other edge cases
func (tm *TaskManagerSimple) cleanupGhostLocks() {
	if tm.redisClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Scan for all acquired keys
	pattern := "smt:slots:*:acquired"
	var cursor uint64
	var ghostsCleared int

	for {
		keys, nextCursor, err := tm.redisClient.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			tm.logger.Error().
				Err(err).
				Msg("[tms|ghost-locks] Failed to scan for acquired keys")
			break
		}

		for _, acquiredKey := range keys {
			// Get acquired count
			acquired, err := tm.redisClient.Get(ctx, acquiredKey).Int()
			if err != nil || acquired <= 0 {
				continue
			}

			// Extract server URL and check holders
			// Key format: smt:slots:{serverURL}:acquired
			serverURL := strings.TrimPrefix(acquiredKey, "smt:slots:")
			serverURL = strings.TrimSuffix(serverURL, ":acquired")
			holdersKey := fmt.Sprintf("smt:slots:%s:holders", serverURL)

			// Check if holders hash exists and has entries
			holdersCount, err := tm.redisClient.HLen(ctx, holdersKey).Result()
			if err != nil {
				continue
			}

			// Ghost lock: acquired > 0 but no holders
			if holdersCount == 0 {
				// Reset acquired to 0
				err = tm.redisClient.Set(ctx, acquiredKey, 0, 0).Err()
				if err != nil {
					tm.logger.Error().
						Err(err).
						Str("server", serverURL).
						Msg("[tms|ghost-locks] Failed to clear ghost lock")
					continue
				}

				// Also clear waiting queue
				waitingKey := fmt.Sprintf("smt:slots:%s:waiting", serverURL)
				_ = tm.redisClient.Del(ctx, waitingKey).Err()

				ghostsCleared++
				tm.logger.Warn().
					Str("server", serverURL).
					Int("wasAcquired", acquired).
					Msg("[tms|ghost-locks] Cleared ghost lock (acquired > 0 but no holders)")
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	if ghostsCleared > 0 {
		tm.logger.Info().
			Int("cleared", ghostsCleared).
			Msg("[tms|ghost-locks] Ghost lock cleanup complete")
	}
}

// cleanupDeadInstance releases all resources held by a crashed instance
func (tm *TaskManagerSimple) cleanupDeadInstance(instanceName string) {
	if tm.redisClient == nil {
		return
	}

	tm.logger.Info().
		Str("deadInstance", instanceName).
		Msg("[tms|distributed] Starting cleanup for dead instance")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Scan for all slot holder hashes
	pattern := "smt:slots:*:holders"
	var cursor uint64
	var cleanedSlots int

	for {
		keys, nextCursor, err := tm.redisClient.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			tm.logger.Error().
				Err(err).
				Str("deadInstance", instanceName).
				Msg("[tms|distributed] Failed to scan for slot holders")
			break
		}

		for _, holdersKey := range keys {
			// Check if dead instance is in this holders hash
			count, err := tm.redisClient.HGet(ctx, holdersKey, instanceName).Int()
			if err == redis.Nil {
				// Instance not in this hash
				continue
			} else if err != nil {
				tm.logger.Error().
					Err(err).
					Str("holdersKey", holdersKey).
					Str("deadInstance", instanceName).
					Msg("[tms|distributed] Failed to check holder")
				continue
			}

			if count > 0 {
				// Dead instance held slots - clean them up
				// Extract server URL from key: smt:slots:{serverURL}:holders
				parts := strings.Split(holdersKey, ":")
				if len(parts) >= 3 {
					serverURL := strings.Join(parts[2:len(parts)-1], ":")

					// Decrement acquired count by the number of slots held
					acquiredKey := fmt.Sprintf("smt:slots:%s:acquired", serverURL)
					_, err := tm.redisClient.DecrBy(ctx, acquiredKey, int64(count)).Result()
					if err != nil {
						tm.logger.Error().
							Err(err).
							Str("server", serverURL).
							Str("deadInstance", instanceName).
							Msg("[tms|distributed] Failed to decrement acquired count")
					}

					// Remove dead instance from holders hash
					err = tm.redisClient.HDel(ctx, holdersKey, instanceName).Err()
					if err != nil {
						tm.logger.Error().
							Err(err).
							Str("holdersKey", holdersKey).
							Str("deadInstance", instanceName).
							Msg("[tms|distributed] Failed to remove holder")
					} else {
						cleanedSlots += count
						tm.logger.Info().
							Str("server", serverURL).
							Str("deadInstance", instanceName).
							Int("slotsReleased", count).
							Msg("[tms|distributed] Released slots from dead instance")
					}
				}
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	tm.logger.Info().
		Str("deadInstance", instanceName).
		Int("totalSlotsReleased", cleanedSlots).
		Msg("[tms|distributed] Cleanup complete for dead instance")
}

// cleanupGhostInstance cleans up resources from previous run of THIS instance
// Called on startup to prevent slot leaks from hard restarts within heartbeat TTL
func (tm *TaskManagerSimple) cleanupGhostInstance() {
	if tm.redisClient == nil || tm.instanceName == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Delete old master lock if held by this instanceName
	lockKey := "smt:master:lock"
	currentHolder, err := tm.redisClient.Get(ctx, lockKey).Result()
	if err == nil && currentHolder == tm.instanceName {
		// Lock is held by previous run of this instance - release it
		err = tm.redisClient.Del(ctx, lockKey).Err()
		if err != nil && err != redis.Nil {
			tm.logger.Warn().
				Err(err).
				Str("instance", tm.instanceName).
				Msg("[tms|distributed] Failed to delete ghost master lock")
		}
	}

	// Delete old heartbeat key
	heartbeatKey := fmt.Sprintf("smt:instances:%s:heartbeat", tm.instanceName)
	err = tm.redisClient.Del(ctx, heartbeatKey).Err()
	if err != nil && err != redis.Nil {
		tm.logger.Warn().
			Err(err).
			Str("instance", tm.instanceName).
			Msg("[tms|distributed] Failed to delete ghost heartbeat")
	}

	// Release all slots held by previous instance with same name
	// SlotCoordinator tracks localSlots in-memory, empty on fresh start
	// ReleaseAllSlots scans Redis for slots held by this instanceName
	if tm.slotCoordinator != nil {
		tm.slotCoordinator.ReleaseAllSlots(ctx)
		tm.logger.Info().
			Str("instance", tm.instanceName).
			Msg("[tms|distributed] Cleaned up ghost instance resources")
	}
}

func NewTaskManagerSimple(
	providers *[]IProvider,
	servers map[string][]string,
	logger *zerolog.Logger,
	getTimeout func(string, string) time.Duration,
) *TaskManagerSimple {
	tm := &TaskManagerSimple{
		providers:            make(map[string]*ProviderData),
		taskInQueue:          make(map[string]struct{}),
		runningTasks:         make(map[string]*RunningTaskInfo),
		isRunning:            0,
		shutdownRequest:      0,
		shutdownCh:           make(chan struct{}),
		logger:               logger,
		getTimeout:           getTimeout,
		serverConcurrencyMap: make(map[string]chan struct{}),
		serverConcurrencyMu:  sync.RWMutex{},
	}

	// Initialize providers
	for _, provider := range *providers {
		providerName := provider.Name()
		serverList, ok := servers[providerName]
		if !ok {
			serverList = []string{}
		}
		pd := &ProviderData{
			taskQueue:        make(TaskQueuePrio, 0, 1024), // Pre-allocate for better performance
			taskQueueLock:    sync.Mutex{},
			commandQueue:     NewCommandQueue(256), // Increased initial capacity to reduce resizing
			commandSet:       make(map[uuid.UUID]struct{}),
			commandSetLock:   sync.Mutex{},
			servers:          serverList,
			availableServers: make(chan string, len(serverList)*2), // Double buffer for re-queuing scenarios
		}

		// Fill the channel with all servers
		for _, srv := range serverList {
			pd.availableServers <- srv

			// Parse server URL to normalize it (remove query params)
			normalizedSrv := srv
			if u, err := url.Parse(srv); err == nil {
				u.RawQuery = ""
				u.Fragment = ""
				normalizedSrv = u.String()
			}

			// Set default concurrency limit of 1 for each server
			// unless it already has a limit set
			if _, exists := tm.serverConcurrencyMap[normalizedSrv]; !exists {
				tm.serverConcurrencyMap[normalizedSrv] = make(chan struct{}, 1)
				logger.Debug().
					Str("server", normalizedSrv).
					Msg("[tms] Set default concurrency limit of 1 for server")
			}
		}

		// IMPORTANT: tie the condition to the same mutex used for the queue
		pd.taskQueueCond = sync.NewCond(&pd.taskQueueLock)

		tm.providers[providerName] = pd
	}

	return tm
}

func (tm *TaskManagerSimple) SetTaskManagerServerMaxParallel(prefix string, maxParallel int) {
	// Check if this is a slave trying to modify Master-controlled server
	if !tm.isMaster && tm.slotCoordinator != nil {
		tm.masterControlledServersMu.RLock()
		isMasterControlled := tm.masterControlledServers[prefix]
		tm.masterControlledServersMu.RUnlock()

		if isMasterControlled {
			tm.logger.Warn().
				Str("server", prefix).
				Str("instance", tm.instanceName).
				Msg("[tms] Slave ignoring SetTaskManagerServerMaxParallel for Master-controlled server")
			return
		}
	}

	// Update local concurrency map
	tm.serverConcurrencyMu.Lock()
	if maxParallel <= 0 {
		delete(tm.serverConcurrencyMap, prefix)
	} else {
		tm.serverConcurrencyMap[prefix] = make(chan struct{}, maxParallel)
	}
	tm.serverConcurrencyMu.Unlock()

	// Sync to Redis if distributed mode
	if tm.slotCoordinator != nil && maxParallel > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := tm.slotCoordinator.SetServerMaxConcurrency(ctx, prefix, maxParallel)
		cancel()
		if err != nil {
			tm.logger.Error().
				Err(err).
				Str("server", prefix).
				Str("instance", tm.instanceName).
				Msg("[tms] Failed to sync slot limit to Redis")
		}

		// Master should publish updated config
		if tm.isMaster {
			// Republish config with updated limits
			// Note: This triggers hot-reload on all slaves
			tm.logger.Debug().
				Str("server", prefix).
				Int("maxParallel", maxParallel).
				Msg("[tms] Master updated slot limit, republishing config")
		}
	}
}

func (tm *TaskManagerSimple) HasShutdownRequest() bool {
	return atomic.LoadInt32(&tm.shutdownRequest) == 1
}

func (tm *TaskManagerSimple) IsRunning() bool {
	return atomic.LoadInt32(&tm.isRunning) == 1
}

// Helper methods for thread-safe taskInQueue operations
func (tm *TaskManagerSimple) isTaskInQueue(taskID string) bool {
	tm.taskInQueueMu.RLock()
	_, exists := tm.taskInQueue[taskID]
	tm.taskInQueueMu.RUnlock()
	return exists
}

func (tm *TaskManagerSimple) addTaskToQueue(taskID string) bool {
	tm.taskInQueueMu.Lock()
	if _, exists := tm.taskInQueue[taskID]; exists {
		tm.taskInQueueMu.Unlock()
		return false // Task already in queue
	}
	tm.taskInQueue[taskID] = struct{}{}
	tm.taskInQueueMu.Unlock()
	return true // Task was added
}

// delTaskInQueue removes a task ID from the map
func (tm *TaskManagerSimple) delTaskInQueue(task ITask) {
	tm.taskInQueueMu.Lock()
	delete(tm.taskInQueue, task.GetID())
	// Periodically recreate the map to release memory
	// This prevents long-term memory growth from map internals
	if len(tm.taskInQueue) == 0 {
		// Map is empty, recreate it to release internal buckets
		tm.taskInQueue = make(map[string]struct{})
	}
	tm.taskInQueueMu.Unlock()
}

func (tm *TaskManagerSimple) AddTasks(tasks []ITask) (count int, err error) {
	for _, task := range tasks {
		if tm.AddTask(task) {
			count++
		}
	}
	return count, err
}

// AddTask enqueues a task if not already known. Returns true if successfully enqueued.
func (tm *TaskManagerSimple) AddTask(task ITask) bool {
	if atomic.LoadInt32(&tm.shutdownRequest) == 1 {
		return false
	}
	if atomic.LoadInt32(&tm.isRunning) != 1 {
		// Manager not running; cannot queue
		return false
	}

	taskID := task.GetID()
	provider := task.GetProvider()
	if provider == nil {
		err := fmt.Errorf("task '%s' has no provider", taskID)
		tm.logger.Error().Err(err).Msg("[tms|nil_provider|error]")
		task.MarkAsFailed(0, err)
		task.OnComplete()
		// Clean up all task tracking on early failure (though provider mapping not yet stored)
		tm.cleanupTaskTracking(taskID)
		return false
	}

	providerName := provider.Name()
	pd, ok := tm.providers[providerName]
	if !ok {
		err := fmt.Errorf(errProviderNotFound, providerName)
		tm.logger.Error().Err(err).
			Str("provider", providerName).
			Str("taskID", taskID).
			Msg("[tms] provider not found")
		task.MarkAsFailed(0, err)
		task.OnComplete()
		// Clean up all task tracking on early failure (though provider mapping not yet stored)
		tm.cleanupTaskTracking(taskID)
		return false
	}

	// Use our thread-safe helper method
	if !tm.addTaskToQueue(taskID) {
		return false
	}

	// Store taskID -> providerName mapping for O(1) lookup in DelTask
	tm.taskToProvider.Store(taskID, providerName)

	// Push into the priority queue
	priority := task.GetPriority()
	twp := taskWithPriorityPool.Get().(*TaskWithPriority)
	twp.task = task
	twp.priority = priority
	twp.index = 0

	pd.taskQueueLock.Lock()
	heap.Push(&pd.taskQueue, twp)
	atomic.AddInt32(&pd.taskCount, 1)
	pd.taskQueueCond.Signal()
	pd.taskQueueLock.Unlock()

	return true
}

// DelTask removes a task from queue or cancels it if running
func (tm *TaskManagerSimple) DelTask(taskID string, interruptFn func(task ITask, server string) error) DelTaskResult {
	if atomic.LoadInt32(&tm.isRunning) != 1 {
		return DelTaskErrorNotRunning
	}

	// First, check if task is currently running
	tm.runningTasksMu.Lock()
	if runningTask, exists := tm.runningTasks[taskID]; exists {
		// Task is currently running - set interrupt function and signal cancellation
		runningTask.interruptFn = interruptFn
		close(runningTask.cancelCh) // Signal the running task to cancel
		task := runningTask.task
		server := runningTask.server
		tm.runningTasksMu.Unlock()

		// Call the interrupt function if provided, passing both task and server
		if interruptFn != nil {
			err := interruptFn(task, server)
			if err != nil {
				tm.logger.Debug().Err(err).Str("taskID", taskID).Str("server", server).Msg("[tms|DelTask] Interrupt function returned error")
			}
		}

		tm.logger.Debug().Str("taskID", taskID).Str("server", server).Msg("[tms|DelTask] Interrupted running task")
		return DelTaskInterruptedRunning
	}
	tm.runningTasksMu.Unlock()

	// Task is not running, check if it's in queue
	if !tm.isTaskInQueue(taskID) {
		return DelTaskNotFound
	}

	// Task is queued - use O(1) lookup to find which provider owns it
	providerNameInterface, ok := tm.taskToProvider.Load(taskID)
	if !ok {
		// Task was in queue map but not in provider map - shouldn't happen
		tm.logger.Warn().Str("taskID", taskID).Msg("[tms|DelTask] Task in queue but provider mapping not found")
		return DelTaskNotFound
	}

	providerName := providerNameInterface.(string)
	pd, providerExists := tm.providers[providerName]
	if !providerExists {
		tm.logger.Warn().Str("taskID", taskID).Str("provider", providerName).Msg("[tms|DelTask] Provider not found for task")
		return DelTaskNotFound
	}

	// Search only this provider's queue
	var removedTask ITask
	removed := false

	pd.taskQueueLock.Lock()
	for i := 0; i < pd.taskQueue.Len(); i++ {
		if pd.taskQueue[i].task.GetID() == taskID {
			// Found it - remove from heap
			removedItem := heap.Remove(&pd.taskQueue, i).(*TaskWithPriority)
			atomic.AddInt32(&pd.taskCount, -1)

			// Capture the task before clearing it
			removedTask = removedItem.task

			// Return TaskWithPriority to pool
			removedItem.task = nil
			taskWithPriorityPool.Put(removedItem)

			removed = true
			tm.logger.Debug().
				Str("taskID", taskID).
				Str("provider", providerName).
				Msg("[tms|DelTask] Removed task from queue")
			break
		}
	}
	pd.taskQueueLock.Unlock()

	if removed {
		// Remove from taskInQueue tracking
		tm.delTaskInQueue(removedTask)

		// Clean up provider mapping
		tm.taskToProvider.Delete(taskID)

		// Call interrupt function even for queued tasks (server is empty since not assigned yet)
		if interruptFn != nil {
			err := interruptFn(removedTask, "")
			if err != nil {
				tm.logger.Debug().Err(err).Str("taskID", taskID).Msg("[tms|DelTask] Interrupt function returned error for queued task")
			}
		}

		return DelTaskRemovedFromQueue
	}

	return DelTaskNotFound
}

// Helper methods for running task tracking
func (tm *TaskManagerSimple) registerRunningTask(taskID string, task ITask, providerName, server string) *RunningTaskInfo {
	tm.runningTasksMu.Lock()
	defer tm.runningTasksMu.Unlock()

	info := &RunningTaskInfo{
		task:         task,
		cancelCh:     make(chan struct{}),
		providerName: providerName,
		server:       server,
	}
	tm.runningTasks[taskID] = info

	return info
}

func (tm *TaskManagerSimple) unregisterRunningTask(taskID string) {
	tm.runningTasksMu.Lock()
	defer tm.runningTasksMu.Unlock()

	delete(tm.runningTasks, taskID)
}

// taskWrapper is a minimal ITask implementation for cleanup purposes
type taskWrapper struct {
	id string
}

func (tw *taskWrapper) GetID() string                   { return tw.id }
func (tw *taskWrapper) MarkAsSuccess(t int64)           {}
func (tw *taskWrapper) MarkAsFailed(t int64, err error) {}
func (tw *taskWrapper) GetPriority() int                { return 0 }
func (tw *taskWrapper) GetMaxRetries() int              { return 0 }
func (tw *taskWrapper) GetRetries() int                 { return 0 }
func (tw *taskWrapper) GetCreatedAt() time.Time         { return time.Time{} }
func (tw *taskWrapper) GetTaskGroup() ITaskGroup        { return nil }
func (tw *taskWrapper) GetProvider() IProvider          { return nil }
func (tw *taskWrapper) UpdateRetries(int) error         { return nil }
func (tw *taskWrapper) GetTimeout() time.Duration       { return 0 }
func (tw *taskWrapper) UpdateLastError(string) error    { return nil }
func (tw *taskWrapper) GetCallbackName() string         { return "" }
func (tw *taskWrapper) OnComplete()                     {}
func (tw *taskWrapper) OnStart()                        {}

func (tm *TaskManagerSimple) Start() {
	if tm.IsRunning() {
		return
	}
	atomic.StoreInt32(&tm.isRunning, 1)

	for providerName := range tm.providers {
		tm.wg.Add(1)
		go tm.providerDispatcher(providerName)
	}
}

func (tm *TaskManagerSimple) providerDispatcher(providerName string) {
	defer func() {
		tm.logger.Debug().
			Str("instance", tm.instanceName).
			Str("provider", providerName).
			Msg("[tms] providerDispatcher exiting, calling wg.Done()")
		tm.wg.Done()
	}()
	pd := tm.providers[providerName]
	shutdownCh := tm.shutdownCh
	tm.logger.Debug().
		Str("instance", tm.instanceName).
		Str("provider", providerName).
		Msg("[tms] providerDispatcher started")

	const batchSize = 4
	serverBatch := make([]string, 0, batchSize)

	var isCommand bool
	var command Command
	var task ITask
	var taskWithPriority *TaskWithPriority
	var hasWork bool
	var server string

	for {
		// If we have leftover servers in the local batch, try to use them
		if len(serverBatch) > 0 {
			server = serverBatch[0]
			serverBatch = serverBatch[1:]

			isCommand = false
			command = Command{}
			task = nil
			hasWork = false

			pd.taskQueueLock.Lock()
			if pd.taskQueue.Len() > 0 {
				taskWithPriority = heap.Pop(&pd.taskQueue).(*TaskWithPriority)
				atomic.AddInt32(&pd.taskCount, -1)
				task = taskWithPriority.task
				hasWork = true
			} else if pd.commandQueue.Len() > 0 {
				command, _ = pd.commandQueue.Dequeue()
				atomic.AddInt32(&pd.commandCount, -1)
				isCommand = true
				hasWork = true
			} else {
				// No tasks or commands; return the server
				pd.taskQueueLock.Unlock()
				pd.availableServers <- server
				continue
			}
			pd.taskQueueLock.Unlock()

			if hasWork {
				tm.wg.Add(1)
				if isCommand {
					go tm.processCommand(command, providerName, server)
				} else {
					go tm.processTask(task, providerName, server)
				}
			}
			continue
		}

		// Otherwise, wait for tasks/commands
		isCommand = false
		command = Command{}
		taskWithPriority = nil
		hasWork = false

		// Check for shutdown before waiting
		if tm.HasShutdownRequest() {
			tm.logger.Debug().
				Str("instance", tm.instanceName).
				Str("provider", providerName).
				Msg("[tms] providerDispatcher: shutdown detected before wait")
			return
		}

		// Fast path: check atomic counters first
		if atomic.LoadInt32(&pd.taskCount) == 0 && atomic.LoadInt32(&pd.commandCount) == 0 {
			tm.logger.Debug().
				Str("instance", tm.instanceName).
				Str("provider", providerName).
				Msg("[tms] providerDispatcher: entering Wait()")
			pd.taskQueueLock.Lock()
			// Double-check under lock
			for pd.taskQueue.Len() == 0 && pd.commandQueue.Len() == 0 && !tm.HasShutdownRequest() {
				pd.taskQueueCond.Wait()
			}
			pd.taskQueueLock.Unlock()
			tm.logger.Debug().
				Str("instance", tm.instanceName).
				Str("provider", providerName).
				Msg("[tms] providerDispatcher: woke from Wait()")
			if tm.HasShutdownRequest() {
				tm.logger.Debug().
					Str("instance", tm.instanceName).
					Str("provider", providerName).
					Msg("[tms] providerDispatcher: shutdown detected after wait")
				return
			}
		}

		pd.taskQueueLock.Lock()
		if pd.taskQueue.Len() > 0 {
			taskWithPriority = heap.Pop(&pd.taskQueue).(*TaskWithPriority)
			atomic.AddInt32(&pd.taskCount, -1)
			hasWork = true
		} else if pd.commandQueue.Len() > 0 {
			command, _ = pd.commandQueue.Dequeue()
			atomic.AddInt32(&pd.commandCount, -1)
			isCommand = true
			hasWork = true
		}
		pd.taskQueueLock.Unlock()

		if !hasWork {
			continue
		}

		// Grab a server (with proper shutdown handling)
		tm.logger.Debug().
			Str("instance", tm.instanceName).
			Str("provider", providerName).
			Msg("[tms] providerDispatcher: waiting for availableServers")
		select {
		case <-shutdownCh:
			tm.logger.Debug().
				Str("instance", tm.instanceName).
				Str("provider", providerName).
				Msg("[tms] providerDispatcher: shutdown in select")
			if !isCommand && taskWithPriority != nil {
				// Return TWP to pool
				taskWithPriority.task = nil
				taskWithPriorityPool.Put(taskWithPriority)
			}
			return

		case server = <-pd.availableServers:
			tm.logger.Debug().
				Str("instance", tm.instanceName).
				Str("provider", providerName).
				Msg("[tms] providerDispatcher: got server from channel")
			tm.wg.Add(1)
			if isCommand {
				go tm.processCommand(command, providerName, server)
			} else {
				task = taskWithPriority.task
				// Return TWP to pool
				taskWithPriority.task = nil
				taskWithPriorityPool.Put(taskWithPriority)

				go tm.processTask(task, providerName, server)
			}

			// Skip server batching to reduce complexity
		}
	}
}

type contextKey string

const (
	taskIDKey       contextKey = "taskID"
	providerNameKey contextKey = "providerName"
	serverNameKey   contextKey = "serverName"
)

func (tm *TaskManagerSimple) processTask(task ITask, providerName, server string) {
	// CRITICAL: defer wg.Done() FIRST before any early returns
	defer tm.wg.Done()

	started := time.Now()
	var onCompleteCalled bool

	taskID := task.GetID()
	provider := task.GetProvider()
	pd, providerExists := tm.providers[providerName]

	tm.logger.Debug().
		Str("instance", tm.instanceName).
		Str("taskID", taskID).
		Msg("[tms] >>> processTask STARTED")

	// 1) Acquire concurrency FIRST (non-blocking with retry).
	// Use distributed coordination if available, otherwise fallback to local semaphores
	var semaphore chan struct{}
	var hasLimit bool
	var usingRedis bool

	// Normalize server URL for slot coordination
	normalizedServer := server
	if u, err := url.Parse(server); err == nil {
		u.RawQuery = ""
		u.Fragment = ""
		normalizedServer = u.String()
	}

	if tm.slotCoordinator != nil {
		// Distributed mode - use Redis coordination
		usingRedis = true
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		// Fairness check: yield if others have been starving
		tm.slotCoordinator.CheckFairnessBeforeAcquire(ctx, normalizedServer)

		acquired := tm.slotCoordinator.AcquireSlot(ctx, normalizedServer)
		if !acquired {
			// Limit reached - re-queue task
			tm.logger.Debug().
				Str("server", normalizedServer).
				Str("taskID", taskID).
				Msg("[tms|processTask] Redis slot limit reached, re-queuing task")

			// Return server to pool
			tm.returnServerToPool(providerExists, pd, server)

			// Re-queue without incrementing retry counter
			tm.delTaskInQueue(task)

			// Calculate exponential backoff
			backoffDelay := tm.calculateConcurrencyBackoff(taskID)
			time.Sleep(backoffDelay)

			tm.AddTask(task)
			return
		}

		tm.logger.Debug().
			Str("server", normalizedServer).
			Str("taskID", taskID).
			Msg("[tms|processTask] Acquired Redis slot")
	} else {
		// Local mode - use channel semaphores
		semaphore, hasLimit = tm.getServerSemaphore(server)
		if hasLimit {
			// Try to acquire non-blocking first
			select {
			case semaphore <- struct{}{}:
				tm.logger.Debug().
					Str("server", server).
					Str("taskID", taskID).
					Msg("[tms|processTask] Acquired local slot immediately")
			default:
				// If blocked, return server and re-queue task
				tm.logger.Debug().
					Str("server", server).
					Str("taskID", taskID).
					Msg("[tms|processTask] Local concurrency limit reached, re-queuing task")

				// Return server to pool
				tm.returnServerToPool(providerExists, pd, server)

				// Re-queue without incrementing retry counter
				tm.delTaskInQueue(task)

				// Calculate exponential backoff
				backoffDelay := tm.calculateConcurrencyBackoff(taskID)
				time.Sleep(backoffDelay)

				tm.AddTask(task)
				return
			}
		}
	}

	// Register this task as running
	runningTask := tm.registerRunningTask(taskID, task, providerName, server)

	defer func() {
		tm.logger.Debug().
			Str("instance", tm.instanceName).
			Str("taskID", taskID).
			Msg("[tms] >>> processTask cleanup starting")
		// Unregister running task
		tm.unregisterRunningTask(taskID)

		// 4) Always release concurrency when done
		if usingRedis {
			// Release Redis slot
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			tm.slotCoordinator.ReleaseSlot(ctx, normalizedServer)
			cancel()
			tm.logger.Debug().
				Str("server", normalizedServer).
				Str("taskID", taskID).
				Msg("[tms|processTask] Released Redis slot")
		} else if hasLimit {
			// Release local semaphore
			<-semaphore
			tm.logger.Debug().
				Str("server", server).
				Str("taskID", taskID).
				Msg("[tms|processTask] Released local slot")
		}
		// Return server to the pool
		tm.returnServerToPool(providerExists, pd, server)
	}()

	// Recover from panic ...
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic occurred: %v\n%s", r, string(debug.Stack()))
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic", providerName, taskID, server)
			now := time.Now()
			elapsed := now.Sub(started)
			task.MarkAsFailed(elapsed.Milliseconds(), err)
			if !onCompleteCalled {
				task.OnComplete()
				onCompleteCalled = true
			}
			tm.delTaskInQueue(task)
			// Clean up all task tracking on panic
			tm.cleanupTaskTracking(taskID)
		}
	}()

	// Validate provider
	if provider == nil {
		err := fmt.Errorf("task '%s' has no provider", taskID)
		tm.logger.Error().Err(err).
			Msgf("[tms|%s|%s|%s] Task has no provider", providerName, taskID, server)
		now := time.Now()
		task.MarkAsFailed(now.Sub(started).Milliseconds(), err)
		if !onCompleteCalled {
			task.OnComplete()
			onCompleteCalled = true
		}
		tm.delTaskInQueue(task)
		// Clean up all task tracking on provider validation failure
		tm.cleanupTaskTracking(taskID)
		return
	}

	// Check if task was cancelled before execution
	select {
	case <-runningTask.cancelCh:
		// Task was cancelled via DelTask
		tm.logger.Debug().Str("taskID", taskID).Msg("[tms|processTask] Task cancelled before execution")
		now := time.Now()
		task.MarkAsFailed(now.Sub(started).Milliseconds(), fmt.Errorf("task cancelled"))
		if !onCompleteCalled {
			task.OnComplete()
			onCompleteCalled = true
		}
		tm.delTaskInQueue(task)
		tm.cleanupTaskTracking(taskID)
		return
	default:
		// Continue with execution
	}

	// 2) Now that concurrency is acquired, we start the actual timed work:
	err, totalTime := tm.HandleWithTimeout(providerName, task, server, tm.HandleTask)
	if err != nil {
		retries := task.GetRetries()
		maxRetries := task.GetMaxRetries()
		if retries >= maxRetries || err == sql.ErrNoRows {
			// Final failure
			tm.logger.Error().Err(err).
				Msgf("[tms|%s|%s|%s] max retries reached or no rows", providerName, taskID, server)
			task.MarkAsFailed(totalTime, err)
			if !onCompleteCalled {
				task.OnComplete()
				onCompleteCalled = true
			}
			tm.delTaskInQueue(task)
			// Clean up all task tracking on final failure
			tm.cleanupTaskTracking(taskID)
		} else {
			// Retry scenario - DON'T clean up provider mapping, task will be re-added
			if level := tm.logger.GetLevel(); level <= zerolog.DebugLevel {
				tm.logger.Debug().
					Err(err).
					Str("provider", providerName).
					Str("taskID", taskID).
					Str("server", server).
					Int("retry", retries+1).
					Int("maxRetries", maxRetries).
					Msg("[tms] retrying task")
			}
			task.UpdateRetries(retries + 1)
			tm.delTaskInQueue(task) // remove so it can be re-added
			tm.AddTask(task)        // This will re-store the provider mapping
		}
	} else {
		// Success
		task.MarkAsSuccess(totalTime)
		if !onCompleteCalled {
			task.OnComplete()
			onCompleteCalled = true
		}
		tm.delTaskInQueue(task)
		// Clean up all task tracking on success
		tm.cleanupTaskTracking(taskID)
	}
}

// HandleTask calls provider.Handle(task, server) directly
func (tm *TaskManagerSimple) HandleTask(task ITask, server string) error {
	provider := task.GetProvider()
	if provider == nil {
		return fmt.Errorf("task '%s' has no provider", task.GetID())
	}
	return provider.Handle(task, server)
}
func (tm *TaskManagerSimple) returnServerToPool(providerExists bool, pd *ProviderData, server string) {
	if !providerExists {
		return
	}
	// Always block to ensure server is returned
	pd.availableServers <- server
}

// getServerSemaphore checks if the server has a concurrency limit.
// Always returns true now since we default to limit of 1.
func (tm *TaskManagerSimple) getServerSemaphore(server string) (chan struct{}, bool) {
	// Optional: parse out query, to unify concurrency for any query string
	if u, err := url.Parse(server); err == nil {
		// Remove query and fragment, so "https://foo?x=1" => "https://foo"
		u.RawQuery = ""
		u.Fragment = ""
		server = u.String()
	}

	tm.serverConcurrencyMu.RLock()

	// Check if we have an exact match or prefix match
	if sem, exists := tm.serverConcurrencyMap[server]; exists {
		tm.serverConcurrencyMu.RUnlock()
		return sem, true
	}

	// Check for prefix matches
	for prefix, sem := range tm.serverConcurrencyMap {
		if strings.HasPrefix(server, prefix) {
			tm.serverConcurrencyMu.RUnlock()
			return sem, true
		}
	}

	// No match found - need to create default
	tm.serverConcurrencyMu.RUnlock()

	// Upgrade to write lock to create default
	tm.serverConcurrencyMu.Lock()
	defer tm.serverConcurrencyMu.Unlock()

	// Double-check in case another goroutine created it
	if sem, exists := tm.serverConcurrencyMap[server]; exists {
		return sem, true
	}

	// Create default limit of 1
	defaultSemaphore := make(chan struct{}, 1)
	tm.serverConcurrencyMap[server] = defaultSemaphore

	tm.logger.Debug().
		Str("server", server).
		Msg("[tms] Created default concurrency limit of 1 for unknown server")

	return defaultSemaphore, true
}

// calculateConcurrencyBackoff computes exponential backoff for concurrency retries
func (tm *TaskManagerSimple) calculateConcurrencyBackoff(taskID string) time.Duration {
	// Get current retry count (defaults to 0 if not found)
	retriesInterface, _ := tm.concurrencyRetryMap.Load(taskID)
	retries := 0
	if retriesInterface != nil {
		retries = retriesInterface.(int)
	}

	// Increment retry count
	tm.concurrencyRetryMap.Store(taskID, retries+1)

	// Exponential backoff: 5ms * 2^retries, capped at 800ms
	baseDelay := 5 * time.Millisecond
	maxDelay := 800 * time.Millisecond

	exponentialDelay := time.Duration(float64(baseDelay) * math.Pow(2, float64(retries)))
	if exponentialDelay > maxDelay {
		exponentialDelay = maxDelay
	}

	// Add 0-50% jitter to prevent thundering herd
	jitterPercent := rand.Float64() * 0.5 // 0-50%
	jitter := time.Duration(float64(exponentialDelay) * jitterPercent)

	finalDelay := exponentialDelay + jitter

	tm.logger.Debug().
		Str("taskID", taskID).
		Int("concurrencyRetries", retries+1).
		Dur("backoffDelay", finalDelay).
		Msg("[tms] Concurrency backoff calculated")

	return finalDelay
}

// cleanupConcurrencyRetries removes tracking for completed task
func (tm *TaskManagerSimple) cleanupConcurrencyRetries(taskID string) {
	tm.concurrencyRetryMap.Delete(taskID)
}

// cleanupTaskTracking removes all tracking for a completed/failed task
func (tm *TaskManagerSimple) cleanupTaskTracking(taskID string) {
	tm.concurrencyRetryMap.Delete(taskID)
	tm.taskToProvider.Delete(taskID)
}

// Shutdown signals and waits for all provider dispatchers
func (tm *TaskManagerSimple) Shutdown() {
	if !tm.IsRunning() {
		if tm.logger.GetLevel() <= zerolog.DebugLevel {
			tm.logger.Debug().Msg("[tms] Task manager shutdown [ALREADY STOPPED]")
		}
		return
	}
	tm.logger.Debug().Str("instance", tm.instanceName).Msg("[tms] >>> Shutdown starting")

	atomic.StoreInt32(&tm.shutdownRequest, 1)
	close(tm.shutdownCh)
	tm.logger.Debug().Str("instance", tm.instanceName).Msg("[tms] >>> Closed shutdownCh")

	// Wake all dispatchers to ensure they check shutdown
	for _, pd := range tm.providers {
		pd.taskQueueLock.Lock()
		// Use Broadcast to ensure all waiting goroutines wake up
		pd.taskQueueCond.Broadcast()
		pd.taskQueueLock.Unlock()
	}
	tm.logger.Debug().Str("instance", tm.instanceName).Msg("[tms] >>> Broadcasted to all providers")

	tm.logger.Debug().Str("instance", tm.instanceName).Msg("[tms] >>> Waiting for wg.Wait()...")
	tm.wg.Wait()
	tm.logger.Debug().Str("instance", tm.instanceName).Msg("[tms] >>> wg.Wait() complete")
	atomic.StoreInt32(&tm.isRunning, 0)

	// Distributed cleanup
	if tm.redisClient != nil {
		tm.logger.Debug().Str("instance", tm.instanceName).Msg("[tms] >>> Starting distributed cleanup")

		// Close config pubsub FIRST to unblock subscribeConfigUpdates before checking shutdownCh
		if tm.configPubSub != nil {
			tm.logger.Debug().Str("instance", tm.instanceName).Msg("[tms] >>> Closing configPubSub")
			_ = tm.configPubSub.Close()
			tm.logger.Debug().Str("instance", tm.instanceName).Msg("[tms] >>> configPubSub closed")
		}

		// Stop heartbeat
		if tm.heartbeatStop != nil {
			tm.logger.Debug().Str("instance", tm.instanceName).Msg("[tms] >>> Closing heartbeatStop")
			close(tm.heartbeatStop)
		}

		// Release master lock if we're the master
		if tm.isMaster && tm.lockRenewalStop != nil {
			tm.logger.Debug().Str("instance", tm.instanceName).Msg("[tms] >>> Closing lockRenewalStop")
			close(tm.lockRenewalStop)
			tm.releaseMasterLock()
		}

		// Release all Redis slots held by this instance
		if tm.slotCoordinator != nil {
			tm.logger.Debug().Str("instance", tm.instanceName).Msg("[tms] >>> Releasing all slots")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			tm.slotCoordinator.ReleaseAllSlots(ctx)
			cancel()
			tm.logger.Debug().Str("instance", tm.instanceName).Msg("[tms] >>> Slots released")
		}

		// Close Redis client
		tm.logger.Debug().Str("instance", tm.instanceName).Msg("[tms] >>> Closing Redis client")
		if err := tm.redisClient.Close(); err != nil {
			tm.logger.Error().
				Err(err).
				Str("instance", tm.instanceName).
				Msg("[tms|distributed] Failed to close Redis client")
		} else {
			tm.logger.Debug().
				Str("instance", tm.instanceName).
				Msg("[tms|distributed] Redis client closed")
		}
	}

	if tm.logger.GetLevel() <= zerolog.DebugLevel {
		tm.logger.Debug().Msg("[tms] Task manager shutdown [FINISHED]")
	}
}

// HandleWithTimeout wraps the provider handler in a context timeout
func (tm *TaskManagerSimple) HandleWithTimeout(
	pn string,
	task ITask,
	server string,
	handler func(ITask, string) error,
) (error, int64) {

	var err error
	taskID := task.GetID()
	callbackName := task.GetCallbackName()

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic occurred: %v", r)
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic in task", pn, taskID, server)
		}
	}()

	maxTimeout := tm.getTimeout(callbackName, pn)

	ctx := context.WithValue(context.Background(), taskIDKey, taskID)
	ctx = context.WithValue(ctx, providerNameKey, pn)
	ctx = context.WithValue(ctx, serverNameKey, server)
	ctx, cancel := context.WithTimeout(ctx, maxTimeout)
	defer cancel()

	done := make(chan error, 1)
	startTime := time.Now()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				e := fmt.Errorf("panic occurred in handler: %v\n%s", r, string(debug.Stack()))
				tm.logger.Error().Err(e).
					Msgf("[tms|%s|%s|%s] panic in handler", pn, taskID, server)
				done <- e
			}
		}()

		if tm.logger.GetLevel() <= zerolog.DebugLevel {
			tm.logger.Debug().
				Str("provider", pn).
				Str("taskID", taskID).
				Str("server", server).
				Msg("[tms|HandleWithTimeout] Task STARTED")
		}

		// Actually run the provider's handle
		done <- handler(task, server)
	}()

	var elapsed time.Duration
	select {
	case <-ctx.Done():
		now := time.Now()
		elapsed = now.Sub(startTime)
		err = fmt.Errorf(errTaskTimeout, pn, taskID, server)
		tm.logger.Error().
			Err(err).
			Msgf("[%s|%s] Task FAILED-TIMEOUT on server %s, took %s", pn, taskID, server, elapsed)
	case e := <-done:
		now := time.Now()
		elapsed = now.Sub(startTime)
		err = e
		if err == nil {
			if level := tm.logger.GetLevel(); level <= zerolog.DebugLevel {
				tm.logger.Debug().
					Str("provider", pn).
					Str("taskID", taskID).
					Str("server", server).
					Dur("duration", elapsed).
					Msg("[tms] Task COMPLETED")
			}
		} else {
			tm.logger.Error().
				Err(err).
				Str("provider", pn).
				Str("taskID", taskID).
				Str("server", server).
				Dur("duration", elapsed).
				Msg("[tms] Task FAILED")
		}
	}
	return err, elapsed.Milliseconds()
}

var (
	TaskQueueManagerInstance *TaskManagerSimple
	// Only used to guard TaskQueueManagerInstance creation/usage:
	taskManagerMutex sync.Mutex
	taskManagerCond  = sync.NewCond(&taskManagerMutex)
	addMaxRetries    = 3

	// Pool for TaskWithPriority objects
	taskWithPriorityPool = sync.Pool{
		New: func() interface{} {
			return &TaskWithPriority{}
		},
	}

	// Common error strings
	errTaskNoProvider   = "task '%s' has no provider"
	errProviderNotFound = "provider '%s' not found"
	errTaskTimeout      = "[tms|%s|%s] Task timed out on server %s"
	errPanicOccurred    = "panic occurred: %v\n%s"
	errPanicInHandler   = "panic occurred in handler: %v\n%s"
)

// InitTaskQueueManagerMaster creates and starts a master instance with Redis coordination
func InitTaskQueueManagerMaster(
	cfg ConfigOptions,
	logger *zerolog.Logger,
	providers *[]IProvider,
	tasks []ITask,
	servers map[string][]string,
	getTimeout func(string, string) time.Duration,
) error {
	taskManagerMutex.Lock()
	defer taskManagerMutex.Unlock()

	logger.Info().Msg("[tms|distributed] Master initialization starting")

	// Create task manager
	tm := NewTaskManagerSimple(providers, servers, logger, getTimeout)

	// Initialize Redis connection
	if err := tm.initRedisClient(&cfg); err != nil {
		return fmt.Errorf("failed to initialize Redis: %w", err)
	}

	// Clean up ghost instance resources from previous run
	tm.cleanupGhostInstance()

	// Initialize stop channels
	tm.heartbeatStop = make(chan struct{})
	tm.lockRenewalStop = make(chan struct{})

	// Acquire master lock
	if tm.redisClient != nil {
		if err := tm.acquireMasterLock(); err != nil {
			return fmt.Errorf("failed to acquire master lock: %w", err)
		}

		// Start lock renewal goroutine
		go tm.renewMasterLock()

		// Publish providers/servers config to Redis
		if err := tm.publishConfig(providers, servers); err != nil {
			return fmt.Errorf("failed to publish config: %w", err)
		}

		// Publish server slot limits to Redis for distributed coordination
		if tm.slotCoordinator != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			for serverURL, semaphore := range tm.serverConcurrencyMap {
				maxConcurrency := cap(semaphore)
				if err := tm.slotCoordinator.SetServerMaxConcurrency(ctx, serverURL, maxConcurrency); err != nil {
					cancel()
					return fmt.Errorf("failed to publish slot limits: %w", err)
				}
			}
			cancel()
			logger.Info().
				Str("instance", tm.instanceName).
				Int("serversPublished", len(tm.serverConcurrencyMap)).
				Msg("[tms|distributed] Published server slot limits to Redis")
		}

		// Start heartbeat
		go tm.startHeartbeat()

		// Start dead instance monitor
		go tm.monitorDeadInstances()
	}

	// Start task manager
	tm.Start()

	// Store globally
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&TaskQueueManagerInstance)), unsafe.Pointer(tm))

	logger.Info().
		Str("instance", tm.instanceName).
		Bool("isMaster", tm.isMaster).
		Msg("[tms|distributed] Master started")
	taskManagerCond.Broadcast()

	// Requeue uncompleted tasks
	RequeueTaskIfNeeded(logger, tasks)

	return nil
}

// InitTaskQueueSlave creates and starts a slave instance with optional config from Redis
func InitTaskQueueSlave(
	cfg ConfigOptions,
	logger *zerolog.Logger,
	providers *[]IProvider,
	servers map[string][]string,
	tasks []ITask,
	getTimeout func(string, string) time.Duration,
) error {
	taskManagerMutex.Lock()
	defer taskManagerMutex.Unlock()

	logger.Info().Msg("[tms|distributed] Slave initialization starting")

	var finalProviders *[]IProvider
	var finalServers map[string][]string

	// Try to load config from Redis (optional - for centralized deployment control)
	tempProviders := []IProvider{}
	tm := NewTaskManagerSimple(&tempProviders, make(map[string][]string), logger, getTimeout)

	if err := tm.initRedisClient(&cfg); err != nil {
		return fmt.Errorf("failed to initialize Redis: %w", err)
	}

	useMasterConfig := false
	if tm.redisClient != nil {
		// Try loading Master config (non-blocking, fails gracefully)
		config, err := tm.loadConfigFromRedis()
		if err == nil {
			logger.Info().
				Str("instance", tm.instanceName).
				Int64("version", config.Version).
				Msg("[tms|distributed] Loaded Master config - will match providers")

			// Build local provider map
			localProviderMap := make(map[string]IProvider)
			for _, provider := range *providers {
				localProviderMap[provider.Name()] = provider
			}

			// Match providers with Master's config
			var matchedProviders []IProvider
			matchedServers := make(map[string][]string)

			for _, providerConfig := range config.Providers {
				if localProvider, exists := localProviderMap[providerConfig.Name]; exists {
					matchedProviders = append(matchedProviders, localProvider)
					matchedServers[providerConfig.Name] = config.Servers[providerConfig.Name]
					logger.Debug().
						Str("provider", providerConfig.Name).
						Int("servers", len(config.Servers[providerConfig.Name])).
						Msg("[tms|distributed] Matched provider")
				}
			}

			if len(matchedProviders) > 0 {
				finalProviders = &matchedProviders
				finalServers = matchedServers
				useMasterConfig = true

				// Track Master-controlled servers
				if config.ServerLimits != nil && len(config.ServerLimits) > 0 {
					if tm.masterControlledServers == nil {
						tm.masterControlledServers = make(map[string]bool)
					}
					for serverURL := range config.ServerLimits {
						tm.masterControlledServers[serverURL] = true
					}
					logger.Debug().
						Str("instance", tm.instanceName).
						Int("masterServers", len(config.ServerLimits)).
						Msg("[tms|distributed] Tracked Master-controlled servers")
				}
			}
		} else {
			logger.Warn().
				Err(err).
				Msg("[tms|distributed] No Master config found - using local config")
		}
	}

	// Fallback: use local config if Master config not available
	if !useMasterConfig {
		finalProviders = providers
		finalServers = servers
		logger.Info().Msg("[tms|distributed] Using local provider/server config")
	}

	// Preserve masterControlledServers for the new TM
	masterControlledServers := tm.masterControlledServers

	// Create task manager with final config
	tm = NewTaskManagerSimple(finalProviders, finalServers, logger, getTimeout)

	// Restore masterControlledServers
	if masterControlledServers != nil {
		tm.masterControlledServers = masterControlledServers
	}

	// Re-initialize Redis (we need the client in the new instance)
	if err := tm.initRedisClient(&cfg); err != nil {
		return fmt.Errorf("failed to re-initialize Redis: %w", err)
	}

	// Clean up ghost instance resources from previous run
	tm.cleanupGhostInstance()

	tm.heartbeatStop = make(chan struct{})

	if tm.redisClient != nil {
		// Subscribe to config updates
		go tm.subscribeConfigUpdates()

		// Start heartbeat
		go tm.startHeartbeat()

		// Start dead instance monitor (slaves also monitor for cleanup)
		go tm.monitorDeadInstances()
	}

	// Start task manager
	tm.Start()

	// Store globally
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&TaskQueueManagerInstance)), unsafe.Pointer(tm))

	logger.Info().
		Str("instance", tm.instanceName).
		Bool("isMaster", tm.isMaster).
		Int("providers", len(*finalProviders)).
		Msg("[tms|distributed] Slave started with matched providers")
	taskManagerCond.Broadcast()

	// Requeue uncompleted tasks
	RequeueTaskIfNeeded(logger, tasks)

	return nil
}

// InitTaskQueueManager creates and starts the global manager (local-only mode)
func InitTaskQueueManager(
	logger *zerolog.Logger,
	providers *[]IProvider,
	tasks []ITask,
	servers map[string][]string,
	getTimeout func(string, string) time.Duration,
) {
	taskManagerMutex.Lock()
	defer taskManagerMutex.Unlock()

	logger.Info().Msg("[tms] Task manager initialization")

	tm := NewTaskManagerSimple(providers, servers, logger, getTimeout)
	tm.Start()

	// Use atomic store for lock-free access
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&TaskQueueManagerInstance)), unsafe.Pointer(tm))

	logger.Info().Msg("[tms] Task manager started")
	taskManagerCond.Broadcast()

	// Requeue any uncompleted tasks
	RequeueTaskIfNeeded(logger, tasks)
}

// RequeueTaskIfNeeded re-injects tasks that were incomplete
func RequeueTaskIfNeeded(logger *zerolog.Logger, tasks []ITask) {
	tm := GetTaskQueueManagerInstance()
	if tm == nil {
		logger.Warn().Msg("[tms] Cannot requeue tasks - manager not initialized")
		return
	}
	count, _ := tm.AddTasks(tasks)
	logger.Info().Msgf("[tms] Requeued %d. (%d tasks in queue)", count, len(tasks))
}

// AddTask is the global helper for adding tasks
func AddTask(task ITask, logger *zerolog.Logger) {
	tm := GetTaskQueueManagerInstance()
	if tm == nil {
		return
	}

	if tm.HasShutdownRequest() {
		return
	}

	// Direct add without retries for duplicates
	if !tm.AddTask(task) {
		logger.Debug().Str("taskID", task.GetID()).Msg("[tms|add-task] Task not added (duplicate)")
	}
}

// GetTaskQueueManagerInstance returns the global instance using atomic load
func GetTaskQueueManagerInstance() *TaskManagerSimple {
	return (*TaskManagerSimple)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&TaskQueueManagerInstance))))
}

// DelTask is the global helper for deleting/cancelling tasks
func DelTask(taskID string, interruptFn func(task ITask, server string) error, logger *zerolog.Logger) DelTaskResult {
	tm := GetTaskQueueManagerInstance()
	if tm == nil {
		return DelTaskErrorNotRunning
	}

	if tm.HasShutdownRequest() {
		return DelTaskErrorShuttingDown
	}

	result := tm.DelTask(taskID, interruptFn)
	logger.Debug().Str("taskID", taskID).Str("result", result.String()).Msg("[tms|del-task] Task deletion attempted")

	return result
}
