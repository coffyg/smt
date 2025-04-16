package smt

import (
	"sync"
	"time"
)

// AdaptiveTimeoutStats tracks execution statistics for a specific task type
type AdaptiveTimeoutStats struct {
	count             int64         // Number of executions tracked
	sumDuration       int64         // Sum of all durations in milliseconds
	avgDuration       int64         // Average duration in milliseconds
	minDuration       int64         // Minimum duration seen in milliseconds
	maxDuration       int64         // Maximum duration seen in milliseconds
	lastUpdated       time.Time     // When this stat was last updated
	lastActualTimeout time.Duration // Last actual timeout used
}

// AdaptiveTimeoutManager manages adaptive timeouts for tasks
type AdaptiveTimeoutManager struct {
	baseTimeouts   map[string]map[string]time.Duration // Provider -> callback -> base timeout
	stats          map[string]map[string]*AdaptiveTimeoutStats // Provider -> callback -> stats
	lock           sync.RWMutex
	enabled        bool
	
	// Configuration options
	minMultiplier   float64 // Minimum multiplier for base timeout
	maxMultiplier   float64 // Maximum multiplier for base timeout
	adjustmentRate  float64 // How quickly to adjust timeouts (0.0-1.0)
	safetyMargin    float64 // Extra margin added to computed timeout (e.g. 1.5 = 50% extra)
	minSampleSize   int64   // Minimum sample size before adjusting timeouts
	decayInterval   time.Duration // How often to decay old stats
}

// NewAdaptiveTimeoutManager creates a new AdaptiveTimeoutManager
func NewAdaptiveTimeoutManager(enabled bool) *AdaptiveTimeoutManager {
	manager := &AdaptiveTimeoutManager{
		baseTimeouts:  make(map[string]map[string]time.Duration),
		stats:         make(map[string]map[string]*AdaptiveTimeoutStats),
		enabled:       enabled,
		minMultiplier: 0.5,   // Min timeout is 50% of base
		maxMultiplier: 3.0,   // Max timeout is 300% of base
		adjustmentRate: 0.2,  // Adjust by 20% each time
		safetyMargin:  1.5,   // Add 50% safety margin
		minSampleSize: 5,     // Need at least 5 samples
		decayInterval: 24 * time.Hour, // Decay stats once per day
	}
	
	// Set global instance
	globalAdaptiveTimeoutManager = manager
	
	return manager
}

// RegisterBaseTimeout registers a base timeout for a provider/callback pair
func (m *AdaptiveTimeoutManager) RegisterBaseTimeout(provider, callback string, timeout time.Duration) {
	m.lock.Lock()
	defer m.lock.Unlock()
	
	// Initialize maps if they don't exist
	if _, exists := m.baseTimeouts[provider]; !exists {
		m.baseTimeouts[provider] = make(map[string]time.Duration)
	}
	
	// Store the base timeout
	m.baseTimeouts[provider][callback] = timeout
}

// Cache for adaptive timeouts to reduce lock contention
type timeoutCacheEntry struct {
	timeout    time.Duration
	expiration time.Time
}

// Cached timeouts for frequently accessed provider+callback combinations
var timeoutCache sync.Map // map[string]timeoutCacheEntry

// Cache TTL for timeouts (1 second)
const timeoutCacheTTL = 1 * time.Second

// getCacheKey creates a cache key for the provider and callback
func getCacheKey(provider, callback string) string {
	return provider + ":" + callback
}

// GetTimeout returns the appropriate timeout for a task
func (m *AdaptiveTimeoutManager) GetTimeout(callback, provider string) time.Duration {
	// Fast path for disabled adaptive timeouts
	if !m.enabled {
		// Fallback to base timeout if adaptive timeouts are disabled
		return m.getBaseTimeout(provider, callback)
	}
	
	// Check cache first to avoid lock contention
	cacheKey := getCacheKey(provider, callback)
	if entry, ok := timeoutCache.Load(cacheKey); ok {
		cachedEntry := entry.(timeoutCacheEntry)
		if time.Now().Before(cachedEntry.expiration) {
			// Cache hit and still valid
			return cachedEntry.timeout
		}
		// Cache expired, will recalculate
	}
	
	// Cache miss or expired - calculate timeout
	// Fast path with read lock first to avoid expensive RWLock.Lock
	m.lock.RLock()
	
	// Get base timeout
	baseTimeout := m.getBaseTimeoutLocked(provider, callback)
	
	// Get stats if available
	stats, exists := m.getStatsLocked(provider, callback)
	
	// If we have insufficient data, return early with base timeout
	if !exists || stats.count < m.minSampleSize {
		// Not enough data to adjust, return base timeout
		m.lock.RUnlock()
		
		// Cache the result
		timeoutCache.Store(cacheKey, timeoutCacheEntry{
			timeout:    baseTimeout,
			expiration: time.Now().Add(timeoutCacheTTL),
		})
		
		return baseTimeout
	}
	
	// Quickly grab the stats values we need under the lock
	avgDurationMs := stats.avgDuration
	
	// Cache configuration parameters to avoid lock contention
	minMultiplier := m.minMultiplier
	maxMultiplier := m.maxMultiplier
	safetyMargin := m.safetyMargin
	
	// We have all the data we need, release read lock
	m.lock.RUnlock()
	
	// Fast path for invalid averages
	if avgDurationMs <= 0 {
		// Cache the result
		timeoutCache.Store(cacheKey, timeoutCacheEntry{
			timeout:    baseTimeout,
			expiration: time.Now().Add(timeoutCacheTTL),
		})
		
		return baseTimeout
	}
	
	// Calculate actual timeout outside of lock
	avgDuration := time.Duration(avgDurationMs) * time.Millisecond
	adjustedTimeout := time.Duration(float64(avgDuration) * safetyMargin)
	
	// Apply min/max constraints based on base timeout (outside lock)
	minTimeout := time.Duration(float64(baseTimeout) * minMultiplier)
	maxTimeout := time.Duration(float64(baseTimeout) * maxMultiplier)
	
	if adjustedTimeout < minTimeout {
		adjustedTimeout = minTimeout
	} else if adjustedTimeout > maxTimeout {
		adjustedTimeout = maxTimeout
	}
	
	// Cache the result
	timeoutCache.Store(cacheKey, timeoutCacheEntry{
		timeout:    adjustedTimeout,
		expiration: time.Now().Add(timeoutCacheTTL),
	})
	
	return adjustedTimeout
}

// execRecord represents a pending execution record
type execRecord struct {
	provider   string
	callback   string
	durationMs int64
	timeout    time.Duration
	timestamp  time.Time
}

// execRecordChan is a buffered channel for background processing of execution records
var execRecordChan = make(chan execRecord, 1000)

// global adaptive timeout manager instance
var globalAdaptiveTimeoutManager *AdaptiveTimeoutManager

// init starts the background execution record processor
func init() {
	go processExecRecords()
}

// processExecRecords processes execution records in the background
func processExecRecords() {
	recordBatch := make([]execRecord, 0, 100)
	
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case record := <-execRecordChan:
			recordBatch = append(recordBatch, record)
			
			// Process in batches for efficiency
			if len(recordBatch) >= 100 && globalAdaptiveTimeoutManager != nil {
				processRecordBatch(globalAdaptiveTimeoutManager, recordBatch)
				recordBatch = recordBatch[:0] // Clear batch
			}
			
		case <-ticker.C:
			// Process any remaining records periodically
			if len(recordBatch) > 0 && globalAdaptiveTimeoutManager != nil {
				processRecordBatch(globalAdaptiveTimeoutManager, recordBatch)
				recordBatch = recordBatch[:0] // Clear batch
			}
		}
	}
}

// processRecordBatch processes a batch of execution records
func processRecordBatch(m *AdaptiveTimeoutManager, records []execRecord) {
	// Skip if adaptive timeouts are disabled
	if !m.enabled {
		return
	}
	
	m.lock.Lock()
	defer m.lock.Unlock()
	
	for _, record := range records {
		if record.durationMs <= 0 {
			continue
		}
		
		// Initialize maps if they don't exist
		if _, exists := m.stats[record.provider]; !exists {
			m.stats[record.provider] = make(map[string]*AdaptiveTimeoutStats)
		}
		
		// Get or create stats
		stats, exists := m.stats[record.provider][record.callback]
		if !exists {
			stats = &AdaptiveTimeoutStats{
				minDuration:       record.durationMs,
				maxDuration:       record.durationMs,
				lastActualTimeout: record.timeout,
			}
			m.stats[record.provider][record.callback] = stats
		}
		
		// Update stats
		stats.count++
		stats.sumDuration += record.durationMs
		stats.avgDuration = stats.sumDuration / stats.count
		
		if record.durationMs < stats.minDuration {
			stats.minDuration = record.durationMs
		}
		if record.durationMs > stats.maxDuration {
			stats.maxDuration = record.durationMs
		}
		
		stats.lastUpdated = record.timestamp
		stats.lastActualTimeout = record.timeout
		
		// Invalidate cache for this entry
		timeoutCache.Delete(getCacheKey(record.provider, record.callback))
	}
}

// RecordExecution records the execution time of a task
func (m *AdaptiveTimeoutManager) RecordExecution(provider, callback string, durationMs int64, timeout time.Duration) {
	if !m.enabled || durationMs <= 0 {
		return
	}
	
	// Send execution record to background worker via non-blocking channel
	select {
	case execRecordChan <- execRecord{
		provider:   provider,
		callback:   callback,
		durationMs: durationMs,
		timeout:    timeout,
		timestamp:  time.Now(),
	}:
		// Record sent successfully
	default:
		// Channel full, drop record
		// This is acceptable as we prioritize performance over recording every execution
	}
}

// getBaseTimeoutLocked gets the base timeout (with lock already held)
func (m *AdaptiveTimeoutManager) getBaseTimeoutLocked(provider, callback string) time.Duration {
	// Check if we have a specific timeout for this provider/callback
	if providerMap, exists := m.baseTimeouts[provider]; exists {
		if timeout, found := providerMap[callback]; found {
			return timeout
		}
	}
	
	// Check if we have a default for this provider
	if providerMap, exists := m.baseTimeouts[provider]; exists {
		if timeout, found := providerMap[""]; found {
			return timeout
		}
	}
	
	// Check if we have a default for this callback across all providers
	if providerMap, exists := m.baseTimeouts[""]; exists {
		if timeout, found := providerMap[callback]; found {
			return timeout
		}
	}
	
	// Return global default
	if providerMap, exists := m.baseTimeouts[""]; exists {
		if timeout, found := providerMap[""]; found {
			return timeout
		}
	}
	
	// No timeout found, return a reasonable default
	return 5 * time.Second
}

// getBaseTimeout gets the base timeout
func (m *AdaptiveTimeoutManager) getBaseTimeout(provider, callback string) time.Duration {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.getBaseTimeoutLocked(provider, callback)
}

// getStatsLocked gets stats for a provider/callback (with lock already held)
func (m *AdaptiveTimeoutManager) getStatsLocked(provider, callback string) (*AdaptiveTimeoutStats, bool) {
	// Check if we have specific stats for this provider/callback
	if providerMap, exists := m.stats[provider]; exists {
		if stats, found := providerMap[callback]; found {
			return stats, true
		}
	}
	
	return nil, false
}

// DecayOldStats reduces the weight of old statistics
func (m *AdaptiveTimeoutManager) DecayOldStats() {
	m.lock.Lock()
	defer m.lock.Unlock()
	
	now := time.Now()
	
	// Iterate through all stats
	for _, providerMap := range m.stats {
		for _, stats := range providerMap {
			// Check if stats are old enough to decay
			if now.Sub(stats.lastUpdated) > m.decayInterval && stats.count > 0 {
				// Decay by halving the count and sum, keeping the same average
				stats.count = stats.count / 2
				if stats.count < 1 {
					stats.count = 1
				}
				
				stats.sumDuration = stats.avgDuration * stats.count
				
				// Update last updated time
				stats.lastUpdated = now
			}
		}
	}
}

// Reset clears all stats but keeps the base timeouts
func (m *AdaptiveTimeoutManager) Reset() {
	m.lock.Lock()
	defer m.lock.Unlock()
	
	// Clear stats but keep base timeouts
	m.stats = make(map[string]map[string]*AdaptiveTimeoutStats)
}

// SetEnabled enables or disables adaptive timeouts
func (m *AdaptiveTimeoutManager) SetEnabled(enabled bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.enabled = enabled
}

// IsEnabled returns whether adaptive timeouts are enabled
func (m *AdaptiveTimeoutManager) IsEnabled() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.enabled
}