package smt

import (
	"sync"
	"sync/atomic"
	"time"
)

// ServerStats tracks performance metrics for a server
type ServerStats struct {
	successCount   atomic.Int64
	errorCount     atomic.Int64
	timeoutCount   atomic.Int64
	responseTimeNs atomic.Int64 // Moving average of response time in nanoseconds
	lastUsed       atomic.Int64 // Unix timestamp of last usage
	weight         atomic.Int64 // Dynamic weight for server selection
}

// ServerSelectionManager manages server selection for providers
type ServerSelectionManager struct {
	stats       map[string]*ServerStats // server -> stats
	mu          sync.RWMutex
	updateCount atomic.Int64 // For occasional weight recalculation
}

// NewServerSelectionManager creates a new server selection manager
func NewServerSelectionManager() *ServerSelectionManager {
	return &ServerSelectionManager{
		stats: make(map[string]*ServerStats),
	}
}

// RegisterServer adds a server to the manager
func (ssm *ServerSelectionManager) RegisterServer(server string) {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	
	if _, exists := ssm.stats[server]; !exists {
		ssm.stats[server] = &ServerStats{}
		ssm.stats[server].weight.Store(100) // Default weight
	}
}

// RecordSuccess records a successful task execution
func (ssm *ServerSelectionManager) RecordSuccess(server string, responseTimeNs int64) {
	ssm.mu.RLock()
	stats, exists := ssm.stats[server]
	ssm.mu.RUnlock()
	
	if !exists {
		ssm.RegisterServer(server)
		ssm.mu.RLock()
		stats = ssm.stats[server]
		ssm.mu.RUnlock()
	}
	
	stats.successCount.Add(1)
	stats.lastUsed.Store(time.Now().Unix())
	
	// Update moving average for response time (weighted 70/30)
	currentAvg := stats.responseTimeNs.Load()
	if currentAvg == 0 {
		stats.responseTimeNs.Store(responseTimeNs)
	} else {
		newAvg := (currentAvg*7 + responseTimeNs*3) / 10
		stats.responseTimeNs.Store(newAvg)
	}
	
	// Occasionally update weights
	if ssm.updateCount.Add(1)%50 == 0 {
		ssm.updateServerWeights()
	}
}

// RecordError records a task execution error
func (ssm *ServerSelectionManager) RecordError(server string, isTimeout bool) {
	ssm.mu.RLock()
	stats, exists := ssm.stats[server]
	ssm.mu.RUnlock()
	
	if !exists {
		ssm.RegisterServer(server)
		ssm.mu.RLock()
		stats = ssm.stats[server]
		ssm.mu.RUnlock()
	}
	
	stats.errorCount.Add(1)
	if isTimeout {
		stats.timeoutCount.Add(1)
	}
	stats.lastUsed.Store(time.Now().Unix())
	
	// Update weights more aggressively on errors
	ssm.updateServerWeights()
}

// GetOptimalServer returns the best server based on weights
func (ssm *ServerSelectionManager) GetOptimalServer(servers []string) string {
	if len(servers) == 0 {
		return ""
	}
	
	if len(servers) == 1 {
		return servers[0]
	}
	
	// Find server with highest weight
	var bestServer string
	var bestWeight int64 = -1
	
	ssm.mu.RLock()
	defer ssm.mu.RUnlock()
	
	for _, server := range servers {
		stats, exists := ssm.stats[server]
		if !exists {
			// If we don't have stats, default to high weight to try the server
			return server
		}
		
		weight := stats.weight.Load()
		if weight > bestWeight {
			bestWeight = weight
			bestServer = server
		}
	}
	
	return bestServer
}

// updateServerWeights recalculates server weights based on performance
func (ssm *ServerSelectionManager) updateServerWeights() {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	
	for _, stats := range ssm.stats {
		successCount := stats.successCount.Load()
		errorCount := stats.errorCount.Load()
		timeoutCount := stats.timeoutCount.Load()
		responseTimeNs := stats.responseTimeNs.Load()
		
		// Skip servers with no data
		if successCount+errorCount == 0 {
			continue
		}
		
		// Calculate success rate (0-100)
		successRate := int64(100 * successCount / (successCount + errorCount))
		
		// Heavily penalize timeouts
		timeoutPenalty := int64(0)
		if timeoutCount > 0 {
			timeoutPenalty = timeoutCount * 5
		}
		
		// Response time factor (normalize to 0-50 range)
		// Lower is better
		responseTimeFactor := int64(0)
		if responseTimeNs > 0 {
			// Scale response time 
			// 0ms = 0, 100ms = 10, 1s = 50
			responseTimeFactor = min(50, responseTimeNs/(1000*1000*20))
		}
		
		// Calculate weight (0-100)
		weight := int64(100)
		if successCount+errorCount > 5 { // Only apply sophisticated weighting with enough data
			weight = min(100, max(1, successRate-timeoutPenalty-responseTimeFactor))
		}
		
		stats.weight.Store(weight)
	}
}

// min returns the minimum of two int64 values
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// max returns the maximum of two int64 values
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}