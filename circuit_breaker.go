package smt

import (
	"sync"
	"time"
)

// CircuitBreakerState represents the state of a circuit breaker
type CircuitBreakerState int

const (
	// CircuitClosed means the circuit is closed and requests flow normally
	CircuitClosed CircuitBreakerState = iota
	// CircuitOpen means the circuit is open and requests fail fast
	CircuitOpen
	// CircuitHalfOpen means the circuit is testing if service has recovered
	CircuitHalfOpen
)

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	state           CircuitBreakerState
	failureCount    int
	failureThreshold int
	resetTimeout    time.Duration
	halfOpenTimeout time.Duration
	lastFailure     time.Time
	successesNeeded int
	successes       int
	failures        int
	mu              sync.RWMutex
	onStateChange   func(from, to CircuitBreakerState)
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(failureThreshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:           CircuitClosed,
		failureThreshold: failureThreshold,
		resetTimeout:    resetTimeout,
		halfOpenTimeout: resetTimeout / 2,
		successesNeeded: 3, // Require 3 successes to close circuit
	}
}

// SetStateChangeHandler sets a callback for state changes
func (cb *CircuitBreaker) SetStateChangeHandler(handler func(from, to CircuitBreakerState)) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.onStateChange = handler
}

// Allow checks if a request should be allowed
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		// Check if we should try half-open
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			// Don't transition here to avoid lock upgrade
			// Just allow one request through
			return true
		}
		return false
	case CircuitHalfOpen:
		// Only allow one pending request when half-open
		return cb.failures+cb.successes < cb.successesNeeded
	default:
		return true
	}
}

// ReportSuccess reports a successful request
func (cb *CircuitBreaker) ReportSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	switch cb.state {
	case CircuitClosed:
		// Reset counters on success
		cb.failureCount = 0
	case CircuitOpen:
		// Transition to half-open on first success after timeout
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			oldState := cb.state
			cb.state = CircuitHalfOpen
			cb.successes = 1
			cb.failures = 0
			if cb.onStateChange != nil {
				cb.onStateChange(oldState, cb.state)
			}
		}
	case CircuitHalfOpen:
		cb.successes++
		// If we have enough successes, close the circuit
		if cb.successes >= cb.successesNeeded {
			oldState := cb.state
			cb.state = CircuitClosed
			cb.failureCount = 0
			cb.successes = 0
			cb.failures = 0
			if cb.onStateChange != nil {
				cb.onStateChange(oldState, cb.state)
			}
		}
	}
}

// ReportFailure reports a failed request
func (cb *CircuitBreaker) ReportFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	cb.lastFailure = time.Now()
	
	switch cb.state {
	case CircuitClosed:
		cb.failureCount++
		// If we hit the threshold, open the circuit
		if cb.failureCount >= cb.failureThreshold {
			oldState := cb.state
			cb.state = CircuitOpen
			if cb.onStateChange != nil {
				cb.onStateChange(oldState, cb.state)
			}
		}
	case CircuitHalfOpen:
		// Any failure in half-open state opens the circuit again
		cb.failures++
		if cb.failures > 0 {
			oldState := cb.state
			cb.state = CircuitOpen
			cb.successes = 0
			cb.failures = 0
			if cb.onStateChange != nil {
				cb.onStateChange(oldState, cb.state)
			}
		}
	}
}

// State returns the current state of the circuit breaker
func (cb *CircuitBreaker) State() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Reset forcibly resets the circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	oldState := cb.state
	cb.state = CircuitClosed
	cb.failureCount = 0
	cb.successes = 0
	cb.failures = 0
	
	if oldState != CircuitClosed && cb.onStateChange != nil {
		cb.onStateChange(oldState, cb.state)
	}
}

// CircuitBreakerManager manages circuit breakers for multiple servers
type CircuitBreakerManager struct {
	breakers        map[string]*CircuitBreaker
	mu              sync.RWMutex
	failureThreshold int
	resetTimeout    time.Duration
}

// NewCircuitBreakerManager creates a new circuit breaker manager
func NewCircuitBreakerManager(failureThreshold int, resetTimeout time.Duration) *CircuitBreakerManager {
	return &CircuitBreakerManager{
		breakers:        make(map[string]*CircuitBreaker),
		failureThreshold: failureThreshold,
		resetTimeout:    resetTimeout,
	}
}

// GetBreaker gets or creates a circuit breaker for a server
func (cbm *CircuitBreakerManager) GetBreaker(server string) *CircuitBreaker {
	// Try read lock first for better concurrency
	cbm.mu.RLock()
	if breaker, exists := cbm.breakers[server]; exists {
		cbm.mu.RUnlock()
		return breaker
	}
	cbm.mu.RUnlock()
	
	// Not found, create new breaker
	cbm.mu.Lock()
	defer cbm.mu.Unlock()
	
	// Check again in case another goroutine created it
	if breaker, exists := cbm.breakers[server]; exists {
		return breaker
	}
	
	// Create new breaker
	breaker := NewCircuitBreaker(cbm.failureThreshold, cbm.resetTimeout)
	cbm.breakers[server] = breaker
	return breaker
}

// ReportSuccess reports a successful request for a server
func (cbm *CircuitBreakerManager) ReportSuccess(server string) {
	breaker := cbm.GetBreaker(server)
	breaker.ReportSuccess()
}

// ReportFailure reports a failed request for a server
func (cbm *CircuitBreakerManager) ReportFailure(server string) {
	breaker := cbm.GetBreaker(server)
	breaker.ReportFailure()
}

// AllowRequest checks if a request should be allowed for a server
func (cbm *CircuitBreakerManager) AllowRequest(server string) bool {
	breaker := cbm.GetBreaker(server)
	return breaker.Allow()
}

// ResetBreaker resets the circuit breaker for a server
func (cbm *CircuitBreakerManager) ResetBreaker(server string) {
	breaker := cbm.GetBreaker(server)
	breaker.Reset()
}