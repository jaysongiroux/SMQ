package circuit_breaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jaysongiroux/smq/internal/logger"
)

// State represents the circuit breaker state
type State string

const (
	StateClosed   State = "closed"    // Normal operation, requests allowed
	StateOpen     State = "open"      // Failing, requests blocked
	StateHalfOpen State = "half-open" // Testing recovery, limited requests allowed
)

var (
	// ErrCircuitOpen is returned when the circuit breaker is open
	ErrCircuitOpen = errors.New("circuit breaker is open")

	// ErrTimeout is returned when operation times out
	ErrTimeout = errors.New("operation timed out")
)

// CircuitBreaker implements the circuit breaker pattern with configurable thresholds
type CircuitBreaker struct {
	name string // Name for logging (e.g., "scheduler", "janitor", "buffer")
	log  *logger.Logger

	// Configuration
	maxFailures     int           // Failures before opening circuit
	timeout         time.Duration // Max time for operation
	ResetTimeout    time.Duration // Time before attempting recovery
	halfOpenMaxReqs int           // Max requests to allow in half-open state

	// State
	mu              sync.RWMutex
	state           State
	failures        int
	consecutiveSucc int
	LastFailureTime time.Time
	lastStateChange time.Time
	halfOpenReqs    int

	// Metrics
	totalRequests      int64
	totalSuccesses     int64
	totalFailures      int64
	totalTimeouts      int64
	totalCircuitOpens  int64
	totalCircuitCloses int64
}

// Config holds circuit breaker configuration
type Config struct {
	Name            string        // Name for logging
	MaxFailures     int           // Failures before opening (default: 5)
	Timeout         time.Duration // Operation timeout (default: 30s)
	ResetTimeout    time.Duration // Wait before retry (default: 60s)
	HalfOpenMaxReqs int           // Max requests in half-open (default: 1)
	Log             *logger.Logger
}

// NewCircuitBreaker creates a new circuit breaker with the given configuration
func NewCircuitBreaker(config Config) *CircuitBreaker {
	// Set defaults
	if config.MaxFailures <= 0 {
		config.MaxFailures = 5
	}
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	if config.ResetTimeout <= 0 {
		config.ResetTimeout = 60 * time.Second
	}
	if config.HalfOpenMaxReqs <= 0 {
		config.HalfOpenMaxReqs = 1
	}

	cb := &CircuitBreaker{
		name:            config.Name,
		log:             config.Log,
		maxFailures:     config.MaxFailures,
		timeout:         config.Timeout,
		ResetTimeout:    config.ResetTimeout,
		halfOpenMaxReqs: config.HalfOpenMaxReqs,
		state:           StateClosed,
		lastStateChange: time.Now(),
	}

	config.Log.Info("Circuit breaker '%s' initialized: max_failures=%d, timeout=%v, reset_timeout=%v",
		config.Name, config.MaxFailures, config.Timeout, config.ResetTimeout)

	return cb
}

// Execute runs the given function with circuit breaker protection and timeout
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func(context.Context) error) error {
	// Check if we should allow this request
	if err := cb.beforeRequest(); err != nil {
		return err
	}

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, cb.timeout)
	defer cancel()

	// Execute function in goroutine to enable timeout
	errChan := make(chan error, 1)
	go func() {
		errChan <- fn(timeoutCtx)
	}()

	// Wait for completion or timeout
	select {
	case err := <-errChan:
		// Function completed
		if err != nil {
			cb.onFailure(err)
			return err
		}
		cb.onSuccess()
		return nil

	case <-timeoutCtx.Done():
		// Timeout occurred
		cb.onTimeout()
		return fmt.Errorf("%w: %s circuit breaker timeout after %v", ErrTimeout, cb.name, cb.timeout)
	}
}

// beforeRequest checks if the request should be allowed
func (cb *CircuitBreaker) beforeRequest() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.totalRequests++

	switch cb.state {
	case StateClosed:
		// Normal operation, allow request
		return nil

	case StateOpen:
		// Check if we should transition to half-open
		if time.Since(cb.LastFailureTime) >= cb.ResetTimeout {
			cb.log.Info("Circuit breaker '%s' transitioning to half-open (testing recovery)", cb.name)
			cb.setState(StateHalfOpen)
			cb.halfOpenReqs = 0
			return nil
		}
		// Still open, reject request
		return fmt.Errorf("%w: %s circuit breaker open (failed %d times, last failure %v ago)",
			ErrCircuitOpen, cb.name, cb.failures, time.Since(cb.LastFailureTime).Round(time.Second))

	case StateHalfOpen:
		// Allow limited requests to test recovery
		if cb.halfOpenReqs >= cb.halfOpenMaxReqs {
			return fmt.Errorf("%w: %s circuit breaker half-open (testing in progress)",
				ErrCircuitOpen, cb.name)
		}
		cb.halfOpenReqs++
		return nil

	default:
		return fmt.Errorf("unknown circuit breaker state: %s", cb.state)
	}
}

// onSuccess records a successful operation
func (cb *CircuitBreaker) onSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.totalSuccesses++

	switch cb.state {
	case StateClosed:
		// Reset failure count on success
		cb.failures = 0

	case StateHalfOpen:
		// Success in half-open, increment consecutive successes
		cb.consecutiveSucc++
		cb.log.Debug("Circuit breaker '%s' half-open success (%d/%d)",
			cb.name, cb.consecutiveSucc, cb.halfOpenMaxReqs)

		// If enough successes, close the circuit
		if cb.consecutiveSucc >= cb.halfOpenMaxReqs {
			cb.log.Info("Circuit breaker '%s' closing (recovered after %v)",
				cb.name, time.Since(cb.lastStateChange).Round(time.Second))
			cb.setState(StateClosed)
			cb.failures = 0
			cb.consecutiveSucc = 0
			cb.totalCircuitCloses++
		}
	}
}

// onFailure records a failed operation
func (cb *CircuitBreaker) onFailure(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.totalFailures++
	cb.failures++
	cb.LastFailureTime = time.Now()

	cb.log.Warn("Circuit breaker '%s' failure (%d/%d): %v",
		cb.name, cb.failures, cb.maxFailures, err)

	switch cb.state {
	case StateClosed:
		// Check if we should open the circuit
		if cb.failures >= cb.maxFailures {
			cb.log.Error("Circuit breaker '%s' opening due to %d consecutive failures",
				cb.name, cb.failures)
			cb.setState(StateOpen)
			cb.totalCircuitOpens++
		}

	case StateHalfOpen:
		// Failure in half-open, go back to open
		cb.log.Warn("Circuit breaker '%s' re-opening (recovery failed)", cb.name)
		cb.setState(StateOpen)
		cb.consecutiveSucc = 0
		cb.totalCircuitOpens++
	}
}

// onTimeout records a timeout
func (cb *CircuitBreaker) onTimeout() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.totalTimeouts++
	cb.totalFailures++
	cb.failures++
	cb.LastFailureTime = time.Now()

	cb.log.Warn("Circuit breaker '%s' timeout (%d/%d) after %v",
		cb.name, cb.failures, cb.maxFailures, cb.timeout)

	// Treat timeouts like failures
	if cb.state == StateClosed && cb.failures >= cb.maxFailures {
		cb.log.Error("Circuit breaker '%s' opening due to %d consecutive timeouts/failures",
			cb.name, cb.failures)
		cb.setState(StateOpen)
		cb.totalCircuitOpens++
	} else if cb.state == StateHalfOpen {
		cb.log.Warn("Circuit breaker '%s' re-opening (recovery timed out)", cb.name)
		cb.setState(StateOpen)
		cb.consecutiveSucc = 0
		cb.totalCircuitOpens++
	}
}

// setState changes the circuit breaker state (must be called with lock held)
func (cb *CircuitBreaker) setState(newState State) {
	if cb.state != newState {
		cb.log.Info("Circuit breaker '%s' state change: %s -> %s",
			cb.name, cb.state, newState)
		cb.state = newState
		cb.lastStateChange = time.Now()
	}
}

// GetState returns the current state (thread-safe)
func (cb *CircuitBreaker) GetState() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// GetMetrics returns current metrics (thread-safe)
func (cb *CircuitBreaker) GetMetrics() Metrics {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	successRate := float64(0)
	if cb.totalRequests > 0 {
		successRate = float64(cb.totalSuccesses) / float64(cb.totalRequests) * 100
	}

	return Metrics{
		Name:                cb.name,
		State:               cb.state,
		TotalRequests:       cb.totalRequests,
		TotalSuccesses:      cb.totalSuccesses,
		TotalFailures:       cb.totalFailures,
		TotalTimeouts:       cb.totalTimeouts,
		TotalCircuitOpens:   cb.totalCircuitOpens,
		TotalCircuitCloses:  cb.totalCircuitCloses,
		SuccessRate:         successRate,
		ConsecutiveFailures: cb.failures,
		LastFailureTime:     cb.LastFailureTime,
		LastStateChange:     cb.lastStateChange,
	}
}

// Metrics holds circuit breaker metrics
type Metrics struct {
	Name                string
	State               State
	TotalRequests       int64
	TotalSuccesses      int64
	TotalFailures       int64
	TotalTimeouts       int64
	TotalCircuitOpens   int64
	TotalCircuitCloses  int64
	SuccessRate         float64
	ConsecutiveFailures int
	LastFailureTime     time.Time
	LastStateChange     time.Time
}

// Reset manually resets the circuit breaker to closed state
// Useful for testing or manual intervention
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.log.Info("Circuit breaker '%s' manually reset", cb.name)
	cb.state = StateClosed
	cb.failures = 0
	cb.consecutiveSucc = 0
	cb.lastStateChange = time.Now()
}
