package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jaysongiroux/smq/internal/circuit_breaker"
	"github.com/jaysongiroux/smq/internal/db"
	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/models"
	"github.com/jaysongiroux/smq/internal/testutils"
)

// Test helper
func newTestJanitorScheduler(store db.Store) *Scheduler {
	log := logger.New("test", createTestConfig())

	cfg := createTestSchedulerConfig()
	cfg.CBMaxFailures = 1
	cfg.CBTimeout = 20 * time.Millisecond
	cfg.CBResetTimeout = 20 * time.Millisecond
	cfg.HalfOpenMaxReqs = 1 // IMPORTANT for correct behavior

	return NewScheduler(cfg, store, 0, 1, log)
}

func TestJanitorNode_Initialization(t *testing.T) {
	t.Run("creates janitor node with correct initial state", func(t *testing.T) {
		node := &JanitorNode{
			id: 5,
		}

		if node.id != 5 {
			t.Errorf("Expected node id 5, got %d", node.id)
		}

		if node.isRunning {
			t.Error("Expected node to not be running initially")
		}

		if node.messagesCleanedUp != 0 {
			t.Errorf("Expected 0 messages cleaned up, got %d", node.messagesCleanedUp)
		}

		if node.nodesCleanedUp != 0 {
			t.Errorf("Expected 0 nodes cleaned up, got %d", node.nodesCleanedUp)
		}

		if node.lastError != nil {
			t.Errorf("Expected no error, got %v", node.lastError)
		}
	})
}

func TestJanitorNode_CleanupStaleMessages(t *testing.T) {
	t.Run("successfully cleans up stale messages", func(t *testing.T) {
		store := &testutils.MockStore{MarkStaleCount: 12}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		// Initial state
		node.mu.RLock()
		initialCount := node.messagesCleanedUp
		node.mu.RUnlock()

		if initialCount != 0 {
			t.Errorf("Expected initial count 0, got %d", initialCount)
		}

		// Perform cleanup
		scheduler.cleanupStaleMessages(node)

		// Verify state updated
		node.mu.RLock()
		finalCount := node.messagesCleanedUp
		lastRun := node.lastRun
		lastError := node.lastError
		node.mu.RUnlock()

		if finalCount != 12 {
			t.Errorf("Expected 12 messages cleaned up, got %d", finalCount)
		}

		if lastRun.IsZero() {
			t.Error("Expected lastRun to be set")
		}

		if lastError != nil {
			t.Errorf("Expected no error, got %v", lastError)
		}
	})

	t.Run("accumulates cleanup counts across multiple runs", func(t *testing.T) {
		store := &testutils.MockStore{MarkStaleCount: 5}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		// Run cleanup multiple times
		scheduler.cleanupStaleMessages(node)
		scheduler.cleanupStaleMessages(node)
		scheduler.cleanupStaleMessages(node)

		node.mu.RLock()
		totalCount := node.messagesCleanedUp
		node.mu.RUnlock()

		expectedTotal := int64(15) // 5 * 3
		if totalCount != expectedTotal {
			t.Errorf("Expected %d total messages cleaned up, got %d", expectedTotal, totalCount)
		}

		store.Mu.Lock()
		calls := store.MarkStaleCalls
		store.Mu.Unlock()

		if calls != 3 {
			t.Errorf("Expected 3 calls to store, got %d", calls)
		}
	})

	t.Run("handles cleanup errors without updating count", func(t *testing.T) {
		testError := errors.New("database connection lost")
		store := &testutils.MockStore{
			MarkStaleCount: 5,
			MarkStaleError: testError,
		}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		scheduler.cleanupStaleMessages(node)

		node.mu.RLock()
		count := node.messagesCleanedUp
		lastError := node.lastError
		lastRun := node.lastRun
		node.mu.RUnlock()

		if count != 0 {
			t.Errorf("Expected count to remain 0 on error, got %d", count)
		}

		if lastError == nil {
			t.Error("Expected error to be recorded")
		}

		if !errors.Is(lastError, testError) {
			t.Errorf("Expected error %v, got %v", testError, lastError)
		}

		if lastRun.IsZero() {
			t.Error("Expected lastRun to be updated even on error")
		}
	})

	t.Run("clears previous error on successful cleanup", func(t *testing.T) {
		store := &testutils.MockStore{MarkStaleCount: 3}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		// Set an error manually
		node.mu.Lock()
		node.lastError = errors.New("previous error")
		node.mu.Unlock()

		// Perform successful cleanup
		scheduler.cleanupStaleMessages(node)

		node.mu.RLock()
		lastError := node.lastError
		node.mu.RUnlock()

		if lastError != nil {
			t.Errorf("Expected error to be cleared, got %v", lastError)
		}
	})
}

func TestJanitorNode_CleanupStaleNodes(t *testing.T) {
	t.Run("successfully cleans up stale nodes", func(t *testing.T) {
		store := &testutils.MockStore{DeleteStaleNodesCount: 7}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		scheduler.cleanupStaleNodes(node)

		node.mu.RLock()
		count := node.nodesCleanedUp
		lastError := node.lastError
		node.mu.RUnlock()

		if count != 7 {
			t.Errorf("Expected 7 nodes cleaned up, got %d", count)
		}

		if lastError != nil {
			t.Errorf("Expected no error, got %v", lastError)
		}
	})

	t.Run("accumulates node cleanup counts", func(t *testing.T) {
		store := &testutils.MockStore{DeleteStaleNodesCount: 2}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		// Run multiple cleanups
		for i := 0; i < 5; i++ {
			scheduler.cleanupStaleNodes(node)
		}

		node.mu.RLock()
		totalCount := node.nodesCleanedUp
		node.mu.RUnlock()

		expectedTotal := int64(10) // 2 * 5
		if totalCount != expectedTotal {
			t.Errorf("Expected %d total nodes cleaned up, got %d", expectedTotal, totalCount)
		}
	})

	t.Run("handles node cleanup errors", func(t *testing.T) {
		testError := errors.New("permission denied")
		store := &testutils.MockStore{
			DeleteStaleNodesCount: 3,
			DeleteStaleNodesError: testError,
		}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		scheduler.cleanupStaleNodes(node)

		node.mu.RLock()
		count := node.nodesCleanedUp
		lastError := node.lastError
		node.mu.RUnlock()

		if count != 0 {
			t.Errorf("Expected count to remain 0 on error, got %d", count)
		}

		if !errors.Is(lastError, testError) {
			t.Errorf("Expected error %v, got %v", testError, lastError)
		}
	})
}

func TestJanitorNode_CleanupFailedMessages(t *testing.T) {
	t.Run("successfully cleans up failed messages", func(t *testing.T) {
		store := &testutils.MockStore{CleanFailedMessagesCount: 15}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		scheduler.cleanupFailedMessages(node)

		store.Mu.Lock()
		calls := store.CleanFailedMessagesCalls
		store.Mu.Unlock()

		if calls != 1 {
			t.Errorf("Expected 1 call to CleanFailedMessages, got %d", calls)
		}
	})

	t.Run("handles failed message cleanup errors", func(t *testing.T) {
		testError := errors.New("cleanup failed")
		store := &testutils.MockStore{
			CleanFailedMessagesError: testError,
		}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		// This should log error but not crash
		scheduler.cleanupFailedMessages(node)

		store.Mu.Lock()
		calls := store.CleanFailedMessagesCalls
		store.Mu.Unlock()

		if calls != 1 {
			t.Errorf("Expected 1 call even on error, got %d", calls)
		}
	})
}

func TestJanitorNode_ConcurrentAccess(t *testing.T) {
	t.Run("handles concurrent stat updates safely", func(t *testing.T) {
		store := &testutils.MockStore{
			MarkStaleCount:        1,
			DeleteStaleNodesCount: 1,
		}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		// Run many concurrent cleanups
		var wg sync.WaitGroup
		iterations := 50

		wg.Add(iterations)
		for i := 0; i < iterations; i++ {
			go func() {
				defer wg.Done()
				scheduler.cleanupStaleMessages(node)
			}()
		}

		wg.Add(iterations)
		for i := 0; i < iterations; i++ {
			go func() {
				defer wg.Done()
				scheduler.cleanupStaleNodes(node)
			}()
		}

		wg.Wait()

		node.mu.RLock()
		messagesCount := node.messagesCleanedUp
		nodesCount := node.nodesCleanedUp
		node.mu.RUnlock()

		expectedMessages := int64(iterations)
		expectedNodes := int64(iterations)

		if messagesCount != expectedMessages {
			t.Errorf("Expected %d messages cleaned up, got %d", expectedMessages, messagesCount)
		}

		if nodesCount != expectedNodes {
			t.Errorf("Expected %d nodes cleaned up, got %d", expectedNodes, nodesCount)
		}
	})

	t.Run("health check is thread-safe during cleanups", func(t *testing.T) {
		store := &testutils.MockStore{MarkStaleCount: 1}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 3, log)

		var wg sync.WaitGroup
		stopChan := make(chan bool)

		// Run cleanups continuously
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(nodeIdx int) {
				defer wg.Done()
				node := scheduler.janitorNodes[nodeIdx]
				for {
					select {
					case <-stopChan:
						return
					default:
						scheduler.cleanupStaleMessages(node)
						time.Sleep(1 * time.Millisecond)
					}
				}
			}(i)
		}

		// Check health concurrently
		for i := 0; i < 10; i++ {
			health := scheduler.getJanitorHealth()
			if len(health) != 3 {
				t.Errorf("Expected 3 health reports, got %d", len(health))
			}
		}

		close(stopChan)
		wg.Wait()
	})
}

func TestJanitorNode_JanitorLoop(t *testing.T) {
	t.Run("janitor loop runs periodic cleanup", func(t *testing.T) {
		store := &testutils.MockStore{
			MarkStaleCount:        2,
			DeleteStaleNodesCount: 1,
		}
		log := logger.New("test", createTestConfig())

		// Use fast interval for testing
		config := &SchedulerConfig{
			PollInterval:           1 * time.Second,
			PollJitterPercent:      10,
			JanitorInterval:        50 * time.Millisecond,
			JanitorJitterPercent:   10,
			StaleAcquiredThreshold: 5 * time.Minute,
			StaleNodeThreshold:     10 * time.Minute,
		}

		scheduler := NewScheduler(config, store, 0, 1, log)
		scheduler.Start()

		// Let it run for multiple cycles
		time.Sleep(200 * time.Millisecond)

		scheduler.Stop()

		store.Mu.Lock()
		messageCalls := store.MarkStaleCalls
		nodeCalls := store.DeleteStaleNodesCalls
		failedCalls := store.CleanFailedMessagesCalls
		store.Mu.Unlock()

		// Should have run at least 3 times in 200ms with 50ms interval
		if messageCalls < 3 {
			t.Errorf("Expected at least 3 message cleanup calls, got %d", messageCalls)
		}

		if nodeCalls < 3 {
			t.Errorf("Expected at least 3 node cleanup calls, got %d", nodeCalls)
		}

		// Failed messages cleanup runs once at start
		if failedCalls < 1 {
			t.Errorf("Expected at least 1 failed message cleanup call, got %d", failedCalls)
		}
	})

	t.Run("janitor loop stops gracefully on context cancel", func(t *testing.T) {
		store := &testutils.MockStore{MarkStaleCount: 1}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 2, log)
		scheduler.Start()

		time.Sleep(100 * time.Millisecond)

		// Stop should wait for all goroutines to finish
		err := scheduler.Stop()
		if err != nil {
			t.Errorf("Expected no error stopping scheduler, got: %v", err)
		}

		// All nodes should be stopped
		for i, node := range scheduler.janitorNodes {
			node.mu.RLock()
			isRunning := node.isRunning
			node.mu.RUnlock()

			if isRunning {
				t.Errorf("Janitor node %d should be stopped", i)
			}
		}
	})
}

func TestJanitorNode_HealthReporting(t *testing.T) {
	t.Run("reports detailed health metadata", func(t *testing.T) {
		store := &testutils.MockStore{
			MarkStaleCount:        10,
			DeleteStaleNodesCount: 5,
		}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		// Perform some cleanups
		scheduler.cleanupStaleMessages(node)
		scheduler.cleanupStaleNodes(node)

		// Mark as running
		node.mu.Lock()
		node.isRunning = true
		node.mu.Unlock()

		health := scheduler.getJanitorHealth()

		if h, ok := health["janitor-node-0"]; ok {
			if h.Status != models.HealthStatusHealthy {
				t.Errorf("Expected healthy status, got %s", h.Status)
			}

			metadata := h.Metadata
			if metadata["is_running"] != true {
				t.Error("Expected is_running to be true")
			}

			if metadata["messages_cleaned_up"] != int64(10) {
				t.Errorf("Expected 10 messages cleaned up, got %v", metadata["messages_cleaned_up"])
			}

			if metadata["nodes_cleaned_up"] != int64(5) {
				t.Errorf("Expected 5 nodes cleaned up, got %v", metadata["nodes_cleaned_up"])
			}

			if metadata["last_error"] != nil {
				t.Errorf("Expected no error, got %v", metadata["last_error"])
			}
		} else {
			t.Error("Expected health report for janitor-node-0")
		}
	})

	t.Run("includes error in health metadata", func(t *testing.T) {
		testError := errors.New("test error")
		store := &testutils.MockStore{MarkStaleError: testError}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		node.mu.Lock()
		node.isRunning = true
		node.mu.Unlock()

		// Trigger error
		scheduler.cleanupStaleMessages(node)

		health := scheduler.getJanitorHealth()

		if h, ok := health["janitor-node-0"]; ok {
			if h.Status != models.HealthStatusDegraded {
				t.Errorf("Expected degraded status, got %s", h.Status)
			}

			metadata := h.Metadata
			if metadata["last_error"] == nil {
				t.Error("Expected error to be present in metadata")
			}

			errorStr, ok := metadata["last_error"].(string)
			if !ok {
				t.Error("Expected error to be a string")
			}

			if errorStr != testError.Error() {
				t.Errorf("Expected error %q, got %q", testError.Error(), errorStr)
			}
		}
	})

	t.Run("reports correct status for multiple janitor nodes", func(t *testing.T) {
		store := &testutils.MockStore{MarkStaleCount: 1}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 5, log)

		// Set different states for each node
		now := time.Now()
		for i, node := range scheduler.janitorNodes {
			node.mu.Lock()
			if i < 3 {
				node.isRunning = true
				node.lastRun = now // Set recent lastRun so nodes don't appear delayed
			}
			if i == 2 {
				node.lastError = fmt.Errorf("node %d error", i)
			}
			node.mu.Unlock()
		}

		health := scheduler.getJanitorHealth()

		if len(health) != 5 {
			t.Errorf("Expected 5 health reports, got %d", len(health))
		}

		// Check specific node statuses
		if h, ok := health["janitor-node-0"]; ok {
			if h.Status != models.HealthStatusHealthy {
				t.Errorf("Node 0: expected healthy, got %s", h.Status)
			}
		}

		if h, ok := health["janitor-node-2"]; ok {
			if h.Status != models.HealthStatusDegraded {
				t.Errorf("Node 2: expected degraded (has error), got %s", h.Status)
			}
		}

		if h, ok := health["janitor-node-3"]; ok {
			if h.Status != models.HealthStatusUnhealthy {
				t.Errorf("Node 3: expected unhealthy (not running), got %s", h.Status)
			}
		}
	})
}

func TestJanitorNode_ErrorRecovery(t *testing.T) {
	t.Run("recovers from transient errors", func(t *testing.T) {
		store := &testutils.MockStore{MarkStaleCount: 5}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		// First call fails
		store.Mu.Lock()
		store.MarkStaleError = errors.New("transient error")
		store.Mu.Unlock()

		scheduler.cleanupStaleMessages(node)

		node.mu.RLock()
		firstError := node.lastError
		firstCount := node.messagesCleanedUp
		node.mu.RUnlock()

		if firstError == nil {
			t.Error("Expected error to be recorded")
		}

		if firstCount != 0 {
			t.Errorf("Expected count to be 0 after error, got %d", firstCount)
		}

		// Second call succeeds
		store.Mu.Lock()
		store.MarkStaleError = nil
		store.Mu.Unlock()

		scheduler.cleanupStaleMessages(node)

		node.mu.RLock()
		secondError := node.lastError
		secondCount := node.messagesCleanedUp
		node.mu.RUnlock()

		if secondError != nil {
			t.Errorf("Expected error to be cleared, got %v", secondError)
		}

		if secondCount != 5 {
			t.Errorf("Expected count to be 5 after recovery, got %d", secondCount)
		}
	})
}

func TestJanitorCircuitBreaker_SkipsWhenOpen(t *testing.T) {
	mock := &testutils.MockStore{}
	s := newTestJanitorScheduler(mock)
	node := s.janitorNodes[0]

	// Force open
	node.circuitBreaker.Execute(s.ctx, func(ctx context.Context) error { return errors.New("fail") })
	if node.circuitBreaker.GetState() != circuit_breaker.StateOpen {
		t.Fatalf("expected open")
	}

	calls := 0
	s.genericJanitorCircuitBreaker(node, func(n *JanitorNode) { calls++ })

	if calls != 0 {
		t.Fatalf("expected zero calls when circuit open")
	}
}

func TestJanitorCircuitBreaker_AllowsWhenClosed(t *testing.T) {
	mock := &testutils.MockStore{}
	s := newTestJanitorScheduler(mock)

	node := s.janitorNodes[0]

	calls := 0
	s.genericJanitorCircuitBreaker(node, func(n *JanitorNode) {
		calls++
	})

	if calls != 1 {
		t.Fatalf("expected exactly one call")
	}
}

func TestJanitorCircuitBreaker_TransitionsOpenThenHalfOpenThenClosed(t *testing.T) {
	mock := &testutils.MockStore{}
	s := newTestJanitorScheduler(mock)
	node := s.janitorNodes[0]

	// Force circuit breaker open
	node.circuitBreaker.Execute(s.ctx, func(ctx context.Context) error { return errors.New("fail") })
	if node.circuitBreaker.GetState() != circuit_breaker.StateOpen {
		t.Fatalf("expected open")
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	calls := 0
	transitioned := false

	// Poll until half-open probe occurs.
	for time.Now().Before(deadline) {

		s.genericJanitorCircuitBreaker(node, func(n *JanitorNode) {
			calls++
		})

		metrics := node.circuitBreaker.GetMetrics()

		// Circuit closes immediately after half-open probe
		if metrics.TotalCircuitCloses > 0 {
			transitioned = true
			break
		}

		time.Sleep(1 * time.Millisecond)
	}

	if !transitioned {
		t.Fatalf("expected open → half-open → closed transition, but never occurred")
	}

	// Final state must be closed
	if node.circuitBreaker.GetState() != circuit_breaker.StateClosed {
		t.Fatalf("expected final state closed, got %s", node.circuitBreaker.GetState())
	}

	if calls != 1 {
		t.Fatalf("expected exactly 1 half-open probe call, got %d", calls)
	}
}
