package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jaysongiroux/smq/internal/circuit_breaker"
	"github.com/jaysongiroux/smq/internal/config"
	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/models"
	"github.com/jaysongiroux/smq/internal/testutils"
)

func createTestConfig() *config.Config {
	cfg := &config.Config{}
	cfg.LogLevel = "info"
	return cfg
}

func createTestSchedulerConfig() *SchedulerConfig {
	return &SchedulerConfig{
		PollInterval:           100 * time.Millisecond,
		PollJitterPercent:      10,
		JanitorInterval:        200 * time.Millisecond,
		JanitorJitterPercent:   10,
		StaleAcquiredThreshold: 5 * time.Minute,
		StaleNodeThreshold:     10 * time.Minute,
	}
}

func newTestCB() *circuit_breaker.CircuitBreaker {
	log := logger.New("test", &config.Config{LogLevel: "fatal"})
	return circuit_breaker.NewCircuitBreaker(circuit_breaker.Config{
		Name:            "test-cb",
		MaxFailures:     2,
		Timeout:         50 * time.Millisecond,
		ResetTimeout:    50 * time.Millisecond,
		HalfOpenMaxReqs: 1,
		Log:             log,
	})
}

func TestNewScheduler(t *testing.T) {
	t.Run("creates scheduler with correct number of nodes", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 3, 2, log)

		if scheduler == nil {
			t.Fatal("Expected scheduler to be created")
		}

		if len(scheduler.schedulerNodes) != 3 {
			t.Errorf("Expected 3 scheduler nodes, got %d", len(scheduler.schedulerNodes))
		}

		if len(scheduler.janitorNodes) != 2 {
			t.Errorf("Expected 2 janitor nodes, got %d", len(scheduler.janitorNodes))
		}

		for i, node := range scheduler.schedulerNodes {
			if node.id != i {
				t.Errorf("Expected scheduler node %d to have id %d, got %d", i, i, node.id)
			}
		}

		for i, node := range scheduler.janitorNodes {
			if node.id != i {
				t.Errorf("Expected janitor node %d to have id %d, got %d", i, i, node.id)
			}
		}
	})

	t.Run("creates scheduler with zero nodes", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 0, log)

		if len(scheduler.schedulerNodes) != 0 {
			t.Errorf("Expected 0 scheduler nodes, got %d", len(scheduler.schedulerNodes))
		}

		if len(scheduler.janitorNodes) != 0 {
			t.Errorf("Expected 0 janitor nodes, got %d", len(scheduler.janitorNodes))
		}
	})
}

func TestSchedulerStartStop(t *testing.T) {
	t.Run("starts and stops scheduler successfully", func(t *testing.T) {
		store := &testutils.MockStore{MarkPendingCount: 5}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 2, 1, log)
		scheduler.Start()

		// Give some time for goroutines to start
		time.Sleep(50 * time.Millisecond)

		// Check that nodes are running
		for _, node := range scheduler.schedulerNodes {
			node.mu.RLock()
			if !node.isRunning {
				t.Error("Expected scheduler node to be running")
			}
			node.mu.RUnlock()
		}

		for _, node := range scheduler.janitorNodes {
			node.mu.RLock()
			if !node.isRunning {
				t.Error("Expected janitor node to be running")
			}
			node.mu.RUnlock()
		}

		// Stop scheduler
		err := scheduler.Stop()
		if err != nil {
			t.Errorf("Expected no error stopping scheduler, got: %v", err)
		}

		// Check that nodes are stopped
		for _, node := range scheduler.schedulerNodes {
			node.mu.RLock()
			if node.isRunning {
				t.Error("Expected scheduler node to be stopped")
			}
			node.mu.RUnlock()
		}

		for _, node := range scheduler.janitorNodes {
			node.mu.RLock()
			if node.isRunning {
				t.Error("Expected janitor node to be stopped")
			}
			node.mu.RUnlock()
		}
	})

	t.Run("scheduler nodes mark messages as ready", func(t *testing.T) {
		store := &testutils.MockStore{MarkPendingCount: 10}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 1, 0, log)
		scheduler.Start()

		// Wait for at least 2 poll cycles
		time.Sleep(250 * time.Millisecond)

		scheduler.Stop()

		// Check that MarkPendingMessagesAsReady was called
		store.Mu.Lock()
		calls := store.MarkPendingCalls
		store.Mu.Unlock()

		if calls < 2 {
			t.Errorf("Expected at least 2 calls to MarkPendingMessagesAsReady, got %d", calls)
		}

		// Check that node tracked messages
		node := scheduler.schedulerNodes[0]
		node.mu.RLock()
		messagesMarked := node.messagesMarked
		node.mu.RUnlock()

		expectedMessages := int64(calls) * 10
		if messagesMarked != expectedMessages {
			t.Errorf("Expected %d messages marked, got %d", expectedMessages, messagesMarked)
		}
	})
}

func TestSchedulerHealth(t *testing.T) {
	t.Run("reports healthy status when running normally", func(t *testing.T) {
		store := &testutils.MockStore{MarkPendingCount: 5}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 2, 1, log)
		scheduler.Start()

		// Wait for nodes to run
		time.Sleep(150 * time.Millisecond)

		health := scheduler.Health()

		// Check scheduler nodes
		for i := 0; i < 2; i++ {
			key := "scheduler-node-" + string(rune('0'+i))
			if h, ok := health[key]; ok {
				if h.Status != models.HealthStatusHealthy {
					t.Errorf("Expected scheduler node %d to be healthy, got status: %s", i, h.Status)
				}
			} else {
				t.Errorf("Expected health report for %s", key)
			}
		}

		// Check janitor node
		if h, ok := health["janitor-node-0"]; ok {
			if h.Status != models.HealthStatusHealthy {
				t.Errorf("Expected janitor node 0 to be healthy, got status: %s", h.Status)
			}
		} else {
			t.Error("Expected health report for janitor-node-0")
		}

		scheduler.Stop()
	})

	t.Run("reports unhealthy status when not running", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 1, 1, log)
		// Don't start scheduler

		health := scheduler.Health()

		if h, ok := health["scheduler-node-0"]; ok {
			if h.Status != models.HealthStatusUnhealthy {
				t.Errorf("Expected scheduler node to be unhealthy, got status: %s", h.Status)
			}
		}

		if h, ok := health["janitor-node-0"]; ok {
			if h.Status != models.HealthStatusUnhealthy {
				t.Errorf("Expected janitor node to be unhealthy, got status: %s", h.Status)
			}
		}
	})

	t.Run("reports degraded status when delayed", func(t *testing.T) {
		store := &testutils.MockStore{MarkPendingCount: 5}
		log := logger.New("test", createTestConfig())

		// Use a very long interval so nodes appear delayed
		config := &SchedulerConfig{
			PollInterval:           10 * time.Second,
			PollJitterPercent:      10,
			JanitorInterval:        10 * time.Second,
			JanitorJitterPercent:   10,
			StaleAcquiredThreshold: 5 * time.Minute,
			StaleNodeThreshold:     10 * time.Minute,
		}

		scheduler := NewScheduler(config, store, 1, 0, log)
		scheduler.Start()

		// Manually set lastRun to simulate delay
		time.Sleep(50 * time.Millisecond)
		scheduler.Stop()

		node := scheduler.schedulerNodes[0]
		node.mu.Lock()
		node.lastRun = time.Now().Add(-40 * time.Second) // Simulate old last run
		node.isRunning = true                            // Pretend it's still running
		node.mu.Unlock()

		health := scheduler.Health()

		if h, ok := health["scheduler-node-0"]; ok {
			if h.Status != models.HealthStatusDegraded {
				t.Errorf("Expected scheduler node to be degraded, got status: %s (message: %s)", h.Status, h.Message)
			}
		} else {
			t.Error("Expected health report for scheduler-node-0")
		}
	})
}

func TestMarkPendingMessagesReady(t *testing.T) {
	t.Run("successfully marks messages", func(t *testing.T) {
		store := &testutils.MockStore{MarkPendingCount: 15}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 1, 0, log)
		node := scheduler.schedulerNodes[0]

		scheduler.markPendingMessagesReady(context.Background(), node)

		node.mu.RLock()
		messagesMarked := node.messagesMarked
		node.mu.RUnlock()

		if messagesMarked != 15 {
			t.Errorf("Expected 15 messages marked, got %d", messagesMarked)
		}

		store.Mu.Lock()
		calls := store.MarkPendingCalls
		store.Mu.Unlock()

		if calls != 1 {
			t.Errorf("Expected 1 call to MarkPendingMessagesAsReady, got %d", calls)
		}
	})

	t.Run("handles errors gracefully", func(t *testing.T) {
		store := &testutils.MockStore{
			MarkPendingError: errors.New("database error"),
		}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 1, 0, log)
		node := scheduler.schedulerNodes[0]

		scheduler.markPendingMessagesReady(context.Background(), node)

		node.mu.RLock()
		messagesMarked := node.messagesMarked
		node.mu.RUnlock()

		// Should not increment count on error
		if messagesMarked != 0 {
			t.Errorf("Expected 0 messages marked on error, got %d", messagesMarked)
		}
	})
}

func TestJanitorOperations(t *testing.T) {
	t.Run("cleans up stale messages", func(t *testing.T) {
		store := &testutils.MockStore{MarkStaleCount: 8}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		scheduler.cleanupStaleMessages(node)

		node.mu.RLock()
		messagesCleanedUp := node.messagesCleanedUp
		node.mu.RUnlock()

		if messagesCleanedUp != 8 {
			t.Errorf("Expected 8 messages cleaned up, got %d", messagesCleanedUp)
		}

		store.Mu.Lock()
		calls := store.MarkStaleCalls
		store.Mu.Unlock()

		if calls != 1 {
			t.Errorf("Expected 1 call to MarkStaleAcquiredMessagesAsReady, got %d", calls)
		}
	})

	t.Run("cleans up stale nodes", func(t *testing.T) {
		store := &testutils.MockStore{DeleteStaleNodesCount: 3}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		scheduler.cleanupStaleNodes(node)

		node.mu.RLock()
		nodesCleanedUp := node.nodesCleanedUp
		node.mu.RUnlock()

		if nodesCleanedUp != 3 {
			t.Errorf("Expected 3 nodes cleaned up, got %d", nodesCleanedUp)
		}

		store.Mu.Lock()
		calls := store.DeleteStaleNodesCalls
		store.Mu.Unlock()

		if calls != 1 {
			t.Errorf("Expected 1 call to DeleteStaleNodes, got %d", calls)
		}
	})

	t.Run("cleans up failed messages", func(t *testing.T) {
		store := &testutils.MockStore{CleanFailedMessagesCount: 5}
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

	t.Run("handles cleanup errors gracefully", func(t *testing.T) {
		store := &testutils.MockStore{
			MarkStaleError:        errors.New("stale message error"),
			DeleteStaleNodesError: errors.New("stale node error"),
		}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		scheduler.cleanupStaleMessages(node)
		scheduler.cleanupStaleNodes(node)

		node.mu.RLock()
		lastError := node.lastError
		node.mu.RUnlock()

		if lastError == nil {
			t.Error("Expected error to be recorded")
		}
	})

	t.Run("janitor runs periodic cleanup", func(t *testing.T) {
		store := &testutils.MockStore{
			MarkStaleCount:        2,
			DeleteStaleNodesCount: 1,
		}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		scheduler.Start()

		// Wait for multiple cleanup cycles
		time.Sleep(500 * time.Millisecond)

		scheduler.Stop()

		store.Mu.Lock()
		staleCalls := store.MarkStaleCalls
		nodesCalls := store.DeleteStaleNodesCalls
		store.Mu.Unlock()

		if staleCalls < 2 {
			t.Errorf("Expected at least 2 calls to MarkStaleAcquiredMessagesAsReady, got %d", staleCalls)
		}

		if nodesCalls < 2 {
			t.Errorf("Expected at least 2 calls to DeleteStaleNodes, got %d", nodesCalls)
		}
	})
}

func TestJanitorHealth(t *testing.T) {
	t.Run("reports degraded status on error", func(t *testing.T) {
		// Set errors on both cleanup operations so error persists
		store := &testutils.MockStore{
			MarkStaleError:        errors.New("cleanup failed"),
			DeleteStaleNodesError: errors.New("node cleanup failed"),
		}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		scheduler.Start()

		// Wait for janitor to run multiple times and encounter errors
		time.Sleep(450 * time.Millisecond)

		// Check that the node has the error recorded
		node := scheduler.janitorNodes[0]
		node.mu.RLock()
		hasError := node.lastError != nil
		node.mu.RUnlock()

		if !hasError {
			t.Error("Expected error to be recorded in janitor node")
		}

		health := scheduler.Health()

		if h, ok := health["janitor-node-0"]; ok {
			if h.Status != models.HealthStatusDegraded {
				t.Errorf("Expected janitor node to be degraded after error, got status: %s (message: %s)", h.Status, h.Message)
			}
		} else {
			t.Error("Expected health report for janitor-node-0")
		}

		scheduler.Stop()
	})
}

func TestCircuitBreaker_ClosesAfterSuccess(t *testing.T) {
	cb := newTestCB()

	err := cb.Execute(context.Background(), func(ctx context.Context) error {
		return nil
	})

	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}

	if cb.GetState() != circuit_breaker.StateClosed {
		t.Fatalf("expected closed state")
	}
}

func TestCircuitBreaker_OpensAfterFailures(t *testing.T) {
	cb := newTestCB()

	// Cause 2 failures
	cb.Execute(context.Background(), func(ctx context.Context) error { return errors.New("fail") })
	cb.Execute(context.Background(), func(ctx context.Context) error { return errors.New("fail") })

	if cb.GetState() != circuit_breaker.StateOpen {
		t.Fatalf("expected open state after failures")
	}
}

func TestCircuitBreaker_BlocksWhenOpen(t *testing.T) {
	cb := newTestCB()

	// open it
	cb.Execute(context.Background(), func(ctx context.Context) error { return errors.New("fail") })
	cb.Execute(context.Background(), func(ctx context.Context) error { return errors.New("fail") })

	err := cb.Execute(context.Background(), func(ctx context.Context) error { return nil })
	if !errors.Is(err, circuit_breaker.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}
func TestCircuitBreaker_EntersHalfOpenAndClosesImmediately(t *testing.T) {
	cb := newTestCB()

	// Force failures
	cb.Execute(context.Background(), func(ctx context.Context) error { return errors.New("fail") })
	cb.Execute(context.Background(), func(ctx context.Context) error { return errors.New("fail") })

	if cb.GetState() != circuit_breaker.StateOpen {
		t.Fatalf("expected open")
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	transitionedToHalfOpen := false

	for time.Now().Before(deadline) {

		_ = cb.Execute(context.Background(), func(ctx context.Context) error { return nil })

		metrics := cb.GetMetrics()

		if metrics.TotalCircuitCloses > 0 {
			transitionedToHalfOpen = true
			break
		}

		time.Sleep(1 * time.Millisecond)
	}

	if !transitionedToHalfOpen {
		t.Fatalf("expected half-open→closed transition, but never occurred")
	}

	if cb.GetState() != circuit_breaker.StateClosed {
		t.Fatalf("expected final state to be closed, got %s", cb.GetState())
	}
}

func TestCircuitBreaker_ClosesAfterHalfOpenSuccess(t *testing.T) {
	cb := newTestCB()

	// Fail until breaker is open
	cb.Execute(context.Background(), func(ctx context.Context) error { return errors.New("fail") })
	cb.Execute(context.Background(), func(ctx context.Context) error { return errors.New("fail") })

	if cb.GetState() != circuit_breaker.StateOpen {
		t.Fatalf("expected open")
	}

	// Poll until breaker closes (HalfOpen → Closed)
	deadline := time.Now().Add(500 * time.Millisecond)
	transitioned := false

	for time.Now().Before(deadline) {
		_ = cb.Execute(context.Background(), func(ctx context.Context) error { return nil })

		metrics := cb.GetMetrics()

		if metrics.TotalCircuitCloses > 0 {
			transitioned = true
			break
		}

		time.Sleep(1 * time.Millisecond)
	}

	if !transitioned {
		t.Fatalf("expected transition half-open → closed based on success probe")
	}

	if cb.GetState() != circuit_breaker.StateClosed {
		t.Fatalf("expected final state closed, got %s", cb.GetState())
	}
}

func TestCircuitBreaker_TimeoutTriggersFailure(t *testing.T) {
	cb := newTestCB()

	err := cb.Execute(context.Background(), func(ctx context.Context) error {
		<-ctx.Done() // deterministic timeout
		return nil
	})

	if !errors.Is(err, circuit_breaker.ErrTimeout) {
		t.Fatalf("expected timeout error, got %v", err)
	}

	metrics := cb.GetMetrics()

	if metrics.TotalTimeouts != 1 {
		t.Fatalf("expected 1 timeout, got %d", metrics.TotalTimeouts)
	}

	if metrics.TotalFailures != 1 {
		t.Fatalf("expected 1 failure, got %d", metrics.TotalFailures)
	}

	if cb.GetState() != circuit_breaker.StateClosed {
		t.Fatalf("expected closed state after a single timeout, got %s", cb.GetState())
	}
}
