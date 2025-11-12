package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/config"
	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/models"
)

// mockStore implements db.Store interface for testing
type mockStore struct {
	markPendingCount         int64
	markPendingError         error
	markStaleCount           int64
	markStaleError           error
	deleteStaleNodesCount    int64
	deleteStaleNodesError    error
	cleanFailedMessagesCount int64
	cleanFailedMessagesError error
	markPendingCalls         int
	markStaleCalls           int
	deleteStaleNodesCalls    int
	cleanFailedMessagesCalls int
	mu                       sync.Mutex
}

func (m *mockStore) BatchCreateMessages(ctx context.Context, msgs []*models.Message) error {
	return nil
}

func (m *mockStore) DeleteMessage(ctx context.Context, id uuid.UUID) error {
	return nil
}

func (m *mockStore) UpdateMessageStatus(ctx context.Context, id uuid.UUID, status models.MessageStatus) error {
	return nil
}

func (m *mockStore) MarkPendingMessagesAsReady(ctx context.Context, currentTime time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.markPendingCalls++
	if m.markPendingError != nil {
		return 0, m.markPendingError
	}
	return m.markPendingCount, nil
}

func (m *mockStore) MarkStaleAcquiredMessagesAsReady(ctx context.Context, staleThreshold time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.markStaleCalls++
	if m.markStaleError != nil {
		return 0, m.markStaleError
	}
	return m.markStaleCount, nil
}

func (m *mockStore) AcquireNextMessage(ctx context.Context, channel string, max int) ([]*models.Message, error) {
	return nil, nil
}

func (m *mockStore) AckMessage(ctx context.Context, ids []uuid.UUID) error {
	return nil
}

func (m *mockStore) NackMessage(ctx context.Context, ids []uuid.UUID) error {
	return nil
}

func (m *mockStore) CleanFailedMessages(ctx context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanFailedMessagesCalls++
	if m.cleanFailedMessagesError != nil {
		return 0, m.cleanFailedMessagesError
	}
	return m.cleanFailedMessagesCount, nil
}

func (m *mockStore) ListChannels(ctx context.Context, limit int, offset int) ([]*models.Channel, error) {
	return nil, nil
}

func (m *mockStore) RegisterNode(ctx context.Context, node *models.Node) error {
	return nil
}

func (m *mockStore) UpdateNode(ctx context.Context, nodeID string, status string, metadata map[string]interface{}) error {
	return nil
}

func (m *mockStore) DeleteNode(ctx context.Context, nodeID string) error {
	return nil
}

func (m *mockStore) DeleteStaleNodes(ctx context.Context, staleThreshold time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteStaleNodesCalls++
	if m.deleteStaleNodesError != nil {
		return 0, m.deleteStaleNodesError
	}
	return m.deleteStaleNodesCount, nil
}

func (m *mockStore) ListNodes(ctx context.Context, limit int, offset int) ([]*models.Node, error) {
	return nil, nil
}

func (m *mockStore) Ping(ctx context.Context) error {
	return nil
}

func (m *mockStore) Close() error {
	return nil
}

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

func TestNewScheduler(t *testing.T) {
	t.Run("creates scheduler with correct number of nodes", func(t *testing.T) {
		store := &mockStore{}
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
		store := &mockStore{}
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
		store := &mockStore{markPendingCount: 5}
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
		store := &mockStore{markPendingCount: 10}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 1, 0, log)
		scheduler.Start()

		// Wait for at least 2 poll cycles
		time.Sleep(250 * time.Millisecond)

		scheduler.Stop()

		// Check that MarkPendingMessagesAsReady was called
		store.mu.Lock()
		calls := store.markPendingCalls
		store.mu.Unlock()

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
		store := &mockStore{markPendingCount: 5}
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
		store := &mockStore{}
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
		store := &mockStore{markPendingCount: 5}
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
		store := &mockStore{markPendingCount: 15}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 1, 0, log)
		node := scheduler.schedulerNodes[0]

		scheduler.markPendingMessagesReady(node)

		node.mu.RLock()
		messagesMarked := node.messagesMarked
		node.mu.RUnlock()

		if messagesMarked != 15 {
			t.Errorf("Expected 15 messages marked, got %d", messagesMarked)
		}

		store.mu.Lock()
		calls := store.markPendingCalls
		store.mu.Unlock()

		if calls != 1 {
			t.Errorf("Expected 1 call to MarkPendingMessagesAsReady, got %d", calls)
		}
	})

	t.Run("handles errors gracefully", func(t *testing.T) {
		store := &mockStore{
			markPendingError: errors.New("database error"),
		}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 1, 0, log)
		node := scheduler.schedulerNodes[0]

		scheduler.markPendingMessagesReady(node)

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
		store := &mockStore{markStaleCount: 8}
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

		store.mu.Lock()
		calls := store.markStaleCalls
		store.mu.Unlock()

		if calls != 1 {
			t.Errorf("Expected 1 call to MarkStaleAcquiredMessagesAsReady, got %d", calls)
		}
	})

	t.Run("cleans up stale nodes", func(t *testing.T) {
		store := &mockStore{deleteStaleNodesCount: 3}
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

		store.mu.Lock()
		calls := store.deleteStaleNodesCalls
		store.mu.Unlock()

		if calls != 1 {
			t.Errorf("Expected 1 call to DeleteStaleNodes, got %d", calls)
		}
	})

	t.Run("cleans up failed messages", func(t *testing.T) {
		store := &mockStore{cleanFailedMessagesCount: 5}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		node := scheduler.janitorNodes[0]

		scheduler.cleanupFailedMessages(node)

		store.mu.Lock()
		calls := store.cleanFailedMessagesCalls
		store.mu.Unlock()

		if calls != 1 {
			t.Errorf("Expected 1 call to CleanFailedMessages, got %d", calls)
		}
	})

	t.Run("handles cleanup errors gracefully", func(t *testing.T) {
		store := &mockStore{
			markStaleError:        errors.New("stale message error"),
			deleteStaleNodesError: errors.New("stale node error"),
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
		store := &mockStore{
			markStaleCount:        2,
			deleteStaleNodesCount: 1,
		}
		log := logger.New("test", createTestConfig())
		config := createTestSchedulerConfig()

		scheduler := NewScheduler(config, store, 0, 1, log)
		scheduler.Start()

		// Wait for multiple cleanup cycles
		time.Sleep(500 * time.Millisecond)

		scheduler.Stop()

		store.mu.Lock()
		staleCalls := store.markStaleCalls
		nodesCalls := store.deleteStaleNodesCalls
		store.mu.Unlock()

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
		store := &mockStore{
			markStaleError:        errors.New("cleanup failed"),
			deleteStaleNodesError: errors.New("node cleanup failed"),
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
