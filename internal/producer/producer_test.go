package producer

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

// mockBuffer implements buffer.Buffer interface for testing
type mockBuffer struct {
	addError error
	messages []*models.Message
	mu       sync.Mutex
}

func (m *mockBuffer) Start() {
	// No-op for tests
}

func (m *mockBuffer) Stop() error {
	return nil
}

func (m *mockBuffer) Add(msg *models.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.addError != nil {
		return m.addError
	}
	m.messages = append(m.messages, msg)
	return nil
}

func (m *mockBuffer) Flush() error {
	return nil
}

func (m *mockBuffer) Size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.messages)
}

func (m *mockBuffer) Health() *models.ComponentHealth {
	return &models.ComponentHealth{
		Status: models.HealthStatusHealthy,
	}
}

// mockStore implements db.Store interface for testing
type mockStore struct {
	deleteError error
	deleteCalls int
	mu          sync.Mutex
}

func (m *mockStore) BatchCreateMessages(ctx context.Context, msgs []*models.Message) error {
	return nil
}

func (m *mockStore) DeleteMessage(ctx context.Context, id uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalls++
	if m.deleteError != nil {
		return m.deleteError
	}
	return nil
}

func (m *mockStore) UpdateMessageStatus(ctx context.Context, id uuid.UUID, status models.MessageStatus) error {
	return nil
}

func (m *mockStore) MarkPendingMessagesAsReady(ctx context.Context, currentTime time.Time) (int64, error) {
	return 0, nil
}

func (m *mockStore) MarkStaleAcquiredMessagesAsReady(ctx context.Context, staleThreshold time.Duration) (int64, error) {
	return 0, nil
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
	return 0, nil
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
	return 0, nil
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
	cfg.MinScheduledAtFutureMs = 5000
	return cfg
}

func TestNewProducer(t *testing.T) {
	t.Run("creates producer with correct configuration", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())
		maxPayloadSizeKB := 10240

		producer := NewProducer(store, buf, log, maxPayloadSizeKB, createTestConfig())

		if producer == nil {
			t.Fatal("Expected producer to be created")
		}

		if producer.maxPayloadSizeKB != maxPayloadSizeKB {
			t.Errorf("Expected maxPayloadSizeKB %d, got %d", maxPayloadSizeKB, producer.maxPayloadSizeKB)
		}

		if producer.messagesCreated != 0 {
			t.Errorf("Expected messagesCreated 0, got %d", producer.messagesCreated)
		}
	})
}

func TestProducerStartStop(t *testing.T) {
	t.Run("starts and stops producer", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())

		err := producer.Start()
		if err != nil {
			t.Errorf("Expected no error starting producer, got: %v", err)
		}

		producer.mu.RLock()
		isRunning := producer.isRunning
		producer.mu.RUnlock()

		if !isRunning {
			t.Error("Expected producer to be running after start")
		}

		err = producer.Stop()
		if err != nil {
			t.Errorf("Expected no error stopping producer, got: %v", err)
		}

		producer.mu.RLock()
		isRunning = producer.isRunning
		producer.mu.RUnlock()

		if isRunning {
			t.Error("Expected producer to be stopped after stop")
		}
	})
}

func TestProducerHealth(t *testing.T) {
	t.Run("reports healthy status when running", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		producer.Start()

		health := producer.Health()

		if health.Status != models.HealthStatusHealthy {
			t.Errorf("Expected healthy status, got %s", health.Status)
		}

		if health.Name != "producer" {
			t.Errorf("Expected name 'producer', got %s", health.Name)
		}

		metadata := health.Metadata
		if metadata["is_running"] != true {
			t.Error("Expected is_running to be true")
		}
	})

	t.Run("reports unhealthy status when stopped", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		// Don't start producer

		health := producer.Health()

		if health.Status != models.HealthStatusUnhealthy {
			t.Errorf("Expected unhealthy status, got %s", health.Status)
		}
	})

	t.Run("includes metrics in health metadata", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		producer.Start()

		// Simulate some activity
		producer.mu.Lock()
		producer.messagesCreated = 100
		producer.messagesDeleted = 50
		producer.creationErrors = 5
		producer.deletionErrors = 2
		producer.mu.Unlock()

		health := producer.Health()
		metadata := health.Metadata

		if metadata["messages_created"] != int64(100) {
			t.Errorf("Expected messages_created 100, got %v", metadata["messages_created"])
		}

		if metadata["messages_deleted"] != int64(50) {
			t.Errorf("Expected messages_deleted 50, got %v", metadata["messages_deleted"])
		}

		if metadata["creation_errors"] != int64(5) {
			t.Errorf("Expected creation_errors 5, got %v", metadata["creation_errors"])
		}

		if metadata["deletion_errors"] != int64(2) {
			t.Errorf("Expected deletion_errors 2, got %v", metadata["deletion_errors"])
		}
	})
}

func TestCreateMessage(t *testing.T) {
	t.Run("successfully creates message", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		producer.Start()

		ctx := context.Background()
		scheduledAt := time.Now().Add(10 * time.Second)

		req := &models.CreateMessageRequest{
			Channel:     "test-channel",
			Payload:     []byte(`{"test": "data"}`),
			ScheduledAt: models.UnixTime{Time: scheduledAt},
		}

		messageID, err := producer.CreateMessage(ctx, req)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if messageID == uuid.Nil {
			t.Error("Expected valid message ID")
		}

		producer.mu.RLock()
		messagesCreated := producer.messagesCreated
		producer.mu.RUnlock()

		if messagesCreated != 1 {
			t.Errorf("Expected messagesCreated 1, got %d", messagesCreated)
		}

		buf.mu.Lock()
		bufferSize := len(buf.messages)
		buf.mu.Unlock()

		if bufferSize != 1 {
			t.Errorf("Expected 1 message in buffer, got %d", bufferSize)
		}
	})

	t.Run("fails when channel is empty", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())

		ctx := context.Background()
		req := &models.CreateMessageRequest{
			Channel:     "",
			Payload:     []byte(`{"test": "data"}`),
			ScheduledAt: models.UnixTime{Time: time.Now().Add(10 * time.Second)},
		}

		_, err := producer.CreateMessage(ctx, req)
		if err == nil {
			t.Error("Expected error for empty channel")
		}

		if err.Error() != "channel is required" {
			t.Errorf("Expected 'channel is required' error, got: %v", err)
		}

		producer.mu.RLock()
		creationErrors := producer.creationErrors
		producer.mu.RUnlock()

		if creationErrors != 1 {
			t.Errorf("Expected creationErrors 1, got %d", creationErrors)
		}
	})

	t.Run("fails when payload is empty", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())

		ctx := context.Background()
		req := &models.CreateMessageRequest{
			Channel:     "test-channel",
			Payload:     []byte{},
			ScheduledAt: models.UnixTime{Time: time.Now().Add(10 * time.Second)},
		}

		_, err := producer.CreateMessage(ctx, req)
		if err == nil {
			t.Error("Expected error for empty payload")
		}

		if err.Error() != "payload is required" {
			t.Errorf("Expected 'payload is required' error, got: %v", err)
		}
	})

	t.Run("fails when payload is not valid JSON", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())

		ctx := context.Background()
		req := &models.CreateMessageRequest{
			Channel:     "test-channel",
			Payload:     []byte(`invalid json`),
			ScheduledAt: models.UnixTime{Time: time.Now().Add(10 * time.Second)},
		}

		_, err := producer.CreateMessage(ctx, req)
		if err == nil {
			t.Error("Expected error for invalid JSON")
		}

		if err.Error() != "payload must be valid JSON" {
			t.Errorf("Expected 'payload must be valid JSON' error, got: %v", err)
		}
	})

	t.Run("fails when scheduled_at is not far enough in future", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())

		ctx := context.Background()
		req := &models.CreateMessageRequest{
			Channel:     "test-channel",
			Payload:     []byte(`{"test": "data"}`),
			ScheduledAt: models.UnixTime{Time: time.Now().Add(1 * time.Second)},
		}

		_, err := producer.CreateMessage(ctx, req)
		if err == nil {
			t.Error("Expected error for scheduled_at not far enough in future")
		}
	})

	t.Run("fails when payload exceeds max size", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 1, createTestConfig()) // 1 KB limit

		ctx := context.Background()
		// Create a payload larger than 1 KB
		largePayload := make([]byte, 2048)
		for i := range largePayload {
			largePayload[i] = 'a'
		}
		// Wrap in valid JSON
		largePayload = []byte(`{"data":"` + string(largePayload) + `"}`)

		req := &models.CreateMessageRequest{
			Channel:     "test-channel",
			Payload:     largePayload,
			ScheduledAt: models.UnixTime{Time: time.Now().Add(10 * time.Second)},
		}

		_, err := producer.CreateMessage(ctx, req)
		if err == nil {
			t.Error("Expected error for payload exceeding max size")
		}
	})

	t.Run("fails when buffer returns error", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{
			addError: errors.New("buffer full"),
		}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())

		ctx := context.Background()
		req := &models.CreateMessageRequest{
			Channel:     "test-channel",
			Payload:     []byte(`{"test": "data"}`),
			ScheduledAt: models.UnixTime{Time: time.Now().Add(10 * time.Second)},
		}

		_, err := producer.CreateMessage(ctx, req)
		if err == nil {
			t.Error("Expected error when buffer add fails")
		}

		producer.mu.RLock()
		creationErrors := producer.creationErrors
		producer.mu.RUnlock()

		if creationErrors != 1 {
			t.Errorf("Expected creationErrors 1, got %d", creationErrors)
		}
	})

	t.Run("sets status to READY for immediate messages", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())

		ctx := context.Background()
		req := &models.CreateMessageRequest{
			Channel:     "test-channel",
			Payload:     []byte(`{"test": "data"}`),
			ScheduledAt: models.UnixTime{Time: time.Now().Add(10 * time.Second)},
		}

		_, err := producer.CreateMessage(ctx, req)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		buf.mu.Lock()
		message := buf.messages[0]
		buf.mu.Unlock()

		if message.Status != models.StatusPending {
			t.Errorf("Expected status PENDING, got %s", message.Status)
		}
	})

	t.Run("clears last error on successful create", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())

		// Set a previous error
		producer.mu.Lock()
		producer.lastError = errors.New("previous error")
		producer.mu.Unlock()

		ctx := context.Background()
		req := &models.CreateMessageRequest{
			Channel:     "test-channel",
			Payload:     []byte(`{"test": "data"}`),
			ScheduledAt: models.UnixTime{Time: time.Now().Add(10 * time.Second)},
		}

		_, err := producer.CreateMessage(ctx, req)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		producer.mu.RLock()
		lastError := producer.lastError
		producer.mu.RUnlock()

		if lastError != nil {
			t.Errorf("Expected lastError to be cleared, got: %v", lastError)
		}
	})
}

func TestDeleteMessage(t *testing.T) {
	t.Run("successfully deletes message", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		producer.Start()

		ctx := context.Background()
		messageID := uuid.New()

		err := producer.DeleteMessage(ctx, messageID)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		producer.mu.RLock()
		messagesDeleted := producer.messagesDeleted
		producer.mu.RUnlock()

		if messagesDeleted != 1 {
			t.Errorf("Expected messagesDeleted 1, got %d", messagesDeleted)
		}

		store.mu.Lock()
		deleteCalls := store.deleteCalls
		store.mu.Unlock()

		if deleteCalls != 1 {
			t.Errorf("Expected 1 delete call to store, got %d", deleteCalls)
		}
	})

	t.Run("fails when store returns error", func(t *testing.T) {
		store := &mockStore{
			deleteError: errors.New("database error"),
		}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())

		ctx := context.Background()
		messageID := uuid.New()

		err := producer.DeleteMessage(ctx, messageID)
		if err == nil {
			t.Error("Expected error when store delete fails")
		}

		producer.mu.RLock()
		deletionErrors := producer.deletionErrors
		producer.mu.RUnlock()

		if deletionErrors != 1 {
			t.Errorf("Expected deletionErrors 1, got %d", deletionErrors)
		}
	})

	t.Run("clears last error on successful delete", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())

		// Set a previous error
		producer.mu.Lock()
		producer.lastError = errors.New("previous error")
		producer.mu.Unlock()

		ctx := context.Background()
		messageID := uuid.New()

		err := producer.DeleteMessage(ctx, messageID)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		producer.mu.RLock()
		lastError := producer.lastError
		producer.mu.RUnlock()

		if lastError != nil {
			t.Errorf("Expected lastError to be cleared, got: %v", lastError)
		}
	})

	t.Run("updates last active time", func(t *testing.T) {
		store := &mockStore{}
		buf := &mockBuffer{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())

		before := time.Now()

		ctx := context.Background()
		messageID := uuid.New()

		producer.DeleteMessage(ctx, messageID)

		producer.mu.RLock()
		lastActive := producer.lastActive
		producer.mu.RUnlock()

		if lastActive.Before(before) {
			t.Error("Expected lastActive to be updated")
		}
	})
}

func TestFormatError(t *testing.T) {
	t.Run("returns nil for nil error", func(t *testing.T) {
		result := formatError(nil)
		if result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	})

	t.Run("returns error string for non-nil error", func(t *testing.T) {
		err := errors.New("test error")
		result := formatError(err)

		if result != "test error" {
			t.Errorf("Expected 'test error', got %v", result)
		}
	})
}
