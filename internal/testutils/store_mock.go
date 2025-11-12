package testutils

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/models"
)

// mockStore for handler tests
type MockStoreHandler struct {
	DeleteError error
	DeleteCalls int
	Mu          sync.Mutex
}

func (m *MockStoreHandler) BatchCreateMessages(ctx context.Context, msgs []*models.Message) error {
	return nil
}

func (m *MockStoreHandler) DeleteMessage(ctx context.Context, id uuid.UUID) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.DeleteCalls++
	if m.DeleteError != nil {
		return m.DeleteError
	}
	return nil
}

func (m *MockStoreHandler) UpdateMessageStatus(ctx context.Context, id uuid.UUID, status models.MessageStatus) error {
	return nil
}

func (m *MockStoreHandler) MarkPendingMessagesAsReady(ctx context.Context, currentTime time.Time) (int64, error) {
	return 0, nil
}

func (m *MockStoreHandler) MarkStaleAcquiredMessagesAsReady(ctx context.Context, staleThreshold time.Duration) (int64, error) {
	return 0, nil
}

func (m *MockStoreHandler) AcquireNextMessage(ctx context.Context, channel string, max int) ([]*models.Message, error) {
	return nil, nil
}

func (m *MockStoreHandler) AckMessage(ctx context.Context, ids []uuid.UUID) error {
	return nil
}

func (m *MockStoreHandler) NackMessage(ctx context.Context, ids []uuid.UUID) error {
	return nil
}

func (m *MockStoreHandler) CleanFailedMessages(ctx context.Context) (int64, error) {
	return 0, nil
}

func (m *MockStoreHandler) ListChannels(ctx context.Context, limit int, offset int) ([]*models.Channel, error) {
	return nil, nil
}

func (m *MockStoreHandler) RegisterNode(ctx context.Context, node *models.Node) error {
	return nil
}

func (m *MockStoreHandler) UpdateNode(ctx context.Context, nodeID string, status string, metadata map[string]interface{}) error {
	return nil
}

func (m *MockStoreHandler) DeleteNode(ctx context.Context, nodeID string) error {
	return nil
}

func (m *MockStoreHandler) DeleteStaleNodes(ctx context.Context, staleThreshold time.Duration) (int64, error) {
	return 0, nil
}

func (m *MockStoreHandler) ListNodes(ctx context.Context, limit int, offset int) ([]*models.Node, error) {
	return nil, nil
}

func (m *MockStoreHandler) Ping(ctx context.Context) error {
	return nil
}

func (m *MockStoreHandler) Close() error {
	return nil
}
