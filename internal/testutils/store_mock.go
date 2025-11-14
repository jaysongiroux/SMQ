package testutils

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/models"
)

// mockStore implements db.Store interface for testing
type MockStore struct {
	MarkPendingCount         int64
	MarkPendingError         error
	MarkStaleCount           int64
	MarkStaleError           error
	DeleteStaleNodesCount    int64
	DeleteStaleNodesError    error
	CleanFailedMessagesCount int64
	CleanFailedMessagesError error
	MarkPendingCalls         int
	MarkStaleCalls           int
	DeleteStaleNodesCalls    int
	AckMessageCalls          int
	NackMessageCalls         int
	DeleteCalls              int
	DeleteError              error
	CleanFailedMessagesCalls int
	Mu                       sync.Mutex
}

func (m *MockStore) BatchCreateMessages(ctx context.Context, msgs []*models.Message) error {
	return nil
}

func (m *MockStore) DeleteMessage(ctx context.Context, id uuid.UUID) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.DeleteCalls++
	if m.DeleteError != nil {
		return m.DeleteError
	}
	return nil
}

func (m *MockStore) UpdateMessageStatus(ctx context.Context, id uuid.UUID, status models.MessageStatus) error {
	return nil
}

func (m *MockStore) MarkPendingMessagesAsReady(ctx context.Context, currentTime time.Time) (int64, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.MarkPendingCalls++
	if m.MarkPendingError != nil {
		return 0, m.MarkPendingError
	}
	return m.MarkPendingCount, nil
}

func (m *MockStore) MarkStaleAcquiredMessagesAsReady(ctx context.Context, staleThreshold time.Duration) (int64, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.MarkStaleCalls++
	if m.MarkStaleError != nil {
		return 0, m.MarkStaleError
	}
	return m.MarkStaleCount, nil
}

func (m *MockStore) AcquireNextMessage(ctx context.Context, channel string, max int) ([]*models.Message, error) {
	return nil, nil
}

func (m *MockStore) AckMessage(ctx context.Context, ids []uuid.UUID) error {
	return nil
}

func (m *MockStore) NackMessage(ctx context.Context, ids []uuid.UUID) error {
	return nil
}

func (m *MockStore) CleanFailedMessages(ctx context.Context) (int64, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.CleanFailedMessagesCalls++
	if m.CleanFailedMessagesError != nil {
		return 0, m.CleanFailedMessagesError
	}
	return m.CleanFailedMessagesCount, nil
}

func (m *MockStore) ListChannels(ctx context.Context, limit int, offset int) ([]*models.Channel, error) {
	return nil, nil
}

func (m *MockStore) RegisterNode(ctx context.Context, node *models.Node) error {
	return nil
}

func (m *MockStore) UpdateNode(ctx context.Context, nodeID string, status string, metadata map[string]interface{}) error {
	return nil
}

func (m *MockStore) DeleteNode(ctx context.Context, nodeID string) error {
	return nil
}

func (m *MockStore) DeleteStaleNodes(ctx context.Context, staleThreshold time.Duration) (int64, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.DeleteStaleNodesCalls++
	if m.DeleteStaleNodesError != nil {
		return 0, m.DeleteStaleNodesError
	}
	return m.DeleteStaleNodesCount, nil
}

func (m *MockStore) ListNodes(ctx context.Context, limit int, offset int) ([]*models.Node, error) {
	return nil, nil
}

func (m *MockStore) Ping(ctx context.Context) error {
	return nil
}

func (m *MockStore) Close() error {
	return nil
}
