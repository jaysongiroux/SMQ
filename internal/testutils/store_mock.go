package testutils

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/models"
)

// mockStore implements db.Store interface for testing
type MockStore struct {
	BatchCreateMessagesCalled int
	MarkPendingCount          int64
	MarkPendingError          error
	MarkStaleCount            int64
	MarkStaleError            error
	DeleteStaleNodesCount     int64
	DeleteStaleNodesError     error
	CleanFailedMessagesCount  int64
	BatchCreateError          error
	CleanFailedMessagesError  error
	MarkPendingCalls          int
	AcquireNextMessageCalls   int
	AcquireNextMessageError   error
	AckMessageError           error
	NackMessageError          error
	ListChannelsCalls         int
	ListChannelsError         error
	ListChannelsResult        []*models.Channel
	AcquireNextMessageResult  []*models.Message
	RegisterNodeResult        *models.Node
	RegisterNodeError         error
	RegisterNodeCalls         int
	LastRegisteredNode        *models.Node
	ListNodesResult           []*models.Node
	ListNodesCalls            int
	ListNodesError            error
	MarkStaleCalls            int
	DeleteStaleNodesCalls     int
	AckMessageCalls           int
	NackMessageCalls          int
	DeleteCalls               int
	DeleteError               error
	CleanFailedMessagesCalls  int
	Mu                        sync.Mutex
	DeleteNotFound            bool
}

func (m *MockStore) BatchCreateMessages(ctx context.Context, msgs []*models.Message) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.BatchCreateMessagesCalled++
	if m.BatchCreateError != nil {
		return m.BatchCreateError
	}

	return nil
}

func (m *MockStore) DeleteMessage(ctx context.Context, messageID uuid.UUID) error {
	m.Mu.Lock()
	m.DeleteCalls++
	m.Mu.Unlock()

	if m.DeleteNotFound {
		return errors.New("message not found")
	}

	if m.DeleteError != nil {
		return m.DeleteError
	}

	return nil
}

func (m *MockStore) UpdateMessageStatus(
	ctx context.Context,
	id uuid.UUID,
	status models.MessageStatus,
) error {
	return nil
}

func (m *MockStore) MarkPendingMessagesAsReady(
	ctx context.Context,
	currentTime time.Time,
) (int64, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.MarkPendingCalls++
	if m.MarkPendingError != nil {
		return 0, m.MarkPendingError
	}
	return m.MarkPendingCount, nil
}

func (m *MockStore) MarkStaleAcquiredMessagesAsReady(
	ctx context.Context,
	staleThreshold time.Duration,
) (int64, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.MarkStaleCalls++
	if m.MarkStaleError != nil {
		return 0, m.MarkStaleError
	}
	return m.MarkStaleCount, nil
}

func copyMessages(messages []*models.Message) []*models.Message {
	copied := make([]*models.Message, len(messages))
	for i, message := range messages {
		copied[i] = &models.Message{
			ID: message.ID,
		}
	}
	return copied
}

func (m *MockStore) AcquireNextMessage(
	ctx context.Context,
	channel string,
	max int,
) ([]*models.Message, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.AcquireNextMessageCalls++
	if m.AcquireNextMessageError != nil {
		return nil, m.AcquireNextMessageError
	}

	if len(m.AcquireNextMessageResult) > 0 {
		return copyMessages(m.AcquireNextMessageResult), nil
	}
	return []*models.Message{}, nil
}

func (m *MockStore) AckMessage(ctx context.Context, ids []uuid.UUID) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.AckMessageCalls++
	if m.AckMessageError != nil {
		return m.AckMessageError
	}
	return nil
}

func (m *MockStore) NackMessage(ctx context.Context, ids []uuid.UUID) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.NackMessageCalls++
	if m.NackMessageError != nil {
		return m.NackMessageError
	}
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

func copyChannels(channels []*models.Channel) []*models.Channel {
	copied := make([]*models.Channel, len(channels))
	for i, channel := range channels {
		copied[i] = &models.Channel{
			Name: channel.Name,
		}
	}
	return copied
}

func (m *MockStore) ListChannels(
	ctx context.Context,
	limit int,
	offset int,
) ([]*models.Channel, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.ListChannelsCalls++
	if m.ListChannelsError != nil {
		return nil, m.ListChannelsError
	}
	if len(m.ListChannelsResult) > 0 {
		return copyChannels(m.ListChannelsResult), nil
	}
	return []*models.Channel{}, nil
}

func (m *MockStore) RegisterNode(ctx context.Context, node *models.Node) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.RegisterNodeCalls++
	m.LastRegisteredNode = node
	if m.RegisterNodeError != nil {
		return m.RegisterNodeError
	}
	return nil
}

func (m *MockStore) UpdateNode(
	ctx context.Context,
	nodeID string,
	status string,
	metadata map[string]interface{},
) error {
	return nil
}

func (m *MockStore) DeleteNode(ctx context.Context, nodeID string) error {
	return nil
}

func (m *MockStore) DeleteStaleNodes(
	ctx context.Context,
	staleThreshold time.Duration,
) (int64, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.DeleteStaleNodesCalls++
	if m.DeleteStaleNodesError != nil {
		return 0, m.DeleteStaleNodesError
	}
	return m.DeleteStaleNodesCount, nil
}

func (m *MockStore) ListNodes(ctx context.Context, limit, offset int) ([]*models.Node, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.ListNodesCalls++
	if m.ListNodesError != nil {
		return nil, m.ListNodesError
	}
	return m.ListNodesResult, nil
}

func (m *MockStore) Ping(ctx context.Context) error {
	return nil
}

func (m *MockStore) Close() error {
	return nil
}
