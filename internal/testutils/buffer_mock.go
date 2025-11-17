package testutils

import (
	"sync"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/models"
)

// mockBuffer for handler tests
type MockBufferHandler struct {
	AddError    error
	RemoveError error
	Messages    []*models.Message
	Mu          sync.Mutex
	RemoveFound bool
	AddCalls    int
	RemoveCalls int
}

func (m *MockBufferHandler) Start() {}

func (m *MockBufferHandler) Stop() error {
	return nil
}

func (m *MockBufferHandler) Remove(messageID uuid.UUID) (bool, error) {
	m.Mu.Lock()
	m.RemoveCalls++
	m.Mu.Unlock()

	if m.RemoveError != nil {
		return m.RemoveFound, m.RemoveError
	}

	return m.RemoveFound, nil
}

func (m *MockBufferHandler) Add(msg *models.Message) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	if m.AddError != nil {
		return m.AddError
	}
	m.Messages = append(m.Messages, msg)
	return nil
}

func (m *MockBufferHandler) Flush() error {
	return nil
}

func (m *MockBufferHandler) Size() int {
	return len(m.Messages)
}

func (m *MockBufferHandler) Health() *models.ComponentHealth {
	return &models.ComponentHealth{
		Status: models.HealthStatusHealthy,
	}
}
