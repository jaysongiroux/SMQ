package testutils

import (
	"sync"

	"github.com/jaysongiroux/smq/internal/models"
)

// mockBuffer for handler tests
type MockBufferHandler struct {
	AddError error
	Messages []*models.Message
	Mu       sync.Mutex
}

func (m *MockBufferHandler) Start() {}

func (m *MockBufferHandler) Stop() error {
	return nil
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
