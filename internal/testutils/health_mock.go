package testutils

import (
	"sync"

	"github.com/jaysongiroux/smq/internal/models"
)

type MockHealthReporter struct {
	Mu           sync.RWMutex
	SystemHealth *models.ComponentHealth
}

func (m *MockHealthReporter) Health() *models.ComponentHealth {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	if m.SystemHealth == nil {
		return nil
	}
	healthCopy := *m.SystemHealth
	return &healthCopy
}

func (m *MockHealthReporter) SetHealth(health *models.ComponentHealth) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.SystemHealth = health
}

func (m *MockHealthReporter) SetStatus(status models.HealthStatus) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	if m.SystemHealth != nil {
		m.SystemHealth.Status = status
	}
}
