package models

import "time"

// HealthStatus represents the health state of a component
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusDegraded  HealthStatus = "degraded"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

// ComponentHealth represents the health of a single component
type ComponentHealth struct {
	Name      string                 `json:"name"`
	Status    HealthStatus           `json:"status"`
	Message   string                 `json:"message,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	CheckedAt time.Time              `json:"checked_at"`
}

// LayerHealth represents the health of a layer (may have multiple nodes)
type LayerHealth struct {
	Name   string                      `json:"name"`
	Status HealthStatus                `json:"status"`
	Nodes  map[string]*ComponentHealth `json:"nodes"` // node_id -> health
}

// SystemHealth represents the overall system health
type SystemHealth struct {
	Status    HealthStatus            `json:"status"`
	Region    string                  `json:"region"`
	Layers    map[string]*LayerHealth `json:"layers"`
	UpdatedAt time.Time               `json:"updated_at"`
}

// HealthReporter interface for components that can report health
type HealthReporter interface {
	Health() *ComponentHealth
}
