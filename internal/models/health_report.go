package models

import "time"

type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusDegraded  HealthStatus = "degraded"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

type ComponentHealth struct {
	Name      string                 `json:"name"`
	Status    HealthStatus           `json:"status"`
	Message   string                 `json:"message,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	CheckedAt time.Time              `json:"checked_at"`
}

type LayerHealth struct {
	Name   string                      `json:"name"`
	Status HealthStatus                `json:"status"`
	Nodes  map[string]*ComponentHealth `json:"nodes"` // node_id -> health
}

type SystemHealth struct {
	Status    HealthStatus            `json:"status"`
	Region    string                  `json:"region"`
	Layers    map[string]*LayerHealth `json:"layers"`
	UpdatedAt time.Time               `json:"updated_at"`
}

type HealthReporter interface {
	Health() *ComponentHealth
}
