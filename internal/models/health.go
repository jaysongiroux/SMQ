package models

import "time"

type NodeHealth struct {
	NodeID        string    `json:"node_id"`
	NodeType      string    `json:"node_type"` // producer, consumer, scheduler
	Status        string    `json:"status"`    // healthy, degraded, unhealthy
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Region        *string   `json:"region,omitempty"`
}

type ClusterHealth struct {
	Status       string       `json:"status"` // healthy, degraded, unhealthy
	TotalNodes   int          `json:"total_nodes"`
	HealthyNodes int          `json:"healthy_nodes"`
	Nodes        []NodeHealth `json:"nodes"`
	Timestamp    time.Time    `json:"timestamp"`
}
