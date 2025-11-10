package models

import "time"

// Node represents a node in the SMQ cluster
type Node struct {
	NodeID       string                 `json:"node_id" db:"node_id"`
	Status       string                 `json:"status" db:"status"`
	LastSeen     time.Time              `json:"last_seen" db:"last_seen"`
	RegisteredAt time.Time              `json:"registered_at" db:"registered_at"`
	Metadata     map[string]interface{} `json:"metadata,omitempty" db:"metadata"`
}
