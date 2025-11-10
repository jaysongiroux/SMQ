package db

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/models"
)

// Store is an abstraction layer for database operations
// This interface allows communication with different database types:
// - Small deployments: Single-node Postgres
// - Large deployments: High-availability Postgres (Aurora, read replicas)
// - Global scale: Multi-region databases (CockroachDB)
type Store interface {
	// Message operations
	BatchCreateMessages(ctx context.Context, msgs []*models.Message) error
	DeleteMessage(ctx context.Context, id uuid.UUID) error
	UpdateMessageStatus(ctx context.Context, id uuid.UUID, status models.MessageStatus) error

	// Scheduler operations
	// MarkPendingMessagesAsReady atomically updates messages that are ready to be consumed
	// UPDATE messages SET status = 'ready' WHERE status = 'pending' AND scheduled_at <= NOW()
	MarkPendingMessagesAsReady(ctx context.Context, currentTime time.Time) (int64, error)

	// MarkStaleAcquiredMessagesAsReady handles stale acquired messages (janitor function)
	// For messages that remain 'acquired' too long (consumer died without ack/nack)
	MarkStaleAcquiredMessagesAsReady(ctx context.Context, staleThreshold time.Duration) (int64, error)

	// Consumer operations
	// AcquireNextMessage atomically finds and locks a list of ready message using SELECT FOR UPDATE SKIP LOCKED
	// It marks the message as 'acquired' and acquired_at
	AcquireNextMessage(ctx context.Context, channel string, max int) ([]*models.Message, error)

	// AckMessage permanently deletes a message after successful processing
	AckMessage(ctx context.Context, ids []uuid.UUID) error

	// NackMessage marks a message as ready again and increments retry count
	// If retry count exceeds threshold, marks as failed and moves to dead letter queue
	NackMessage(ctx context.Context, ids []uuid.UUID) error

	// Clean failed messages
	CleanFailedMessages(ctx context.Context) (int64, error)

	// Channel operations
	ListChannels(ctx context.Context, limit int, offset int) ([]*models.Channel, error)

	// Node operations
	RegisterNode(ctx context.Context, node *models.Node) error
	UpdateNode(ctx context.Context, nodeID string, status string, metadata map[string]interface{}) error
	DeleteNode(ctx context.Context, nodeID string) error
	DeleteStaleNodes(ctx context.Context, staleThreshold time.Duration) (int64, error)
	ListNodes(ctx context.Context, limit int, offset int) ([]*models.Node, error)

	// Health check
	Ping(ctx context.Context) error
	Close() error
}

type PGConfig struct {
	ConnectionString            string
	MaxRetries                  int
	JanitorDeleteFailedMessages bool
	MaxOpenConns                int
	MaxIdleConns                int
	Region                      *string
	MultiRegionScheduler        bool
	MultiRegionJanitor          bool
	MultiRegionSupplement       bool
	MaxMessagesPerPoll          int
}

// cassandra config
type CQLConfig struct {
	MaxRetries                  int
	JanitorDeleteFailedMessages bool
	MaxOpenConns                int
	MaxIdleConns                int
	Region                      *string // For multi-region deployments
	MultiRegionScheduler        bool
	MultiRegionJanitor          bool
	MultiRegionSupplement       bool
	MaxMessagesPerPoll          int
	Hosts                       []string
	Keyspace                    string
	Username                    string
	Password                    string
	ConsistencyLevel            string
	ConnectTimeout              int
	QueryTimeout                int
	LocalDatacenter             string
	HostSelectionPolicy         string
	KeepAliveIntervalMs         int
	NumConns                    int
	RetryPolicy                 string
}
