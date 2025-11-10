package cockroach

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jaysongiroux/smq/internal/db"
	"github.com/jaysongiroux/smq/internal/db/pg"
	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/models"
	"github.com/jmoiron/sqlx"
)

// CockroachStore implements the Store interface for CockroachDB
// CockroachDB is PostgreSQL-compatible and can use similar queries
// with additional support for multi-region deployments
type CockroachStore struct {
	config *db.PGConfig
	db     *sqlx.DB
	log    *logger.Logger
}

// NewCockroachStore creates a new CockroachDB store instance
func NewCockroachStore(config *db.PGConfig, log *logger.Logger) (*CockroachStore, error) {
	// Open database connection
	database, err := sqlx.Connect("pgx", config.ConnectionString)
	if err != nil {
		log.Error("Failed to connect to CockroachDB database: %v", err)
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// configure connection pool
	database.SetMaxOpenConns(config.MaxOpenConns)
	database.SetMaxIdleConns(config.MaxIdleConns)
	database.SetConnMaxLifetime(time.Hour)

	// verify connection
	if err := database.Ping(); err != nil {
		log.Error("Failed to ping CockroachDB database: %v", err)
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	store := &CockroachStore{
		config: config,
		db:     database,
		log:    log,
	}

	log.Info("Connected to CockroachDB database")

	// Run migrations
	if err := store.migrate(); err != nil {
		if log != nil {
			log.Error("Failed to run database migrations: %v", err)
		}
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	log.Info("Database migrations completed successfully")

	return store, nil
}

func (s *CockroachStore) migrate() error {
	if err := pg.Migrate(s.log, s.db, Schema); err != nil {
		return err
	}

	InformUserAboutPartitions(s.log)

	return nil
}

// BatchCreateMessages inserts multiple messages in a single transaction
func (s *CockroachStore) BatchCreateMessages(ctx context.Context, msgs []*models.Message) error {
	return pg.BatchCreateMessages(ctx, msgs, s.log, s.db)
}

// DeleteMessage removes a message from the database
func (s *CockroachStore) DeleteMessage(ctx context.Context, id uuid.UUID) error {
	return pg.DeleteMessage(ctx, id, s.log, s.db)
}

// UpdateMessageStatus updates the status of a message
func (s *CockroachStore) UpdateMessageStatus(ctx context.Context, id uuid.UUID, status models.MessageStatus) error {
	return pg.UpdateMessageStatus(ctx, id, status, s.log, s.db)
}

// MarkPendingMessagesAsReady updates pending messages that are ready
// This is now conditionally REGION-SPECIFIC. It only updates messages in its own region.
func (s *CockroachStore) MarkPendingMessagesAsReady(ctx context.Context, currentTime time.Time) (int64, error) {
	baseQuery := `
		UPDATE messages m
		SET status = $1
		FROM (
			SELECT id
			FROM messages
			WHERE status = $2
			  AND scheduled_at <= $3
			  %s -- regionFilter will be injected here
			ORDER BY scheduled_at ASC
			LIMIT $4
		) AS selected
		WHERE m.id = selected.id
	`

	var regionFilter string
	if !s.config.MultiRegionScheduler {
		regionFilter = "AND crdb_region = gateway_region()::crdb_internal_region"
	}

	query := fmt.Sprintf(baseQuery, regionFilter)

	result, err := s.db.ExecContext(ctx, query, models.StatusReady, models.StatusPending, currentTime, s.config.MaxMessagesPerPoll)
	if err != nil {
		s.log.Error("Failed to mark pending messages as ready: %v", err)
		return 0, fmt.Errorf("failed to mark pending messages as ready: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}

// MarkStaleAcquiredMessagesAsReady marks stale acquired messages as ready
// This is now conditionally REGION-SPECIFIC. It only cleans up stale messages from its own region.
func (s *CockroachStore) MarkStaleAcquiredMessagesAsReady(ctx context.Context, staleThreshold time.Duration) (int64, error) {
	staleTime := time.Now().Add(-staleThreshold)

	baseQuery := `
		UPDATE messages 
		SET status = $1, 
		    retry_count = retry_count + 1,
		    acquired_at = NULL
		WHERE status = $2
		  AND acquired_at < $3
		  %s -- regionFilter will be injected here
	`

	var regionFilter string
	if !s.config.MultiRegionJanitor {
		regionFilter = "AND crdb_region = gateway_region()::crdb_internal_region"
	}

	query := fmt.Sprintf(baseQuery, regionFilter)

	result, err := s.db.ExecContext(ctx, query, models.StatusReady, models.StatusAcquired, staleTime)
	if err != nil {
		s.log.Error("Failed to mark stale acquired messages as ready: %v", err)
		return 0, fmt.Errorf("failed to mark stale acquired messages as ready: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}

// acquireMessages is a helper function to run the atomic acquire query.
// It can be targeted at the local region or non-local regions.
func (s *CockroachStore) acquireMessages(ctx context.Context, channel string, limit int, localOnly bool) ([]*models.Message, error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// This is the base atomic query.
	atomicAcquireQuery := `
		WITH locked_messages AS (
			SELECT id
			FROM messages
			WHERE channel = $1
			  AND status = 'READY'
			  -- region filter will be injected here
			  %s
			ORDER BY scheduled_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE messages
		SET status = 'ACQUIRED',
			acquired_at = NOW()
		FROM locked_messages
		WHERE messages.id = locked_messages.id
		RETURNING 
			messages.id, messages.channel, messages.payload, messages.scheduled_at, 
			messages.status, messages.acquired_at, messages.retry_count, 
			messages.created_at, messages.crdb_region as region;
	`

	var regionQuery string
	if localOnly {
		// Query for the local region only
		regionQuery = "AND crdb_region = gateway_region()::crdb_internal_region"
	} else {
		// Query for all *other* regions (supplemental query)
		regionQuery = "AND crdb_region != gateway_region()::crdb_internal_region"
	}

	// Inject the region logic into the query
	finalQuery := fmt.Sprintf(atomicAcquireQuery, regionQuery)

	rows, err := tx.QueryContext(ctx, finalQuery, channel, limit)
	if err != nil {
		s.log.Error("Failed to atomically acquire messages (localOnly=%t) for channel %s: %v. Query: %s", localOnly, channel, err, finalQuery)
		return nil, err
	}
	defer rows.Close()

	var messages []*models.Message
	for rows.Next() {
		msg := &models.Message{}
		var region sql.NullString // Use sql.NullString for crdb_region
		err := rows.Scan(
			&msg.ID,
			&msg.Channel,
			&msg.Payload,
			&msg.ScheduledAt,
			&msg.Status,
			&msg.AcquiredAt,
			&msg.RetryCount,
			&msg.CreatedAt,
			&region,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan acquired message: %w", err)
		}
		if region.Valid {
			msg.Region = &region.String // Assign if valid
		}
		messages = append(messages, msg)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return messages, nil
}

// AcquireNextMessage atomically acquires the next ready message.
// conditionally multi-region aware and respects the MultiRegionSupplement config.
func (s *CockroachStore) AcquireNextMessage(ctx context.Context, channel string, max int) ([]*models.Message, error) {
	// Step 1: Attempt to acquire messages from the local region first.
	localMessages, err := s.acquireMessages(ctx, channel, max, true)
	if err != nil {
		s.log.Error("Failed to acquire local messages for channel %s: %v", channel, err)
		return nil, err // Return error on local-first query
	}

	// Step 2: Check if we are done.
	// We are done if:
	// - Supplementing is disabled OR
	// - We filled the batch on the first try
	if !s.config.MultiRegionSupplement || len(localMessages) >= max {
		return localMessages, nil
	}

	// Step 3: We need to supplement. Calculate the remaining limit.
	remainingLimit := max - len(localMessages)

	// Step 4: Attempt to acquire messages from *other* regions.
	supplementalMessages, err := s.acquireMessages(ctx, channel, remainingLimit, false)
	if err != nil {
		// Log this as a warning, but return the local messages we did get.
		// Failing to supplement shouldn't fail the whole request.
		s.log.Warn("Failed to acquire supplemental messages for channel %s: %v", channel, err)
		return localMessages, nil
	}

	// Step 5: Combine and return the batches.
	return append(localMessages, supplementalMessages...), nil
}

// AckMessage permanently deletes a message
func (s *CockroachStore) AckMessage(ctx context.Context, ids []uuid.UUID) error {
	return pg.AckMessage(ctx, ids, s.log, s.db)
}

// NackMessage marks a message for retry or failure
func (s *CockroachStore) NackMessage(ctx context.Context, ids []uuid.UUID) error {
	return pg.NackMessage(ctx, ids, s.log, s.db, s.config.MaxRetries)
}

// ListChannels retrieves all channels with pagination
// Uses a fast, slightly-stale follower read for performance.
func (s *CockroachStore) ListChannels(ctx context.Context, limit int, offset int) ([]*models.Channel, error) {
	return pg.ListChannels(ctx, limit, offset, s.log, s.db, true)
}

// RegisterNode registers a new node in the cluster
// This function is region-agnostic because the 'nodes' table is LOCALITY GLOBAL.
func (s *CockroachStore) RegisterNode(ctx context.Context, node *models.Node) error {
	return pg.RegisterNode(ctx, node, s.log, s.db)
}

// UpdateNode updates a node's status and metadata
func (s *CockroachStore) UpdateNode(ctx context.Context, nodeID string, status string, metadata map[string]interface{}) error {
	return pg.UpdateNode(ctx, nodeID, status, metadata, s.log, s.db)
}

// DeleteNode removes a node from the cluster
func (s *CockroachStore) DeleteNode(ctx context.Context, nodeID string) error {
	return pg.DeleteNode(ctx, nodeID, s.log, s.db)
}

// DeleteStaleNodes removes nodes that haven't been seen within the staleThreshold duration
func (s *CockroachStore) DeleteStaleNodes(ctx context.Context, staleThreshold time.Duration) (int64, error) {
	return pg.DeleteStaleNodes(ctx, staleThreshold, s.log, s.db)
}

// ListNodes retrieves all nodes in the cluster with pagination
func (s *CockroachStore) ListNodes(ctx context.Context, limit int, offset int) ([]*models.Node, error) {
	return pg.ListNodes(ctx, limit, offset, s.log, s.db)
}

// CleanFailedMessages handles failed messages, either by deleting them or moving them to a DLQ.
// This is now REGION-SPECIFIC. It only cleans up failed messages from its own region.
func (s *CockroachStore) CleanFailedMessages(ctx context.Context) (int64, error) {
	var query string
	var operation string
	var regionFilter string

	if !s.config.MultiRegionJanitor {
		// Default: local-only janitor
		regionFilter = "AND crdb_region = gateway_region()::crdb_internal_region"
	}

	baseFilter := "WHERE status = 'FAILED'"
	finalFilter := fmt.Sprintf("%s %s", baseFilter, regionFilter)

	if s.config.JanitorDeleteFailedMessages {
		query = fmt.Sprintf(`DELETE FROM messages %s`, finalFilter)
		operation = "clean failed messages"
	} else {
		// Appends '.DLQ' to the channel name for failed messages in the local region
		query = fmt.Sprintf(`UPDATE messages SET channel = channel || '.DLQ', status = 'PENDING' %s`, finalFilter)
		operation = "add failed messages to dlq"
	}

	result, err := s.db.ExecContext(ctx, query)
	if err != nil {
		s.log.Error("Failed to %s: %v", operation, err)
		return 0, fmt.Errorf("failed to %s: %w", operation, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}

// Ping checks the database connection
func (s *CockroachStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close closes the database connection
func (s *CockroachStore) Close() error {
	return s.db.Close()
}
