package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/db"
	"github.com/jaysongiroux/smq/internal/db/pg"
	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/models"
	"github.com/jmoiron/sqlx"
)

// PostgresStore implements the Store interface for PostgreSQL
type PostgresStore struct {
	config *db.PGConfig
	db     *sqlx.DB
	log    *logger.Logger
}

// NewPostgresStore creates a new PostgreSQL store instance
func NewPostgresStore(config *db.PGConfig, log *logger.Logger) (*PostgresStore, error) {
	// Build connection string
	// Open database connection
	database, err := sqlx.Connect("postgres", config.ConnectionString)
	if err != nil {
		log.Error("Failed to connect to PostgreSQL database: %v", err)
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Configure connection pool
	database.SetMaxOpenConns(config.MaxOpenConns)
	database.SetMaxIdleConns(config.MaxIdleConns)
	database.SetConnMaxLifetime(time.Hour)

	// Verify connection
	if err := database.Ping(); err != nil {
		log.Error("Failed to ping PostgreSQL database: %v", err)
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	store := &PostgresStore{
		config: config,
		db:     database,
		log:    log,
	}

	log.Info("Connected to PostgreSQL database")

	// Run migrations
	if err := store.migrate(); err != nil {
		log.Error("Failed to run database migrations: %v", err)
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	log.Info("Database migrations completed successfully")

	return store, nil
}

// migrate runs database migrations
func (s *PostgresStore) migrate() error {
	return pg.Migrate(s.log, s.db, Schema)
}

// BatchCreateMessages inserts multiple messages in a single transaction
func (s *PostgresStore) BatchCreateMessages(ctx context.Context, msgs []*models.Message) error {
	return pg.BatchCreateMessages(ctx, msgs, s.log, s.db)
}

// DeleteMessage removes a message from the database
func (s *PostgresStore) DeleteMessage(ctx context.Context, id uuid.UUID) error {
	return pg.DeleteMessage(ctx, id, s.log, s.db)
}

// UpdateMessageStatus updates the status of a message
func (s *PostgresStore) UpdateMessageStatus(ctx context.Context, id uuid.UUID, status models.MessageStatus) error {
	return pg.UpdateMessageStatus(ctx, id, status, s.log, s.db)
}

// MarkPendingMessagesAsReady updates pending messages that are ready
func (s *PostgresStore) MarkPendingMessagesAsReady(ctx context.Context, currentTime time.Time) (int64, error) {
	// Use a sub-select to avoid locking the whole table
	// This is more efficient for concurrent schedulers
	query := `
		UPDATE messages m
		SET status = $1
		FROM (
			SELECT id
			FROM messages
			WHERE status = $2
			  AND scheduled_at <= $3
			ORDER BY scheduled_at ASC
			LIMIT $4
		) AS selected
		WHERE m.id = selected.id
	`

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
// This is the janitor function that handles messages from dead consumers or messages that have not been delivered
func (s *PostgresStore) MarkStaleAcquiredMessagesAsReady(ctx context.Context, staleThreshold time.Duration) (int64, error) {
	// Calculate the stale time threshold
	staleTime := time.Now().Add(-staleThreshold)

	query := `
		UPDATE messages 
		SET status = $1, 
		    retry_count = retry_count + 1,
		    acquired_at = NULL
		WHERE status = $2
		  AND acquired_at < $3
	`

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

// AcquireNextMessage atomically acquires the next ready message
// Uses a Writable CTE with RETURNING for single-query atomicity and efficiency.
func (s *PostgresStore) AcquireNextMessage(ctx context.Context, channel string, max int) ([]*models.Message, error) {
	// Start a transaction for atomicity
	// The transaction is still required for FOR UPDATE to hold locks.
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		s.log.Error("Failed to begin transaction for AcquireNextMessage: %v", err)
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Rollback if not committed

	// This single query finds, locks, updates, and returns the messages.
	// 1. CTE `locked_messages`: Finds and locks the rows using FOR UPDATE SKIP LOCKED.
	// 2. UPDATE: Updates the rows found in the CTE.
	// 3. RETURNING: Returns all columns of the rows *after* the update.
	const atomicAcquireQuery = `
		WITH locked_messages AS (
			SELECT id
			FROM messages
			WHERE channel = $1
			  AND status = 'READY'
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
			messages.created_at, messages.region;
	`

	rows, err := tx.QueryContext(ctx, atomicAcquireQuery, channel, max)
	if err != nil {
		s.log.Error("Failed to atomically acquire messages for channel %s: %v", channel, err)
		return nil, err
	}
	defer rows.Close()

	var messages []*models.Message

	for rows.Next() {
		msg := &models.Message{}
		err := rows.Scan(
			&msg.ID,
			&msg.Channel,
			&msg.Payload,
			&msg.ScheduledAt,
			&msg.Status,     // This will be 'ACQUIRED' from RETURNING
			&msg.AcquiredAt, // This will be the new timestamp from RETURNING
			&msg.RetryCount,
			&msg.CreatedAt,
			&msg.Region,
		)
		if err != nil {
			// tx.Rollback() will be called by defer
			return nil, fmt.Errorf("failed to scan acquired message: %w", err)
		}
		messages = append(messages, msg)
	}

	// Check for any errors encountered during iteration
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	// If no messages were found, len(messages) will be 0.
	// We just need to commit the (empty) transaction and return.

	// Commit the transaction
	// This releases the locks and makes our changes permanent.
	if err := tx.Commit(); err != nil {
		s.log.Error("Failed to commit transaction for AcquireNextMessage: %v", err)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// We successfully acquired a batch of messages!
	// No second loop is needed, as the structs already have the updated status.
	return messages, nil
}

// AckMessage permanently deletes a message after successful processing
func (s *PostgresStore) AckMessage(ctx context.Context, ids []uuid.UUID) error {
	return pg.AckMessage(ctx, ids, s.log, s.db)
}

// NackMessage marks a message for retry or marks it as failed
func (s *PostgresStore) NackMessage(ctx context.Context, ids []uuid.UUID) error {
	return pg.NackMessage(ctx, ids, s.log, s.db, s.config.MaxRetries)
}

// ListChannels retrieves all channels with pagination
// Returns distinct channels that have messages in the database
func (s *PostgresStore) ListChannels(ctx context.Context, limit int, offset int) ([]*models.Channel, error) {
	return pg.ListChannels(ctx, limit, offset, s.log, s.db, false)
}

// RegisterNode registers a new node in the cluster
func (s *PostgresStore) RegisterNode(ctx context.Context, node *models.Node) error {
	return pg.RegisterNode(ctx, node, s.log, s.db)
}

// UpdateNode updates a node's status and metadata
func (s *PostgresStore) UpdateNode(ctx context.Context, nodeID string, status string, metadata map[string]interface{}) error {
	return pg.UpdateNode(ctx, nodeID, status, metadata, s.log, s.db)
}

// DeleteNode removes a node from the cluster
func (s *PostgresStore) DeleteNode(ctx context.Context, nodeID string) error {
	return pg.DeleteNode(ctx, nodeID, s.log, s.db)
}

// DeleteStaleNodes removes nodes that haven't been seen within the staleThreshold duration
// Returns the number of nodes deleted
func (s *PostgresStore) DeleteStaleNodes(ctx context.Context, staleThreshold time.Duration) (int64, error) {
	return pg.DeleteStaleNodes(ctx, staleThreshold, s.log, s.db)
}

// ListNodes retrieves all nodes in the cluster with pagination
func (s *PostgresStore) ListNodes(ctx context.Context, limit int, offset int) ([]*models.Node, error) {
	return pg.ListNodes(ctx, limit, offset, s.log, s.db)
}

func (s *PostgresStore) CleanFailedMessages(ctx context.Context) (int64, error) {
	var query string
	var operation string

	if s.config.JanitorDeleteFailedMessages {
		query = `DELETE FROM messages WHERE status = 'FAILED'`
		operation = "clean failed messages"
	} else {
		query = `UPDATE messages SET channel = channel || '.DLQ' WHERE status = 'FAILED'`
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
func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close closes the database connection
func (s *PostgresStore) Close() error {
	return s.db.Close()
}
