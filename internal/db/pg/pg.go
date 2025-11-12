package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/models"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

func DeleteStaleNodes(ctx context.Context, staleThreshold time.Duration, log *logger.Logger, db *sqlx.DB) (int64, error) {
	// Calculate the cutoff time
	cutoffTime := time.Now().Add(-staleThreshold)

	query := `DELETE FROM nodes WHERE last_seen < $1`

	result, err := db.ExecContext(ctx, query, cutoffTime)
	if err != nil {
		log.Error("Failed to delete stale nodes: %v", err)
		return 0, fmt.Errorf("failed to delete stale nodes: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}

func ListNodes(ctx context.Context, limit int, offset int, log *logger.Logger, db *sqlx.DB) ([]*models.Node, error) {
	// Apply sensible defaults and bounds
	if limit <= 0 {
		limit = 100 // Default page size
	}
	if limit > 1000 {
		limit = 1000 // Max page size to prevent abuse
	}
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT node_id, status, last_seen, registered_at, metadata
		FROM nodes
		ORDER BY registered_at DESC
		LIMIT $1 OFFSET $2
	`

	rows, err := db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		log.Error("Failed to list nodes: %v", err)
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			log.Error("Failed to close rows: %v", err)
		}
	}()

	var nodes []*models.Node
	for rows.Next() {
		var node models.Node
		var metadataJSON []byte

		if err := rows.Scan(
			&node.NodeID,
			&node.Status,
			&node.LastSeen,
			&node.RegisteredAt,
			&metadataJSON,
		); err != nil {
			return nil, fmt.Errorf("failed to scan node: %w", err)
		}

		// Unmarshal JSONB metadata
		if metadataJSON != nil {
			if err := json.Unmarshal(metadataJSON, &node.Metadata); err != nil {
				return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
			}
		}

		nodes = append(nodes, &node)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating nodes: %w", err)
	}

	return nodes, nil
}

func DeleteNode(ctx context.Context, nodeID string, log *logger.Logger, db *sqlx.DB) error {
	query := `DELETE FROM nodes WHERE node_id = $1`

	result, err := db.ExecContext(ctx, query, nodeID)
	if err != nil {
		log.Error("Failed to delete node %s: %v", nodeID, err)
		return fmt.Errorf("failed to delete node: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("node not found: %s", nodeID)
	}

	return nil
}

func UpdateNode(ctx context.Context, nodeID string, status string, metadata map[string]interface{}, log *logger.Logger, db *sqlx.DB) error {
	// Marshal metadata to JSONB
	var metadataJSON []byte
	var err error
	if metadata != nil {
		metadataJSON, err = json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}
	}

	query := `
		UPDATE nodes
		SET status = $1,
		    last_seen = $2,
		    metadata = $3
		WHERE node_id = $4
	`

	result, err := db.ExecContext(ctx, query, status, time.Now(), metadataJSON, nodeID)
	if err != nil {
		log.Error("Failed to update node %s: %v", nodeID, err)
		return fmt.Errorf("failed to update node: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("node not found: %s", nodeID)
	}

	return nil
}

func RegisterNode(ctx context.Context, node *models.Node, log *logger.Logger, db *sqlx.DB) error {
	// Marshal metadata to JSONB
	var metadataJSON []byte
	var err error
	if node.Metadata != nil {
		metadataJSON, err = json.Marshal(node.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}
	}

	query := `
		INSERT INTO nodes (node_id, status, last_seen, registered_at, metadata)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (node_id) DO UPDATE
		SET status = EXCLUDED.status,
		    last_seen = EXCLUDED.last_seen,
		    metadata = EXCLUDED.metadata
	`

	_, err = db.ExecContext(
		ctx,
		query,
		node.NodeID,
		node.Status,
		node.LastSeen,
		node.RegisteredAt,
		metadataJSON,
	)
	if err != nil {
		log.Error("Failed to register node %s: %v", node.NodeID, err)
		return fmt.Errorf("failed to register node: %w", err)
	}

	return nil
}

func ListChannels(ctx context.Context, limit int, offset int, log *logger.Logger, db *sqlx.DB, includeFollowerRead bool) ([]*models.Channel, error) {
	// Apply sensible defaults and bounds
	if limit <= 0 {
		limit = 100 // Default page size
	}
	if limit > 1000 {
		limit = 1000 // Max page size to prevent abuse
	}
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT DISTINCT channel
		FROM messages
		ORDER BY channel
		LIMIT $2 OFFSET $3
	`

	if includeFollowerRead {
		query = `
			SELECT DISTINCT channel
			FROM messages
			AS OF SYSTEM TIME follower_read_timestamp()
			ORDER BY channel
			LIMIT $1 OFFSET $2
		`
	}

	rows, err := db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		log.Error("Failed to list channels: %v", err)
		return nil, fmt.Errorf("failed to list channels: %w", err)
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			log.Error("Failed to close rows: %v", err)
		}
	}()

	var channels []*models.Channel
	for rows.Next() {
		var channelName string
		if err := rows.Scan(&channelName); err != nil {
			return nil, fmt.Errorf("failed to scan channel: %w", err)
		}

		channels = append(channels, &models.Channel{
			Name: channelName,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating channels: %w", err)
	}

	return channels, nil
}

func NackMessage(ctx context.Context, ids []uuid.UUID, log *logger.Logger, db *sqlx.DB, maxRetries int) error {
	if len(ids) == 0 {
		return nil // Nothing to do
	}

	// Check current retry count and determine next action
	query := `
		UPDATE messages
		SET status = CASE 
			WHEN retry_count + 1 >= $1 THEN 'FAILED'
			ELSE 'READY'
		END,
		retry_count = retry_count + 1,
		acquired_at = NULL
		WHERE id = ANY($2)
	`

	result, err := db.ExecContext(ctx, query, maxRetries, pq.Array(ids))
	if err != nil {
		log.Error("Failed to nack messages %v: %v", ids, err)
		return fmt.Errorf("failed to nack messages: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no messages found: %v", ids)
	}

	return nil
}

// AckMessage permanently deletes a message
func AckMessage(ctx context.Context, ids []uuid.UUID, log *logger.Logger, db *sqlx.DB) error {
	if len(ids) == 0 {
		return nil // Nothing to do
	}

	query := `DELETE FROM messages WHERE id = ANY($1)`

	result, err := db.ExecContext(ctx, query, pq.Array(ids))
	if err != nil {
		log.Error("Failed to ack messages %v: %v", ids, err)
		return fmt.Errorf("failed to ack messages: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no messages found: %v", ids)
	}

	return nil
}

func Migrate(log *logger.Logger, db *sqlx.DB, schema string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, schema)
	if err != nil {
		log.Error("Failed to run database migrations: %v", err)
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	log.Info("Database migrations completed successfully")
	return nil
}

func BatchCreateMessages(ctx context.Context, msgs []*models.Message, log *logger.Logger, db *sqlx.DB) error {
	if len(msgs) == 0 {
		return nil
	}

	// Start transaction
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		log.Error("Failed to begin transaction for batch insert: %v", err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		err = tx.Rollback()
		if err != nil {
			log.Error("Failed to rollback transaction: %v", err)
		}
	}()

	query := `
		INSERT INTO messages (id, channel, payload, scheduled_at, status, retry_count, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	stmt, err := tx.PreparexContext(ctx, query)
	if err != nil {
		log.Error("Failed to prepare statement for batch insert: %v", err)
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer func() {
		err = stmt.Close()
		if err != nil {
			log.Error("Failed to close statement: %v", err)
		}
	}()

	// Insert each message using the prepared statement
	for _, msg := range msgs {
		_, err := stmt.ExecContext(
			ctx,
			msg.ID,
			msg.Channel,
			msg.Payload,
			msg.ScheduledAt,
			msg.Status,
			msg.RetryCount,
			msg.CreatedAt,
		)
		if err != nil {
			log.Error("Failed to insert message %s in batch: %v", msg.ID, err)
			return fmt.Errorf("failed to insert message: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		log.Error("Failed to commit transaction for batch insert: %v", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func DeleteMessage(ctx context.Context, id uuid.UUID, log *logger.Logger, db *sqlx.DB) error {
	query := `DELETE FROM messages WHERE id = $1`

	result, err := db.ExecContext(ctx, query, id)
	if err != nil {
		log.Error("Failed to delete message %s: %v", id, err)
		return fmt.Errorf("failed to delete message: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("message not found: %s", id)
	}

	return nil
}

func UpdateMessageStatus(ctx context.Context, id uuid.UUID, status models.MessageStatus, log *logger.Logger, db *sqlx.DB) error {
	query := `UPDATE messages SET status = $1 WHERE id = $2`

	result, err := db.ExecContext(ctx, query, status, id)
	if err != nil {
		log.Error("Failed to update message %s status to %s: %v", id, status, err)
		return fmt.Errorf("failed to update message status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Error("Failed to get rows affected: %v", err)
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("message not found: %s", id)
	}

	return nil
}
