package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/db"
	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/models"
)

// Consumer handles message retrieval, acknowledgment, and channel management
type Consumer struct {
	store      db.Store
	log        *logger.Logger
	nodeID     string // Unique ID for this consumer node
	mu         sync.RWMutex
	isRunning  bool
	lastActive time.Time
}

// NewConsumer creates a new consumer instance
func NewConsumer(store db.Store, nodeID string, log *logger.Logger) *Consumer {
	return &Consumer{
		store:  store,
		nodeID: nodeID,
		log:    log,
	}
}

// Start initializes the consumer
func (c *Consumer) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.isRunning = true
	c.lastActive = time.Now()

	return nil
}

// Stop gracefully stops the consumer
func (c *Consumer) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.isRunning = false

	return nil
}

// Health returns the current health status of the consumer
func (c *Consumer) Health() *models.ComponentHealth {
	c.mu.RLock()
	defer c.mu.RUnlock()

	status := models.HealthStatusHealthy
	message := "Consumer is ready to accept requests"

	if !c.isRunning {
		status = models.HealthStatusUnhealthy
		message = "Consumer is down"
	}

	return &models.ComponentHealth{
		Name:      "consumer",
		Status:    status,
		Message:   message,
		CheckedAt: time.Now(),
		Metadata: map[string]interface{}{
			"is_running":  c.isRunning,
			"last_active": c.lastActive,
			"node_id":     c.nodeID,
		},
	}
}

// PollMessages retrieves ready messages for the given channel
// This atomically queries the database using SELECT FOR UPDATE SKIP LOCKED
// to ensure no two consumers can retrieve the same message
func (c *Consumer) PollMessages(ctx context.Context, channelID string, max int, subsidize bool) ([]*models.Message, error) {
	c.mu.Lock()
	c.lastActive = time.Now()
	c.mu.Unlock()

	c.log.Debug("Polling up to %d messages from channel %s (subsidize: %v)", max, channelID, subsidize)

	// Acquire messages up to max
	msgs, err := c.store.AcquireNextMessage(ctx, channelID, max)
	if err != nil {
		c.log.Error("Failed to acquire message from channel %s: %v", channelID, err)
		return nil, err
	}

	c.log.Info("Acquired %d messages from channel %s", len(msgs), channelID)
	return msgs, nil
}

// AckMessage permanently deletes messages after successful processing
func (c *Consumer) AckMessage(ctx context.Context, messageIDs []uuid.UUID) error {
	c.mu.Lock()
	c.lastActive = time.Now()
	c.mu.Unlock()

	if len(messageIDs) == 0 {
		c.log.Warn("AckMessage called with empty message ID list")
		return nil
	}

	c.log.Debug("Acknowledging %d messages", len(messageIDs))

	err := c.store.AckMessage(ctx, messageIDs)
	if err != nil {
		c.log.Error("Failed to ack %d messages: %v", len(messageIDs), err)
		return err
	}

	c.log.Info("Successfully acknowledged %d messages", len(messageIDs))
	return nil
}

// NackMessage marks messages for retry or moves to DLQ
func (c *Consumer) NackMessage(ctx context.Context, messageIDs []uuid.UUID) error {
	c.mu.Lock()
	c.lastActive = time.Now()
	c.mu.Unlock()

	if len(messageIDs) == 0 {
		c.log.Warn("NackMessage called with empty message ID list")
		return nil
	}

	c.log.Debug("Nacking %d messages", len(messageIDs))

	err := c.store.NackMessage(ctx, messageIDs)
	if err != nil {
		c.log.Error("Failed to nack %d messages: %v", len(messageIDs), err)
		return err
	}

	c.log.Info("Successfully nacked %d messages", len(messageIDs))
	return nil
}

// ListChannels retrieves a paginated list of channels from the database
func (c *Consumer) ListChannels(ctx context.Context, limit, offset int) ([]*models.Channel, error) {
	c.mu.Lock()
	c.lastActive = time.Now()
	c.mu.Unlock()

	c.log.Debug("Listing channels (limit: %d, offset: %d)", limit, offset)

	channels, err := c.store.ListChannels(ctx, limit, offset)
	if err != nil {
		c.log.Error("Failed to list channels: %v", err)
		return nil, err
	}

	c.log.Debug("Retrieved %d channels", len(channels))
	return channels, nil
}
