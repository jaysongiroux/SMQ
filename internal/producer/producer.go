package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/buffer"
	"github.com/jaysongiroux/smq/internal/config"
	"github.com/jaysongiroux/smq/internal/db"
	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/models"
)

type Producer struct {
	config           *config.Config
	store            db.Store
	buffer           buffer.Buffer
	log              *logger.Logger
	mu               sync.RWMutex
	isRunning        bool
	lastActive       time.Time
	messagesCreated  int64
	messagesDeleted  int64
	creationErrors   int64
	deletionErrors   int64
	lastError        error
	maxPayloadSizeKB int
}

func NewProducer(
	store db.Store,
	buf buffer.Buffer,
	log *logger.Logger,
	maxPayloadSizeKB int,
	config *config.Config,
) *Producer {
	return &Producer{
		store:            store,
		buffer:           buf,
		log:              log,
		maxPayloadSizeKB: maxPayloadSizeKB,
		config:           config,
	}
}

func (p *Producer) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.isRunning = true
	p.lastActive = time.Now()

	p.log.Info("Producer started (max payload size: %d KB)", p.maxPayloadSizeKB)
	return nil
}

func (p *Producer) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.isRunning = false

	p.log.Info(
		"Producer stopped (created: %d, deleted: %d, creation errors: %d, deletion errors: %d)",
		p.messagesCreated,
		p.messagesDeleted,
		p.creationErrors,
		p.deletionErrors,
	)
	return nil
}

func (p *Producer) Health() *models.ComponentHealth {
	p.mu.RLock()
	defer p.mu.RUnlock()

	status := models.HealthStatusHealthy
	message := "Producer is ready to accept requests"

	if !p.isRunning {
		status = models.HealthStatusUnhealthy
		message = "Producer is down"
	}

	return &models.ComponentHealth{
		Name:      "producer",
		Status:    status,
		Message:   message,
		CheckedAt: time.Now(),
		Metadata: map[string]interface{}{
			"is_running":       p.isRunning,
			"last_active":      p.lastActive,
			"messages_created": p.messagesCreated,
			"messages_deleted": p.messagesDeleted,
			"creation_errors":  p.creationErrors,
			"deletion_errors":  p.deletionErrors,
			"last_error":       formatError(p.lastError),
		},
	}
}

func formatError(err error) interface{} {
	if err == nil {
		return nil
	}
	return err.Error()
}

func (p *Producer) CreateMessage(
	ctx context.Context,
	req *models.CreateMessageRequest,
) (uuid.UUID, error) {
	p.mu.Lock()
	p.lastActive = time.Now()
	p.mu.Unlock()

	if req.Channel == "" {
		p.log.Warn("Message creation failed: channel is required")
		p.mu.Lock()
		p.creationErrors++
		p.lastError = fmt.Errorf("channel is required")
		p.mu.Unlock()
		return uuid.Nil, fmt.Errorf("channel is required")
	}

	if len(req.Payload) == 0 {
		p.log.Warn("Message creation failed: payload is required")
		p.mu.Lock()
		p.creationErrors++
		p.lastError = fmt.Errorf("payload is required")
		p.mu.Unlock()
		return uuid.Nil, fmt.Errorf("payload is required")
	}

	if !json.Valid(req.Payload) {
		p.log.Warn("Message creation failed: payload is not valid JSON")
		p.mu.Lock()
		p.creationErrors++
		p.lastError = fmt.Errorf("payload must be valid JSON")
		p.mu.Unlock()
		return uuid.Nil, fmt.Errorf("payload must be valid JSON")
	}

	scheduledAt := req.ScheduledAt.Time
	futureInterval := p.config.MinScheduledAtFutureMs
	if scheduledAt.Before(time.Now().Add(time.Duration(futureInterval) * time.Millisecond)) {
		p.log.Warn(
			"Message creation failed: scheduled at is not in the future by at least %d ms",
			futureInterval,
		)
		p.mu.Lock()
		p.creationErrors++
		p.lastError = fmt.Errorf(
			"scheduled at is not in the future by at least %d ms",
			futureInterval,
		)
		p.mu.Unlock()
		return uuid.Nil, fmt.Errorf(
			"scheduled at is not in the future by at least %d ms",
			futureInterval,
		)
	}

	payloadSizeBytes := len(req.Payload)
	payloadSizeKB := payloadSizeBytes / 1024
	if payloadSizeKB > p.maxPayloadSizeKB {
		p.log.Warn("Message creation failed: payload too large (%d KB > %d KB max)",
			payloadSizeKB, p.maxPayloadSizeKB)
		p.mu.Lock()
		p.creationErrors++
		p.lastError = fmt.Errorf(
			"payload size %d KB exceeds maximum %d KB",
			payloadSizeKB,
			p.maxPayloadSizeKB,
		)
		p.mu.Unlock()
		return uuid.Nil, fmt.Errorf(
			"payload size %d KB exceeds maximum %d KB",
			payloadSizeKB,
			p.maxPayloadSizeKB,
		)
	}

	messageID := uuid.Must(uuid.NewV7())

	now := time.Now()
	if scheduledAt.IsZero() {
		scheduledAt = now
	}

	status := models.StatusReady
	if scheduledAt.After(now) {
		status = models.StatusPending
	}

	message := &models.Message{
		ID:          messageID,
		Channel:     req.Channel,
		Payload:     req.Payload,
		ScheduledAt: scheduledAt,
		Status:      status,
		RetryCount:  0,
		CreatedAt:   now,
	}

	p.log.Debug("Adding message %s to buffer (channel: %s, scheduled_at: %s, status: %s)",
		messageID, req.Channel, scheduledAt.Format(time.RFC3339), status)

	if err := p.buffer.Add(message); err != nil {
		p.log.Error("Failed to add message %s to buffer: %v", messageID, err)
		p.mu.Lock()
		p.creationErrors++
		p.lastError = err
		p.mu.Unlock()
		return uuid.Nil, fmt.Errorf("failed to add message to buffer: %w", err)
	}

	p.mu.Lock()
	p.messagesCreated++
	p.lastError = nil
	p.mu.Unlock()

	p.log.Info("Message %s created successfully (channel: %s, payload: %d bytes)",
		messageID, req.Channel, len(req.Payload))

	return messageID, nil
}

// DeleteMessage deletes a message by ID from the database
func (p *Producer) DeleteMessage(ctx context.Context, messageID uuid.UUID) error {
	p.mu.Lock()
	p.lastActive = time.Now()
	p.mu.Unlock()

	p.log.Debug("Deleting message %s", messageID)

	// Try to remove from buffer first
	removed, _ := p.buffer.Remove(messageID)

	if removed {
		// Successfully removed from buffer
		p.mu.Lock()
		p.messagesDeleted++
		p.lastError = nil
		p.mu.Unlock()

		p.log.Info("Message %s deleted successfully from buffer", messageID)
		return nil
	}

	// Not in buffer (or buffer error), try database
	p.log.Debug("Message %s not in buffer, attempting database deletion", messageID)

	if err := p.store.DeleteMessage(ctx, messageID); err != nil {
		p.log.Error("Failed to delete message %s from database: %v", messageID, err)

		p.mu.Lock()
		p.deletionErrors++
		p.lastError = err
		p.mu.Unlock()

		return fmt.Errorf("failed to delete message: %w", err)
	}

	// Update metrics
	p.mu.Lock()
	p.messagesDeleted++
	p.lastError = nil
	p.mu.Unlock()

	p.log.Info("Message %s deleted successfully from database", messageID)

	return nil
}
