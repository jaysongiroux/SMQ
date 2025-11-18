package buffer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/db"
	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/models"
)

// MemoryBuffer is an in-memory buffer that batches messages before writing to the database
// This improves write performance by reducing the number of database round trips
type MemoryBuffer struct {
	config         *Config
	store          db.Store
	messages       chan *models.Message
	flushChan      chan struct{}
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.Mutex
	batch          []*models.Message
	lastFlush      time.Time
	lastFlushError error
	isRunning      bool
	log            *logger.Logger

	// Adaptive tuning fields
	adaptiveEnabled       bool
	adaptiveMaxSize       int          // Current adaptive max size (if adaptive enabled)
	adaptiveFlushTicker   *time.Ticker // Ticker that can be reset for adaptive intervals
	adaptiveTuneThreshold int          // Number of flushes to tune adaptive flushing
	adaptiveMinSize       int          // Adaptive min size (if adaptive enabled)

	// Metrics
	totalFlushed     int64
	totalFlushErrors int64
	messagesDropped  int64
	avgFlushDuration time.Duration
	flushCount       int64 // Track number of flushes for adaptive tuning
}

// NewMemoryBuffer creates a new in-memory buffer instance
func NewMemoryBuffer(config *Config, store db.Store, log *logger.Logger) *MemoryBuffer {
	ctx, cancel := context.WithCancel(context.Background())

	b := &MemoryBuffer{
		config:                config,
		store:                 store,
		log:                   log,
		messages:              make(chan *models.Message, config.MaxSize*2),
		flushChan:             make(chan struct{}, 1),
		ctx:                   ctx,
		cancel:                cancel,
		batch:                 make([]*models.Message, 0, config.MaxSize),
		lastFlush:             time.Now(),
		adaptiveEnabled:       config.Adaptive,
		adaptiveMaxSize:       config.AdaptiveMaxSize,
		adaptiveTuneThreshold: config.AdaptiveTuneThreshold,
		adaptiveMinSize:       config.AdaptiveMinSize,
	}

	return b
}

func (b *MemoryBuffer) Remove(id uuid.UUID) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// remove message from batch
	for i, msg := range b.batch {
		if msg.ID == id {
			b.batch = append(b.batch[:i], b.batch[i+1:]...)
			b.log.Info("Message %s removed from memory buffer", id)
			return true, nil
		}
	}
	b.log.Error("Message %s not found in memory buffer", id)
	return false, errors.New("message not found in memory buffer")
}

// Start begins the buffer workers
func (b *MemoryBuffer) Start() {
	b.mu.Lock()
	b.isRunning = true
	b.mu.Unlock()

	adaptiveStr := "disabled"
	if b.config.Adaptive {
		adaptiveStr = "enabled"
	}
	b.log.Info(
		"Starting memory buffer with config: max_size=%d, flush_interval=%v, worker_count=%d, adaptive=%s",
		b.config.MaxSize,
		b.config.FlushInterval,
		b.config.WorkerCount,
		adaptiveStr,
	)

	// Start flush workers
	for i := 0; i < b.config.WorkerCount; i++ {
		b.wg.Add(1)
		go b.flushWorker(i)
	}
	b.log.Debug("Started %d flush workers", b.config.WorkerCount)

	// Start the batch collector
	b.wg.Add(1)
	go b.batchCollector()
	b.log.Debug("Started batch collector")

	// Start the flush ticker
	b.wg.Add(1)
	go FlushTicker(&GenericBuffer{
		mu:                  &b.mu,
		wg:                  &b.wg,
		adaptiveFlushTicker: &b.adaptiveFlushTicker,
		batch:               &b.batch,
		lastFlush:           &b.lastFlush,
		triggerFlush:        b.triggerFlush,
		ctx:                 b.ctx,
	}, b.config.FlushInterval, b.adaptiveEnabled, b.log)
	b.log.Debug("Started flush ticker")
	b.log.Info("Memory buffer started successfully")
}

// Stop gracefully stops the buffer
func (b *MemoryBuffer) Stop() error {
	b.log.Info("Stopping memory buffer...")

	b.mu.Lock()
	remainingInBatch := len(b.batch)
	b.isRunning = false
	b.mu.Unlock()

	b.log.Debug("Canceling buffer context and closing message channel")
	b.cancel()
	close(b.messages)

	b.log.Debug("Waiting for all buffer workers to complete")
	b.wg.Wait()

	// Flush any remaining messages
	b.log.Info("Flushing %d remaining messages before shutdown", remainingInBatch)
	if err := b.flush(); err != nil {
		b.log.Error("Failed to flush remaining messages during shutdown: %v", err)
		return err
	}

	b.log.Info("Memory buffer stopped successfully (total flushed: %d, errors: %d, dropped: %d)",
		b.totalFlushed, b.totalFlushErrors, b.messagesDropped)
	return nil
}

// Health returns the current health status of the buffer
func (b *MemoryBuffer) Health() *models.ComponentHealth {
	b.mu.Lock()
	defer b.mu.Unlock()

	status := models.HealthStatusHealthy
	message := "Memory buffer is operational"

	currentBatchSize := len(b.batch)
	timeSinceLastFlush := time.Since(b.lastFlush)
	channelUtilization := float64(len(b.messages)) / float64(cap(b.messages)) * 100

	currentMaxSize := b.config.MaxSize
	if b.config.Adaptive {
		currentMaxSize = b.adaptiveMaxSize
	}

	if !b.isRunning {
		status = models.HealthStatusUnhealthy
		message = "Memory buffer is not running - system shutting down or not started"
	} else if b.lastFlushError != nil {
		status = models.HealthStatusUnhealthy
		message = "Last flush failed: " + b.lastFlushError.Error() + " - messages may be lost"
	} else if currentBatchSize >= currentMaxSize {
		status = models.HealthStatusDegraded
		message = "Memory buffer is full - batch size reached maximum capacity. Flush may be blocked or slow."
	} else if currentBatchSize > 0 && timeSinceLastFlush > b.config.FlushInterval*2 {
		status = models.HealthStatusDegraded
		message = "Memory buffer flush is delayed - last flush was " + timeSinceLastFlush.String() + " ago (expected " + b.config.FlushInterval.String() + "). Database may be slow."
	} else if channelUtilization > 80 {
		status = models.HealthStatusDegraded
		message = "Message channel is " + formatFloat(channelUtilization) + "% full - producer may be overwhelming buffer capacity"
	} else if b.totalFlushErrors > 0 && b.totalFlushed > 0 {
		errorRate := float64(b.totalFlushErrors) / float64(b.totalFlushed+b.totalFlushErrors) * 100
		if errorRate > 5 {
			status = models.HealthStatusDegraded
			message = "High flush error rate: " + formatFloat(errorRate) + "% - database connection may be unstable"
		}
	}

	metadata := map[string]interface{}{
		"type":                "memory",
		"is_running":          b.isRunning,
		"current_batch_size":  currentBatchSize,
		"max_size":            b.config.MaxSize,
		"last_flush":          b.lastFlush,
		"time_since_flush":    timeSinceLastFlush.String(),
		"worker_count":        b.config.WorkerCount,
		"channel_size":        len(b.messages),
		"channel_capacity":    cap(b.messages),
		"channel_utilization": formatFloat(channelUtilization) + "%",
		"total_flushed":       b.totalFlushed,
		"total_flush_errors":  b.totalFlushErrors,
		"messages_dropped":    b.messagesDropped,
		"avg_flush_duration":  b.avgFlushDuration.String(),
		"last_flush_error":    formatError(b.lastFlushError),
		"adaptive_enabled":    b.config.Adaptive,
	}

	// Add adaptive-specific metadata
	if b.config.Adaptive {
		metadata["adaptive_max_size"] = b.adaptiveMaxSize
		metadata["base_max_size"] = b.config.MaxSize
		metadata["adaptive_min_size"] = b.adaptiveMinSize
		metadata["adaptive_tune_threshold"] = b.adaptiveTuneThreshold
		metadata["flush_count"] = b.flushCount
	}

	return &models.ComponentHealth{
		Name:      "buffer",
		Status:    status,
		Message:   message,
		CheckedAt: time.Now(),
		Metadata:  metadata,
	}
}

// formatFloat formats a float to 2 decimal places
func formatFloat(f float64) string {
	return fmt.Sprintf("%.2f", f)
}

// formatError safely formats an error for JSON
func formatError(err error) interface{} {
	if err == nil {
		return nil
	}
	return err.Error()
}

// Add adds a message to the buffer
func (b *MemoryBuffer) Add(msg *models.Message) error {
	select {
	case b.messages <- msg:
		b.log.Debug("Message %s added to memory buffer (channel: %s)", msg.ID, msg.Channel)
		return nil
	case <-b.ctx.Done():
		b.log.Warn("Failed to add message %s - memory buffer is shutting down", msg.ID)
		b.mu.Lock()
		b.messagesDropped++
		b.mu.Unlock()
		return b.ctx.Err()
	default:
		// Channel is full
		b.log.Error(
			"Message channel is full - dropping message %s (channel: %s)",
			msg.ID,
			msg.Channel,
		)
		b.mu.Lock()
		b.messagesDropped++
		b.mu.Unlock()
		return context.DeadlineExceeded
	}
}

// batchCollector collects messages into batches
func (b *MemoryBuffer) batchCollector() {
	defer b.wg.Done()

	b.log.Debug("Batch collector started")

	for {
		select {
		case msg, ok := <-b.messages:
			if !ok {
				b.log.Debug("Message channel closed - batch collector exiting")
				return
			}

			b.mu.Lock()
			b.batch = append(b.batch, msg)
			batchSize := len(b.batch)

			// Use adaptive max size if enabled, otherwise use config max size
			currentMaxSize := b.config.MaxSize
			if b.config.Adaptive {
				currentMaxSize = b.adaptiveMaxSize
			}

			shouldFlush := batchSize >= currentMaxSize
			b.mu.Unlock()

			if shouldFlush {
				b.log.Debug("Batch size reached max capacity (%d) - triggering flush", batchSize)
				b.triggerFlush()
			}

		case <-b.ctx.Done():
			b.log.Debug("Context cancelled - batch collector exiting")
			return
		}
	}
}

// triggerFlush signals that a flush should occur
func (b *MemoryBuffer) triggerFlush() {
	select {
	case b.flushChan <- struct{}{}:
		// Flush triggered successfully
	default:
		// Flush already pending
		b.log.Debug("Flush already pending - skipping trigger")
	}
}

// flushWorker processes flush requests
func (b *MemoryBuffer) flushWorker(workerID int) {
	defer b.wg.Done()

	b.log.Debug("Flush worker %d started", workerID)

	for {
		select {
		case <-b.flushChan:
			b.log.Debug("Flush worker %d received flush signal", workerID)
			if err := b.flush(); err != nil {
				b.log.Error("Flush worker %d failed to flush buffer: %v", workerID, err)

				// Update error tracking
				b.mu.Lock()
				b.lastFlushError = err
				b.totalFlushErrors++
				b.mu.Unlock()

				// Implement retry logic with exponential backoff
				b.retryFlush(workerID)
			} else {
				// Clear error on successful flush
				b.mu.Lock()
				b.lastFlushError = nil
				b.mu.Unlock()
			}

		case <-b.ctx.Done():
			b.log.Debug("Flush worker %d exiting", workerID)
			return
		}
	}
}

// retryFlush attempts to retry a failed flush with exponential backoff
func (b *MemoryBuffer) retryFlush(workerID int) {
	err := RetryFlush(
		b.ctx,
		b.log,
		3,
		100*time.Millisecond,
		workerID,
		b.flush,
	)
	if err != nil {
		b.log.Error("Failed to retry flush: %v", err)
		return
	} else {
		b.log.Info("Retry flush successful")
		b.mu.Lock()
		b.lastFlushError = nil
		b.mu.Unlock()
		return
	}
}

// flush writes the current batch to the database
func (b *MemoryBuffer) flush() error {
	b.mu.Lock()
	if len(b.batch) == 0 {
		b.lastFlush = time.Now()
		b.mu.Unlock()
		b.log.Debug("Flush called with empty batch - updating last flush time")
		return nil
	}

	// Take ownership of the current batch
	batch := b.batch
	batchSize := len(batch)

	// Reallocate batch with current adaptive size
	currentMaxSize := b.config.MaxSize
	if b.config.Adaptive {
		currentMaxSize = b.adaptiveMaxSize
	}
	b.batch = make([]*models.Message, 0, currentMaxSize)

	flushStartTime := time.Now()
	b.lastFlush = flushStartTime
	b.mu.Unlock()

	b.log.Info("Flushing batch of %d messages to database", batchSize)

	// Use batch insert for better performance
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startTime := time.Now()
	if err := b.store.BatchCreateMessages(ctx, batch); err != nil {
		b.log.Error("Failed to flush %d messages to database: %v", batchSize, err)

		// Re-add messages back to batch for retry (prevent data loss)
		b.mu.Lock()
		b.batch = append(batch, b.batch...)
		b.mu.Unlock()

		return err
	}

	duration := time.Since(startTime)

	// Update metrics
	b.mu.Lock()
	b.totalFlushed += int64(batchSize)
	b.flushCount++

	// Calculate rolling average flush duration
	if b.avgFlushDuration == 0 {
		b.avgFlushDuration = duration
	} else {
		// Simple moving average: new_avg = (old_avg * 0.9) + (new_value * 0.1)
		b.avgFlushDuration = time.Duration(float64(b.avgFlushDuration)*0.9 + float64(duration)*0.1)
	}
	b.mu.Unlock()

	b.log.Info("Successfully flushed %d messages in %v (avg: %v)",
		batchSize, duration, b.avgFlushDuration)

	// Log warning if flush is taking too long
	if duration > 5*time.Second {
		b.log.Warn("Slow flush detected: %d messages took %v to flush - database may be slow",
			batchSize, duration)
	}

	// Adaptive tuning - only run if adaptive is enabled and we've completed enough flushes
	if b.config.Adaptive {
		b.tuneAdaptiveSettings(batchSize, duration)
	}

	return nil
}

// tuneAdaptiveSettings adjusts the buffer parameters based on flush performance
func (b *MemoryBuffer) tuneAdaptiveSettings(batchSize int, flushDuration time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()

	TuneAdaptiveSettings(
		batchSize,
		b.adaptiveMaxSize,
		b.adaptiveMinSize,
		b.adaptiveTuneThreshold,
		b.flushCount,
		flushDuration,
		b.config.FlushInterval,
		b.log,
	)
}
