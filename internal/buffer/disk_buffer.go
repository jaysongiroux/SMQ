package buffer

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/db"
	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/models"
)

// DiskBuffer is a disk-backed buffer that uses a Write-Ahead Log (WAL) for durability
// Messages are written to disk before being batched and flushed to the database
// This provides resilience against crashes but with slightly lower performance than memory buffer
type DiskBuffer struct {
	config         *Config
	store          db.Store
	walFile        *os.File
	walPath        string
	walMu          sync.Mutex // Protects WAL file operations
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
	walSize          int64
	flushCount       int64 // Track number of flushes for adaptive tuning
}

// NewDiskBuffer creates a new disk-backed buffer instance
func NewDiskBuffer(config *Config, store db.Store, log *logger.Logger) (*DiskBuffer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	b := &DiskBuffer{
		config:                config,
		store:                 store,
		log:                   log,
		walPath:               config.WALPath,
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

	// Open or create WAL file
	var err error
	// check if WAL file exists
	if _, err := os.Stat(b.walPath); os.IsNotExist(err) {
		// create WAL file
		b.walFile, err = os.OpenFile(b.walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
		if err != nil {
			return nil, fmt.Errorf("failed to create WAL file: %w", err)
		}
	} else {
		// open WAL file
		b.walFile, err = os.OpenFile(b.walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
		if err != nil {
			return nil, fmt.Errorf("failed to open WAL file: %w", err)
		}
	}
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	// Get WAL file size
	stat, err := b.walFile.Stat()
	if err != nil {
		defer func() {
			err = b.walFile.Close()
			if err != nil {
				log.Error("Failed to close WAL file: %v", err)
			}
		}()
		cancel()
		return nil, fmt.Errorf("failed to stat WAL file: %w", err)
	}
	b.walSize = stat.Size()

	log.Info("Opened disk buffer WAL file at %s (current size: %d bytes, adaptive: %v)",
		b.walPath, b.walSize, config.Adaptive)

	// Recover any messages from WAL on startup
	if err := b.recoverFromWAL(); err != nil {
		defer func() {
			err = b.walFile.Close()
			if err != nil {
				log.Error("Failed to close WAL file: %v", err)
			}
		}()
		cancel()
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	return b, nil
}

func (b *DiskBuffer) Remove(id uuid.UUID) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, msg := range b.batch {
		if msg.ID == id {
			b.batch = append(b.batch[:i], b.batch[i+1:]...)
			b.log.Info("Message %s removed from disk buffer", id)
			return true, nil
		}
	}
	b.log.Error("Message %s not found in disk buffer", id)
	return false, errors.New("message not found in disk buffer")
}

func (b *DiskBuffer) Start() {
	b.mu.Lock()
	b.isRunning = true
	b.mu.Unlock()

	adaptiveStr := "disabled"
	if b.config.Adaptive {
		adaptiveStr = "enabled"
	}
	b.log.Info("Starting disk buffer with config: max_size=%d, flush_interval=%v, worker_count=%d, wal_path=%s, adaptive=%s",
		b.config.MaxSize, b.config.FlushInterval, b.config.WorkerCount, b.walPath, adaptiveStr)

	for i := 0; i < b.config.WorkerCount; i++ {
		b.wg.Add(1)
		go b.flushWorker(i)
	}
	b.log.Debug("Started %d flush workers", b.config.WorkerCount)

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

	b.log.Info("Disk buffer started successfully")
}

// Stop gracefully stops the buffer
func (b *DiskBuffer) Stop() error {
	b.log.Info("Stopping disk buffer...")

	b.mu.Lock()
	remainingInBatch := len(b.batch)
	b.isRunning = false
	b.mu.Unlock()

	b.log.Debug("Canceling buffer context")
	b.cancel()

	b.log.Debug("Waiting for all buffer workers to complete")
	b.wg.Wait()

	// Flush any remaining messages
	b.log.Info("Flushing %d remaining messages before shutdown", remainingInBatch)
	if err := b.flush(); err != nil {
		b.log.Error("Failed to flush remaining messages during shutdown: %v", err)
		// Don't return error - still try to close WAL file
	}

	// Close WAL file
	b.walMu.Lock()
	if err := b.walFile.Close(); err != nil {
		b.log.Error("Failed to close WAL file: %v", err)
	} else {
		b.log.Info("WAL file closed successfully")
	}
	b.walMu.Unlock()

	b.log.Info("Disk buffer stopped successfully (total flushed: %d, errors: %d, dropped: %d)",
		b.totalFlushed, b.totalFlushErrors, b.messagesDropped)
	return nil
}

// Health returns the current health status of the buffer
func (b *DiskBuffer) Health() *models.ComponentHealth {
	b.mu.Lock()
	defer b.mu.Unlock()

	status := models.HealthStatusHealthy
	message := "Disk buffer is operational"

	currentBatchSize := len(b.batch)
	timeSinceLastFlush := time.Since(b.lastFlush)

	currentMaxSize := b.config.MaxSize
	if !b.config.Adaptive {
		currentMaxSize = b.config.MaxSize
	}

	if !b.isRunning {
		status = models.HealthStatusUnhealthy
		message = "Disk buffer is not running - system shutting down or not started"
	} else if b.lastFlushError != nil {
		status = models.HealthStatusUnhealthy
		message = "Last flush failed: " + b.lastFlushError.Error() + " - messages persisted in WAL"
	} else if currentBatchSize >= currentMaxSize {
		status = models.HealthStatusDegraded
		message = "Disk buffer is full - batch size reached maximum capacity. Flush may be blocked or slow."
	} else if currentBatchSize > 0 && timeSinceLastFlush > b.config.FlushInterval*2 {
		status = models.HealthStatusDegraded
		message = "Disk buffer flush is delayed - last flush was " + timeSinceLastFlush.String() + " ago (expected " + b.config.FlushInterval.String() + "). Database may be slow."
	} else if b.walSize > int64(b.config.MaxSize) {
		status = models.HealthStatusDegraded
		message = "WAL file is large (" + fmt.Sprintf("%.2f MB", float64(b.walSize)/1024/1024) + ") - database may be slow or down"
	} else if b.totalFlushErrors > 0 && b.totalFlushed > 0 {
		errorRate := float64(b.totalFlushErrors) / float64(b.totalFlushed+b.totalFlushErrors) * 100
		if errorRate > 5 {
			status = models.HealthStatusDegraded
			message = "High flush error rate: " + formatFloat(errorRate) + "% - database connection may be unstable"
		}
	}

	metadata := map[string]interface{}{
		"type":               "disk",
		"is_running":         b.isRunning,
		"current_batch_size": currentBatchSize,
		"max_size":           b.config.MaxSize,
		"last_flush":         b.lastFlush,
		"time_since_flush":   timeSinceLastFlush.String(),
		"worker_count":       b.config.WorkerCount,
		"wal_path":           b.walPath,
		"wal_size_bytes":     b.walSize,
		"wal_size_mb":        fmt.Sprintf("%.2f", float64(b.walSize)/1024/1024),
		"total_flushed":      b.totalFlushed,
		"total_flush_errors": b.totalFlushErrors,
		"messages_dropped":   b.messagesDropped,
		"avg_flush_duration": b.avgFlushDuration.String(),
		"last_flush_error":   formatError(b.lastFlushError),
		"adaptive_enabled":   b.config.Adaptive,
		"adaptive_max_size":  b.adaptiveMaxSize,
		"flush_count":        b.flushCount,
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

// Add adds a message to the buffer by writing it to the WAL
func (b *DiskBuffer) Add(msg *models.Message) error {
	// Write to WAL first
	b.walMu.Lock()
	encoder := json.NewEncoder(b.walFile)
	if err := encoder.Encode(msg); err != nil {
		b.walMu.Unlock()
		b.log.Error("Failed to write message %s to WAL: %v", msg.ID, err)
		b.mu.Lock()
		b.messagesDropped++
		b.mu.Unlock()
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Sync to ensure durability
	if err := b.walFile.Sync(); err != nil {
		b.walMu.Unlock()
		b.log.Error("Failed to sync WAL for message %s: %v", msg.ID, err)
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	// Update WAL size
	stat, err := b.walFile.Stat()
	if err == nil {
		b.walSize = stat.Size()
	}
	b.walMu.Unlock()

	// Add to in-memory batch
	b.mu.Lock()
	b.batch = append(b.batch, msg)
	batchSize := len(b.batch)

	// Use adaptive max size if enabled, otherwise use config max size
	currentMaxSize := b.adaptiveMaxSize
	if b.config.Adaptive {
		currentMaxSize = b.adaptiveMaxSize
	}

	shouldFlush := batchSize >= currentMaxSize
	b.mu.Unlock()

	b.log.Debug("Message %s added to disk buffer (channel: %s, wal_size: %d bytes, batch: %d/%d)",
		msg.ID, msg.Channel, b.walSize, batchSize, currentMaxSize)

	if shouldFlush {
		b.log.Debug("Batch size reached max capacity (%d) - triggering flush", batchSize)
		b.triggerFlush()
	}

	return nil
}

// recoverFromWAL reads and recovers messages from the WAL file on startup
func (b *DiskBuffer) recoverFromWAL() error {
	// Seek to beginning of file
	if _, err := b.walFile.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek WAL file: %w", err)
	}

	scanner := bufio.NewScanner(b.walFile)
	count := 0

	for scanner.Scan() {
		var msg models.Message
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			b.log.Warn("Failed to unmarshal WAL entry (skipping): %v", err)
			continue
		}

		b.batch = append(b.batch, &msg)
		count++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading WAL: %w", err)
	}

	if count > 0 {
		b.log.Info("Recovered %d messages from WAL - will flush to database", count)

		// Flush recovered messages to database
		if err := b.flush(); err != nil {
			return fmt.Errorf("failed to flush recovered messages: %w", err)
		}
	} else {
		b.log.Info("No messages to recover from WAL")
	}

	return nil
}

// triggerFlush signals that a flush should occur
func (b *DiskBuffer) triggerFlush() {
	select {
	case b.flushChan <- struct{}{}:
		// Flush triggered successfully
	default:
		// Flush already pending
		b.log.Debug("Flush already pending - skipping trigger")
	}
}

// flushWorker processes flush requests
func (b *DiskBuffer) flushWorker(workerID int) {
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
func (b *DiskBuffer) retryFlush(workerID int) {
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

// flush writes the current batch to the database and truncates the WAL
func (b *DiskBuffer) flush() error {
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

	b.log.Info("Flushing batch of %d messages to database from WAL", batchSize)

	// Use batch insert for better performance
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startTime := time.Now()
	if err := b.store.BatchCreateMessages(ctx, batch); err != nil {
		b.log.Error("Failed to flush %d messages to database: %v", batchSize, err)

		// Re-add messages back to batch (they're still in WAL)
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
		b.avgFlushDuration = time.Duration(float64(b.avgFlushDuration)*0.9 + float64(duration)*0.1)
	}
	b.mu.Unlock()

	// Truncate WAL file since messages are now in database
	b.walMu.Lock()
	if err := b.walFile.Truncate(0); err != nil {
		b.log.Error("Failed to truncate WAL file: %v", err)
	} else {
		if _, err := b.walFile.Seek(0, 0); err != nil {
			b.log.Error("Failed to seek WAL file: %v", err)
		} else {
			b.walSize = 0
			b.log.Debug("WAL file truncated after successful flush")
		}
	}
	b.walMu.Unlock()

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
func (b *DiskBuffer) tuneAdaptiveSettings(batchSize int, flushDuration time.Duration) {
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
