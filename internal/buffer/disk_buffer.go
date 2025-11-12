package buffer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

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

	// Metrics
	totalFlushed     int64
	totalFlushErrors int64
	messagesDropped  int64
	avgFlushDuration time.Duration
	walSize          int64
}

const (
	maxWalSize = 100 * 1024 * 1024 // 100MB
)

// NewDiskBuffer creates a new disk-backed buffer instance
func NewDiskBuffer(config *Config, store db.Store, log *logger.Logger) (*DiskBuffer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	b := &DiskBuffer{
		config:    config,
		store:     store,
		log:       log,
		walPath:   config.WALPath,
		flushChan: make(chan struct{}, 1),
		ctx:       ctx,
		cancel:    cancel,
		batch:     make([]*models.Message, 0, config.MaxSize),
		lastFlush: time.Now(),
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

	log.Info("Opened disk buffer WAL file at %s (current size: %d bytes)", b.walPath, b.walSize)

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

// Start begins the buffer workers
func (b *DiskBuffer) Start() {
	b.mu.Lock()
	b.isRunning = true
	b.mu.Unlock()

	b.log.Info("Starting disk buffer with config: max_size=%d, flush_interval=%v, worker_count=%d, wal_path=%s",
		b.config.MaxSize, b.config.FlushInterval, b.config.WorkerCount, b.walPath)

	// Start flush workers
	for i := 0; i < b.config.WorkerCount; i++ {
		b.wg.Add(1)
		go b.flushWorker(i)
	}
	b.log.Debug("Started %d flush workers", b.config.WorkerCount)

	// Start the flush ticker
	b.wg.Add(1)
	go b.flushTicker()
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

	if !b.isRunning {
		status = models.HealthStatusUnhealthy
		message = "Disk buffer is not running - system shutting down or not started"
	} else if b.lastFlushError != nil {
		status = models.HealthStatusUnhealthy
		message = "Last flush failed: " + b.lastFlushError.Error() + " - messages persisted in WAL"
	} else if currentBatchSize >= b.config.MaxSize {
		status = models.HealthStatusDegraded
		message = "Disk buffer is full - batch size reached maximum capacity. Flush may be blocked or slow."
	} else if currentBatchSize > 0 && timeSinceLastFlush > b.config.FlushInterval*2 {
		status = models.HealthStatusDegraded
		message = "Disk buffer flush is delayed - last flush was " + timeSinceLastFlush.String() + " ago (expected " + b.config.FlushInterval.String() + "). Database may be slow."
	} else if b.walSize > maxWalSize {
		status = models.HealthStatusDegraded
		message = "WAL file is large (" + fmt.Sprintf("%.2f MB", float64(b.walSize)/1024/1024) + ") - database may be slow or down"
	} else if b.totalFlushErrors > 0 && b.totalFlushed > 0 {
		errorRate := float64(b.totalFlushErrors) / float64(b.totalFlushed+b.totalFlushErrors) * 100
		if errorRate > 5 {
			status = models.HealthStatusDegraded
			message = "High flush error rate: " + formatFloat(errorRate) + "% - database connection may be unstable"
		}
	}

	return &models.ComponentHealth{
		Name:      "buffer",
		Status:    status,
		Message:   message,
		CheckedAt: time.Now(),
		Metadata: map[string]interface{}{
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
		},
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
	shouldFlush := batchSize >= b.config.MaxSize
	b.mu.Unlock()

	b.log.Debug("Message %s added to disk buffer (channel: %s, wal_size: %d bytes)",
		msg.ID, msg.Channel, b.walSize)

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

// flushTicker periodically triggers a flush based on the flush interval
func (b *DiskBuffer) flushTicker() {
	defer b.wg.Done()

	b.log.Debug("Flush ticker started with interval: %v", b.config.FlushInterval)

	ticker := time.NewTicker(b.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.mu.Lock()
			batchSize := len(b.batch)
			timeSinceFlush := time.Since(b.lastFlush)
			shouldFlush := batchSize > 0 && timeSinceFlush >= b.config.FlushInterval
			b.mu.Unlock()

			if shouldFlush {
				b.log.Debug("Flush interval reached (%v since last flush) with %d messages - triggering flush",
					timeSinceFlush, batchSize)
				b.triggerFlush()
			}

		case <-b.ctx.Done():
			b.log.Debug("Context cancelled - flush ticker exiting")
			return
		}
	}
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
		b.mu.Unlock()
		b.log.Debug("Flush called with empty batch - skipping")
		return nil
	}

	// Take ownership of the current batch
	batch := b.batch
	batchSize := len(batch)
	b.batch = make([]*models.Message, 0, b.config.MaxSize)
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

	return nil
}
