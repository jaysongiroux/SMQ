package buffer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/models"
)

type GenericBuffer struct {
	mu                  *sync.Mutex
	wg                  *sync.WaitGroup
	adaptiveFlushTicker **time.Ticker
	batch               *[]*models.Message
	lastFlush           *time.Time
	triggerFlush        func()
	ctx                 context.Context
}

// generic retry flush function
func RetryFlush(
	ctx context.Context,
	log *logger.Logger,
	maxRetries int,
	baseDelay time.Duration,
	workerID int,
	flush func() error,
) error {
	for attempt := 1; attempt <= maxRetries; attempt++ {
		shiftAmount := attempt - 1
		if shiftAmount < 0 {
			shiftAmount = 0
		}
		delay := baseDelay * time.Duration(1<<uint(shiftAmount)) // #nosec G115 - shiftAmount is always >= 0
		log.Warn("Retry flush in %d retrying flush in %v (attempt %d/%d)",
			workerID, delay, attempt, maxRetries)

		select {
		case <-time.After(delay):
			if err := flush(); err != nil {
				log.Error("Retry flush attempt %d failed: %v", attempt, err)
				if attempt == maxRetries {
					log.Error("Exhausted all retry attempts - messages persist in WAL")
				}
			} else {
				log.Info("Retry flush successful on attempt %d", attempt)
				return nil
			}
		case <-ctx.Done():
			log.Warn("Retry flush cancelled - context done")
			return context.Canceled
		}
	}

	return nil
}

// tuneAdaptiveSettings adjusts the buffer parameters based on flush performance
func TuneAdaptiveSettings(
	batchSize int,
	adaptiveMaxSize int,
	adaptiveMinSize int,
	adaptiveTuneThreshold int,
	flushCount int64,
	flushDuration time.Duration,
	flushInterval time.Duration,
	log *logger.Logger,
) {

	// Only tune every N flushes to avoid thrashing
	if flushCount%int64(adaptiveTuneThreshold) != 0 {
		return
	}

	oldMaxSize := adaptiveMaxSize

	// If flush took longer than 50% of our flush interval, we're at risk of overlapping
	// flushes, so we should reduce batch size to flush more frequently
	flushOverlapRisk := flushDuration > (flushInterval / 2)

	// If we filled the buffer and triggered a flush before the interval, we need more capacity
	batchWasFull := batchSize >= adaptiveMaxSize

	// If the buffer is filling faster than interval, AND flush is fast, increase size
	if batchWasFull && !flushOverlapRisk && flushDuration < (flushInterval/4) {
		// Flushes are fast and we're hitting capacity - increase batch size
		adaptiveMaxSize = min(adaptiveMaxSize*2, adaptiveMaxSize)
		log.Info("Adaptive tuning: Increasing max batch size from %d to %d (fast flushes, high throughput)",
			oldMaxSize, adaptiveMaxSize)

	} else if flushOverlapRisk {
		// Flushes are slow and risk overlapping with interval - reduce batch size
		// This ensures we flush smaller batches more frequently
		adaptiveMaxSize = max(adaptiveMaxSize/2, adaptiveMinSize)
		log.Info("Adaptive tuning: Decreasing max batch size from %d to %d (slow flushes, preventing overlap)",
			oldMaxSize, adaptiveMaxSize)

	} else if !batchWasFull && batchSize < (adaptiveMaxSize/4) {
		// Buffer is mostly empty at flush time - we can reduce capacity
		adaptiveMaxSize = max(adaptiveMaxSize/2, adaptiveMinSize)
		log.Info("Adaptive tuning: Decreasing max batch size from %d to %d (low throughput)",
			oldMaxSize, adaptiveMaxSize)
	}

	// Log tuning decision details for debugging
	if oldMaxSize != adaptiveMaxSize {
		log.Debug("Adaptive tuning details: batch_size=%d, flush_duration=%v, interval=%v, "+
			"overlap_risk=%v, was_full=%v, avg_flush=%v",
			batchSize, flushDuration, flushInterval, flushOverlapRisk, batchWasFull, flushDuration)
	}
}

func FlushTicker(b *GenericBuffer, flushInterval time.Duration, adaptiveEnabled bool, log *logger.Logger) {
	defer b.wg.Done()

	log.Debug("Flush ticker started with interval: %v (adaptive: %v)",
		flushInterval, adaptiveEnabled)

	ticker := time.NewTicker(flushInterval)
	*b.adaptiveFlushTicker = ticker
	defer func() {
		if *b.adaptiveFlushTicker != nil {
			(*b.adaptiveFlushTicker).Stop()
		}
	}()

	for {
		select {
		case <-ticker.C:
			b.mu.Lock()
			batchSize := len(*b.batch)
			timeSinceFlush := time.Since(*b.lastFlush)
			shouldFlush := batchSize > 0 && timeSinceFlush >= flushInterval
			b.mu.Unlock()

			if shouldFlush {
				log.Debug("Flush interval reached (%v since last flush) with %d messages - triggering flush",
					timeSinceFlush, batchSize)
				b.triggerFlush()
			}

		case <-b.ctx.Done():
			log.Debug("Context cancelled - flush ticker exiting")
			return
		}
	}
}

func RemoveMessage(id uuid.UUID, b *GenericBuffer, log *logger.Logger) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	// remove message based on id from buffer
	for i, msg := range *b.batch {
		if msg.ID == id {
			*b.batch = append((*b.batch)[:i], (*b.batch)[i+1:]...)
			log.Info("Message %s removed from buffer", id)
			return nil
		}
	}
	log.Error("Message %s not found in buffer", id)
	return errors.New("message not found in buffer")
}
