package buffer

import (
	"context"
	"time"

	"github.com/jaysongiroux/smq/internal/logger"
)

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
