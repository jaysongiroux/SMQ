package buffer

import (
	"fmt"
	"time"

	"github.com/jaysongiroux/smq/internal/config"
	"github.com/jaysongiroux/smq/internal/db"
	"github.com/jaysongiroux/smq/internal/logger"
)

// NewBuffer creates a new buffer based on the configuration
// It reads the "buffer_type" value from config and returns the appropriate implementation
func NewBuffer(cfg *config.Config, store db.Store, log *logger.Logger) (Buffer, error) {
	// Create buffer config
	bufferConfig := &Config{
		MaxSize:               cfg.BufferMaxSizeKb,
		FlushInterval:         time.Duration(cfg.BufferFlushIntervalMs) * time.Millisecond,
		WorkerCount:           cfg.BufferWorkerCount,
		WALPath:               cfg.BufferWALPath,
		Adaptive:              cfg.BufferAdaptive,
		AdaptiveMaxSize:       cfg.BufferAdaptiveMaxSize,
		AdaptiveTuneThreshold: cfg.BufferAdaptiveTuneThreshold,
		AdaptiveMinSize:       cfg.BufferAdaptiveMinSize,
	}

	switch cfg.BufferType {
	case "memory":
		return NewMemoryBuffer(bufferConfig, store, log), nil
	case "disk":
		return NewDiskBuffer(bufferConfig, store, log)
	default:
		return nil, fmt.Errorf("unsupported buffer type: %s (supported: memory, disk)", cfg.BufferType)
	}
}
