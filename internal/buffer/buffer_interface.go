package buffer

import (
	"time"

	"github.com/jaysongiroux/smq/internal/models"
)

// Buffer is an interface for different buffer implementations
// Buffers batch messages before writing to the database to improve performance
type Buffer interface {
	// Start begins the buffer workers
	Start()

	// Stop gracefully stops the buffer and flushes remaining messages
	Stop() error

	// Add adds a message to the buffer
	Add(msg *models.Message) error

	// Health returns the current health status of the buffer
	Health() *models.ComponentHealth
}

// Config holds configuration for any buffer type
type Config struct {
	MaxSize       int           // Maximum number of messages in buffer before flush
	FlushInterval time.Duration // Maximum time to wait before flushing
	WorkerCount   int           // Number of worker goroutines for flushing
	WALPath       string        // Path for WAL file (disk buffer only)
}
