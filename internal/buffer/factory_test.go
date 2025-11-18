package buffer

import (
	"strings"
	"testing"
	"time"

	"github.com/jaysongiroux/smq/internal/config"
	"github.com/jaysongiroux/smq/internal/testutils"
)

func createTestConfig(bufferType string) *config.Config {
	return &config.Config{
		BufferType:                  bufferType,
		BufferMaxSizeKb:             1000,
		BufferFlushIntervalMs:       1000,
		BufferWorkerCount:           2,
		BufferWALPath:               "/tmp/test.wal",
		BufferAdaptive:              true,
		BufferAdaptiveMaxSize:       5000,
		BufferAdaptiveTuneThreshold: 5,
		BufferAdaptiveMinSize:       100,
	}
}

func TestNewBuffer(t *testing.T) {
	log := testutils.CreateTestLogger()
	store := &testutils.MockStore{}

	t.Run("creates memory buffer", func(t *testing.T) {
		cfg := createTestConfig("memory")

		buf, err := NewBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if buf == nil {
			t.Fatal("Expected buffer to be created")
		}

		// Type assertion to verify it's a memory buffer
		if _, ok := buf.(*MemoryBuffer); !ok {
			t.Error("Expected MemoryBuffer type")
		}
	})

	t.Run("creates disk buffer", func(t *testing.T) {
		cfg := createTestConfig("disk")

		buf, err := NewBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if buf == nil {
			t.Fatal("Expected buffer to be created")
		}

		// Type assertion to verify it's a disk buffer
		if _, ok := buf.(*DiskBuffer); !ok {
			t.Error("Expected DiskBuffer type")
		}
	})

	t.Run("fails with unsupported buffer type", func(t *testing.T) {
		cfg := createTestConfig("redis")

		buf, err := NewBuffer(cfg, store, log)
		if err == nil {
			t.Error("Expected error with unsupported buffer type")
		}

		if buf != nil {
			t.Error("Expected nil buffer on error")
		}

		if !strings.Contains(err.Error(), "unsupported buffer type") {
			t.Errorf("Expected 'unsupported buffer type' error, got: %v", err)
		}
	})

	t.Run("passes config correctly to memory buffer", func(t *testing.T) {
		cfg := createTestConfig("memory")
		cfg.BufferMaxSizeKb = 2000
		cfg.BufferFlushIntervalMs = 500
		cfg.BufferWorkerCount = 4

		buf, err := NewBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		memBuf := buf.(*MemoryBuffer)

		if memBuf.config.MaxSize != 2000 {
			t.Errorf("Expected MaxSize 2000, got %d", memBuf.config.MaxSize)
		}

		if memBuf.config.FlushInterval != 500*time.Millisecond {
			t.Errorf("Expected FlushInterval 500ms, got %v", memBuf.config.FlushInterval)
		}

		if memBuf.config.WorkerCount != 4 {
			t.Errorf("Expected WorkerCount 4, got %d", memBuf.config.WorkerCount)
		}
	})

	t.Run("passes adaptive config correctly", func(t *testing.T) {
		cfg := createTestConfig("memory")
		cfg.BufferAdaptive = true
		cfg.BufferAdaptiveMaxSize = 10000
		cfg.BufferAdaptiveMinSize = 200
		cfg.BufferAdaptiveTuneThreshold = 10

		buf, err := NewBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		memBuf := buf.(*MemoryBuffer)

		if !memBuf.config.Adaptive {
			t.Error("Expected Adaptive to be true")
		}

		if memBuf.config.AdaptiveMaxSize != 10000 {
			t.Errorf("Expected AdaptiveMaxSize 10000, got %d", memBuf.config.AdaptiveMaxSize)
		}

		if memBuf.config.AdaptiveMinSize != 200 {
			t.Errorf("Expected AdaptiveMinSize 200, got %d", memBuf.config.AdaptiveMinSize)
		}

		if memBuf.config.AdaptiveTuneThreshold != 10 {
			t.Errorf("Expected AdaptiveTuneThreshold 10, got %d", memBuf.config.AdaptiveTuneThreshold)
		}
	})

	t.Run("fails with invalid WAL path", func(t *testing.T) {
		cfg := createTestConfig("disk")
		cfg.BufferWALPath = "/invalid/path/wal.log"

		_, err := NewBuffer(cfg, store, log)
		if err == nil {
			t.Error("Expected error with invalid WAL path")
		}
	})

	t.Run("passes WAL path to disk buffer", func(t *testing.T) {
		cfg := createTestConfig("disk")
		cfg.BufferWALPath = "./wal.log"

		buf, err := NewBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		diskBuf := buf.(*DiskBuffer)

		if diskBuf.config.WALPath != "./wal.log" {
			t.Errorf("Expected WALPath './wal.log', got %s", diskBuf.config.WALPath)
		}
	})
}
