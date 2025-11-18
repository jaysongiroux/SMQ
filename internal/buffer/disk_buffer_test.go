package buffer

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/models"
	"github.com/jaysongiroux/smq/internal/testutils"
)

func createDiskBufferConfig(walPath string) *Config {
	return &Config{
		MaxSize:               100,
		FlushInterval:         100 * time.Millisecond,
		WorkerCount:           2,
		WALPath:               walPath,
		Adaptive:              false,
		AdaptiveMaxSize:       1000,
		AdaptiveMinSize:       10,
		AdaptiveTuneThreshold: 5,
	}
}

func createAdaptiveDiskBufferConfig(walPath string) *Config {
	cfg := createDiskBufferConfig(walPath)
	cfg.Adaptive = true
	return cfg
}

func createTempWAL(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	return walPath
}

func TestDiskBufferNew(t *testing.T) {
	t.Run("creates with valid WAL path", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)
		cfg := createDiskBufferConfig(walPath)

		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if buf == nil {
			t.Fatal("Expected buffer to be created")
		}

		defer buf.walFile.Close()

		if buf.walPath != walPath {
			t.Errorf("Expected walPath %s, got %s", walPath, buf.walPath)
		}
	})

	t.Run("fails with non-existent directory", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createDiskBufferConfig("/nonexistent/dir/wal.log")

		buf, err := NewDiskBuffer(cfg, store, log)
		if err == nil {
			t.Error("Expected error with non-existent directory")
		}

		if buf != nil {
			t.Error("Expected nil buffer on error")
		}
	})

	t.Run("creates with adaptive config", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)
		cfg := createAdaptiveDiskBufferConfig(walPath)

		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		defer buf.walFile.Close()

		if !buf.adaptiveEnabled {
			t.Error("Expected adaptive to be enabled")
		}
	})

	t.Run("opens existing WAL file", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)

		// Create file first
		file, _ := os.Create(walPath)
		file.WriteString("existing data\n")
		file.Close()

		cfg := createDiskBufferConfig(walPath)
		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		defer buf.walFile.Close()

		if buf.walSize == 0 {
			t.Error("Expected WAL size to reflect existing data")
		}
	})
}

func TestDiskBufferAdd(t *testing.T) {
	t.Run("adds message to WAL and batch", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)
		cfg := createDiskBufferConfig(walPath)

		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		defer buf.walFile.Close()

		msg := &models.Message{
			ID:      uuid.New(),
			Channel: "test",
			Payload: []byte(`{"test":"data"}`),
		}

		err = buf.Add(msg)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		if len(buf.batch) != 1 {
			t.Errorf("Expected 1 message in batch, got %d", len(buf.batch))
		}

		if buf.walSize == 0 {
			t.Error("Expected WAL file to have content")
		}
	})
	t.Run("triggers flush when batch full", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)
		cfg := createDiskBufferConfig(walPath)
		cfg.MaxSize = 2
		cfg.WorkerCount = 1

		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		buf.Start()
		defer buf.Stop()

		// Add messages up to capacity
		msg1 := &models.Message{ID: uuid.New(), Channel: "test", Payload: []byte(`{}`)}
		msg2 := &models.Message{ID: uuid.New(), Channel: "test", Payload: []byte(`{}`)}

		err = buf.Add(msg1)
		if err != nil {
			t.Errorf("Expected no error adding first message, got: %v", err)
		}

		// After first message, flush should not be triggered
		if len(buf.flushChan) != 0 {
			t.Error("Expected no flush signal after first message")
		}

		err = buf.Add(msg2)
		if err != nil {
			t.Errorf("Expected no error adding second message, got: %v", err)
		}

		// Wait for the flush to complete
		time.Sleep(100 * time.Millisecond)

		// After flush completes, batch should be empty
		buf.mu.Lock()
		batchLen := len(buf.batch)
		buf.mu.Unlock()

		if batchLen != 0 {
			t.Errorf("Expected batch to be empty after flush, got %d messages", batchLen)
		}

		// Verify messages were flushed to store
		if store.BatchCreateMessagesCalled == 0 {
			t.Error("Expected BatchCreateMessages to be called")
		}
	})

	t.Run("increments messagesDropped on WAL write failure", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)
		cfg := createDiskBufferConfig(walPath)

		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		// Close WAL to force error
		buf.walFile.Close()

		msg := &models.Message{ID: uuid.New(), Channel: "test"}
		err = buf.Add(msg)

		if err == nil {
			t.Error("Expected error with closed WAL")
		}

		if buf.messagesDropped != 1 {
			t.Errorf("Expected 1 dropped message, got %d", buf.messagesDropped)
		}
	})
}

func TestDiskBufferRemove(t *testing.T) {
	t.Run("removes message from batch", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)
		cfg := createDiskBufferConfig(walPath)

		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		defer buf.walFile.Close()

		msg1 := &models.Message{ID: uuid.New()}
		msg2 := &models.Message{ID: uuid.New()}
		buf.batch = []*models.Message{msg1, msg2}

		removed, err := buf.Remove(msg1.ID)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
		if !removed {
			t.Error("Expected message to be removed")
		}
		if len(buf.batch) != 1 {
			t.Errorf("Expected batch length 1, got %d", len(buf.batch))
		}
	})

	t.Run("returns error for non-existent message", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)
		cfg := createDiskBufferConfig(walPath)

		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		defer buf.walFile.Close()

		removed, err := buf.Remove(uuid.New())
		if err == nil {
			t.Error("Expected error for non-existent message")
		}
		if removed {
			t.Error("Expected removed to be false")
		}
	})
}

func TestDiskBufferFlush(t *testing.T) {
	t.Run("flushes batch and truncates WAL", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)
		cfg := createDiskBufferConfig(walPath)

		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		defer buf.walFile.Close()

		msg1 := &models.Message{ID: uuid.New()}
		msg2 := &models.Message{ID: uuid.New()}
		buf.batch = []*models.Message{msg1, msg2}
		buf.walSize = 1000

		err = buf.flush()
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		if len(buf.batch) != 0 {
			t.Errorf("Expected empty batch, got %d", len(buf.batch))
		}

		if buf.walSize != 0 {
			t.Errorf("Expected WAL size 0 after truncate, got %d", buf.walSize)
		}

		if buf.totalFlushed != 2 {
			t.Errorf("Expected totalFlushed 2, got %d", buf.totalFlushed)
		}
	})

	t.Run("handles empty batch", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)
		cfg := createDiskBufferConfig(walPath)

		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		defer buf.walFile.Close()

		err = buf.flush()
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
	})

	t.Run("re-adds messages on failure", func(t *testing.T) {
		store := &testutils.MockStore{
			BatchCreateError: errors.New("database error"),
		}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)
		cfg := createDiskBufferConfig(walPath)

		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		defer buf.walFile.Close()

		msg := &models.Message{ID: uuid.New()}
		buf.batch = []*models.Message{msg}

		err = buf.flush()
		if err == nil {
			t.Error("Expected error from failed flush")
		}

		if len(buf.batch) != 1 {
			t.Errorf("Expected messages to be re-added, got %d", len(buf.batch))
		}
	})
}

func TestDiskBufferRecoverFromWAL(t *testing.T) {
	t.Run("recovers messages from WAL", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)

		// Write messages to WAL file
		msg1 := &models.Message{ID: uuid.New(), Channel: "test1"}
		msg2 := &models.Message{ID: uuid.New(), Channel: "test2"}

		file, _ := os.Create(walPath)
		encoder := json.NewEncoder(file)
		encoder.Encode(msg1)
		encoder.Encode(msg2)
		file.Close()

		cfg := createDiskBufferConfig(walPath)
		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		defer buf.walFile.Close()

		// Messages should have been flushed during recovery
		if buf.totalFlushed != 2 {
			t.Errorf("Expected 2 flushed messages, got %d", buf.totalFlushed)
		}
	})

	t.Run("handles empty WAL", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)

		cfg := createDiskBufferConfig(walPath)
		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		defer buf.walFile.Close()

		if buf.totalFlushed != 0 {
			t.Error("Expected no messages flushed from empty WAL")
		}
	})

	t.Run("skips corrupt WAL entries", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)

		// Write corrupt data to WAL
		file, _ := os.Create(walPath)
		file.WriteString("corrupt json\n")
		file.WriteString("{\"valid\":\"json\"}\n")
		file.Close()

		cfg := createDiskBufferConfig(walPath)
		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		defer buf.walFile.Close()
	})
}

func TestDiskBufferHealth(t *testing.T) {
	t.Run("returns healthy status", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)
		cfg := createDiskBufferConfig(walPath)

		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		defer buf.walFile.Close()

		buf.isRunning = true
		health := buf.Health()

		if health.Status != models.HealthStatusHealthy {
			t.Errorf("Expected healthy status, got %s", health.Status)
		}
	})

	t.Run("reports unhealthy when not running", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)
		cfg := createDiskBufferConfig(walPath)

		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		defer buf.walFile.Close()

		buf.isRunning = false
		health := buf.Health()

		if health.Status != models.HealthStatusUnhealthy {
			t.Errorf("Expected unhealthy status, got %s", health.Status)
		}
	})

	t.Run("reports degraded with large WAL", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)
		cfg := createDiskBufferConfig(walPath)

		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		defer buf.walFile.Close()

		buf.isRunning = true
		buf.walSize = 10000000

		health := buf.Health()

		if health.Status != models.HealthStatusDegraded {
			t.Errorf("Expected degraded status, got %s", health.Status)
		}
	})

	t.Run("includes WAL metadata", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)
		cfg := createDiskBufferConfig(walPath)

		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		defer buf.walFile.Close()

		buf.isRunning = true
		health := buf.Health()

		metadata := health.Metadata
		if metadata["wal_path"] != walPath {
			t.Error("Expected wal_path in metadata")
		}
		if metadata["wal_size_bytes"] == nil {
			t.Error("Expected wal_size_bytes in metadata")
		}
	})
}

func TestDiskBufferStartStop(t *testing.T) {
	t.Run("starts and stops cleanly", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)
		cfg := createDiskBufferConfig(walPath)

		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		buf.Start()

		if !buf.isRunning {
			t.Error("Expected buffer to be running")
		}

		time.Sleep(50 * time.Millisecond)

		err = buf.Stop()
		if err != nil {
			t.Errorf("Expected no error on stop, got: %v", err)
		}

		if buf.isRunning {
			t.Error("Expected buffer to not be running")
		}
	})

	t.Run("flushes remaining messages on stop", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		walPath := createTempWAL(t)
		cfg := createDiskBufferConfig(walPath)

		buf, err := NewDiskBuffer(cfg, store, log)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		msg := &models.Message{ID: uuid.New()}
		buf.batch = []*models.Message{msg}
		buf.isRunning = true

		buf.Stop()

		if buf.totalFlushed != 1 {
			t.Errorf("Expected 1 message flushed, got %d", buf.totalFlushed)
		}
	})
}
