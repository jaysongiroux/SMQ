package buffer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/models"
	"github.com/jaysongiroux/smq/internal/testutils"
)

func createMemoryBufferConfig() *Config {
	return &Config{
		MaxSize:               100,
		FlushInterval:         100 * time.Millisecond,
		WorkerCount:           1,
		Adaptive:              false,
		AdaptiveMaxSize:       1000,
		AdaptiveMinSize:       10,
		AdaptiveTuneThreshold: 5,
	}
}

func createAdaptiveMemoryBufferConfig() *Config {
	cfg := createMemoryBufferConfig()
	cfg.Adaptive = true
	return cfg
}

func TestMemoryBufferNew(t *testing.T) {
	t.Run("creates with default config", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()

		buf := NewMemoryBuffer(cfg, store, log)

		if buf == nil {
			t.Fatal("Expected buffer to be created")
		}
		if buf.config.MaxSize != 100 {
			t.Errorf("Expected MaxSize 100, got %d", buf.config.MaxSize)
		}
		if cap(buf.messages) != 200 {
			t.Errorf("Expected channel capacity 200, got %d", cap(buf.messages))
		}
	})

	t.Run("creates with adaptive config", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createAdaptiveMemoryBufferConfig()

		buf := NewMemoryBuffer(cfg, store, log)

		if !buf.adaptiveEnabled {
			t.Error("Expected adaptive to be enabled")
		}
		if buf.adaptiveMaxSize != 1000 {
			t.Errorf("Expected adaptiveMaxSize 1000, got %d", buf.adaptiveMaxSize)
		}
	})
}

func TestMemoryBufferAdd(t *testing.T) {
	t.Run("adds message successfully", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		buf := NewMemoryBuffer(cfg, store, log)

		msg := &models.Message{
			ID:      uuid.New(),
			Channel: "test",
			Payload: []byte(`{"test":"data"}`),
		}

		err := buf.Add(msg)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		if len(buf.messages) != 1 {
			t.Errorf("Expected 1 message in channel, got %d", len(buf.messages))
		}
	})

	t.Run("drops message when channel full", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		cfg.MaxSize = 1
		buf := NewMemoryBuffer(cfg, store, log)

		// Fill channel to capacity (2x MaxSize)
		for i := 0; i < 2; i++ {
			msg := &models.Message{ID: uuid.New(), Channel: "test"}
			buf.Add(msg)
		}

		// This should be dropped
		msg := &models.Message{ID: uuid.New(), Channel: "test"}
		err := buf.Add(msg)

		if err == nil {
			t.Error("Expected error when channel full")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Expected DeadlineExceeded, got: %v", err)
		}

		buf.mu.Lock()
		dropped := buf.messagesDropped
		buf.mu.Unlock()

		if dropped != 1 {
			t.Errorf("Expected 1 dropped message, got %d", dropped)
		}
	})

	t.Run("fails when context canceled", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		cfg.MaxSize = 1
		buf := NewMemoryBuffer(cfg, store, log)

		// Fill the channel to capacity
		for i := 0; i < 2; i++ {
			msg := &models.Message{ID: uuid.New(), Channel: "test"}
			buf.Add(msg)
		}

		// Now cancel context
		buf.cancel()

		// Channel is full AND context is canceled
		msg := &models.Message{ID: uuid.New(), Channel: "test"}
		err := buf.Add(msg)

		if err == nil {
			t.Error("Expected error with canceled context and full channel")
		}

		buf.mu.Lock()
		dropped := buf.messagesDropped
		buf.mu.Unlock()

		// Should increment messagesDropped
		if dropped != 1 {
			t.Errorf("Expected 1 dropped message, got %d", dropped)
		}
	})
}

func TestMemoryBufferRemove(t *testing.T) {
	t.Run("removes message from batch", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		buf := NewMemoryBuffer(cfg, store, log)

		msg1 := &models.Message{ID: uuid.New()}
		msg2 := &models.Message{ID: uuid.New()}
		msg3 := &models.Message{ID: uuid.New()}

		buf.batch = []*models.Message{msg1, msg2, msg3}

		removed, err := buf.Remove(msg2.ID)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
		if !removed {
			t.Error("Expected message to be removed")
		}
		if len(buf.batch) != 2 {
			t.Errorf("Expected batch length 2, got %d", len(buf.batch))
		}
	})

	t.Run("returns error for non-existent message", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		buf := NewMemoryBuffer(cfg, store, log)

		removed, err := buf.Remove(uuid.New())
		if err == nil {
			t.Error("Expected error for non-existent message")
		}
		if removed {
			t.Error("Expected removed to be false")
		}
	})
}

func TestMemoryBufferFlush(t *testing.T) {
	t.Run("flushes batch successfully", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		buf := NewMemoryBuffer(cfg, store, log)

		msg1 := &models.Message{ID: uuid.New()}
		msg2 := &models.Message{ID: uuid.New()}
		buf.batch = []*models.Message{msg1, msg2}

		err := buf.flush()
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		if len(buf.batch) != 0 {
			t.Errorf("Expected empty batch after flush, got %d", len(buf.batch))
		}

		if buf.totalFlushed != 2 {
			t.Errorf("Expected totalFlushed 2, got %d", buf.totalFlushed)
		}
	})

	t.Run("handles empty batch", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		buf := NewMemoryBuffer(cfg, store, log)

		err := buf.flush()
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
	})

	t.Run("re-adds messages on failure", func(t *testing.T) {
		store := &testutils.MockStore{
			BatchCreateError: errors.New("database error"),
		}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		buf := NewMemoryBuffer(cfg, store, log)

		msg1 := &models.Message{ID: uuid.New()}
		msg2 := &models.Message{ID: uuid.New()}
		buf.batch = []*models.Message{msg1, msg2}

		err := buf.flush()
		if err == nil {
			t.Error("Expected error from failed flush")
		}

		if len(buf.batch) != 2 {
			t.Errorf("Expected messages to be re-added, got %d", len(buf.batch))
		}
	})

	t.Run("updates metrics", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		buf := NewMemoryBuffer(cfg, store, log)

		msg := &models.Message{ID: uuid.New()}
		buf.batch = []*models.Message{msg}

		buf.flush()

		if buf.flushCount != 1 {
			t.Errorf("Expected flushCount 1, got %d", buf.flushCount)
		}
		if buf.avgFlushDuration == 0 {
			t.Error("Expected avgFlushDuration to be set")
		}
	})
}

func TestMemoryBufferHealth(t *testing.T) {
	t.Run("returns healthy status", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		buf := NewMemoryBuffer(cfg, store, log)
		buf.isRunning = true

		health := buf.Health()

		if health.Status != models.HealthStatusHealthy {
			t.Errorf("Expected healthy status, got %s", health.Status)
		}
		if health.Name != "buffer" {
			t.Errorf("Expected name 'buffer', got %s", health.Name)
		}
	})

	t.Run("reports unhealthy when not running", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		buf := NewMemoryBuffer(cfg, store, log)
		buf.isRunning = false

		health := buf.Health()

		if health.Status != models.HealthStatusUnhealthy {
			t.Errorf("Expected unhealthy status, got %s", health.Status)
		}
	})

	t.Run("reports unhealthy on last flush error", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		buf := NewMemoryBuffer(cfg, store, log)
		buf.isRunning = true
		buf.lastFlushError = errors.New("flush failed")

		health := buf.Health()

		if health.Status != models.HealthStatusUnhealthy {
			t.Errorf("Expected unhealthy status, got %s", health.Status)
		}
	})

	t.Run("reports degraded when buffer full", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		cfg.MaxSize = 2
		buf := NewMemoryBuffer(cfg, store, log)
		buf.isRunning = true
		buf.batch = []*models.Message{{ID: uuid.New()}, {ID: uuid.New()}}

		health := buf.Health()

		if health.Status != models.HealthStatusDegraded {
			t.Errorf("Expected degraded status, got %s", health.Status)
		}
	})

	t.Run("reports degraded on delayed flush", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		buf := NewMemoryBuffer(cfg, store, log)
		buf.isRunning = true
		buf.batch = []*models.Message{{ID: uuid.New()}}
		buf.lastFlush = time.Now().Add(-5 * time.Minute)

		health := buf.Health()

		if health.Status != models.HealthStatusDegraded {
			t.Errorf("Expected degraded status, got %s", health.Status)
		}
	})

	t.Run("includes adaptive metadata", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createAdaptiveMemoryBufferConfig()
		buf := NewMemoryBuffer(cfg, store, log)
		buf.isRunning = true

		health := buf.Health()

		metadata := health.Metadata
		if metadata["adaptive_enabled"] != true {
			t.Error("Expected adaptive_enabled in metadata")
		}
		if metadata["adaptive_max_size"] == nil {
			t.Error("Expected adaptive_max_size in metadata")
		}
	})
}

func TestMemoryBufferStartStop(t *testing.T) {
	t.Run("starts and stops cleanly", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		buf := NewMemoryBuffer(cfg, store, log)

		buf.Start()

		if !buf.isRunning {
			t.Error("Expected buffer to be running")
		}

		time.Sleep(50 * time.Millisecond)

		err := buf.Stop()
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
		cfg := createMemoryBufferConfig()
		buf := NewMemoryBuffer(cfg, store, log)

		msg := &models.Message{ID: uuid.New()}
		buf.batch = []*models.Message{msg}
		buf.isRunning = true

		buf.Stop()

		if buf.totalFlushed != 1 {
			t.Errorf("Expected 1 message flushed, got %d", buf.totalFlushed)
		}
	})
}

func TestMemoryBufferTriggerFlush(t *testing.T) {
	t.Run("triggers flush successfully", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		buf := NewMemoryBuffer(cfg, store, log)

		buf.triggerFlush()

		select {
		case <-buf.flushChan:
			// Expected
		case <-time.After(100 * time.Millisecond):
			t.Error("Expected flush signal")
		}
	})

	t.Run("skips trigger when flush pending", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createMemoryBufferConfig()
		buf := NewMemoryBuffer(cfg, store, log)

		// Fill channel
		buf.flushChan <- struct{}{}

		// This should be skipped
		buf.triggerFlush()

		if len(buf.flushChan) != 1 {
			t.Error("Expected only one flush signal")
		}
	})
}

func TestMemoryBufferAdaptiveTuning(t *testing.T) {
	t.Run("calls tuning when adaptive enabled", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := createAdaptiveMemoryBufferConfig()
		cfg.AdaptiveTuneThreshold = 1
		buf := NewMemoryBuffer(cfg, store, log)

		msg := &models.Message{ID: uuid.New()}
		buf.batch = []*models.Message{msg}

		err := buf.flush()
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		// Verify tuning was attempted (flushCount incremented)
		if buf.flushCount != 1 {
			t.Errorf("Expected flushCount 1, got %d", buf.flushCount)
		}
	})
}
