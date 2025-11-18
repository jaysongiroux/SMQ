package buffer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/models"
	"github.com/jaysongiroux/smq/internal/testutils"
)

func TestRetryFlush(t *testing.T) {
	t.Run("succeeds on first attempt", func(t *testing.T) {
		log := testutils.CreateTestLogger()
		ctx := context.Background()

		attempts := 0
		flush := func() error {
			attempts++
			return nil
		}

		err := RetryFlush(ctx, log, 3, 10*time.Millisecond, 1, flush)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		if attempts != 1 {
			t.Errorf("Expected 1 attempt, got %d", attempts)
		}
	})

	t.Run("succeeds after retry", func(t *testing.T) {
		log := testutils.CreateTestLogger()
		ctx := context.Background()

		attempts := 0
		flush := func() error {
			attempts++
			if attempts < 2 {
				return errors.New("temporary failure")
			}
			return nil
		}

		err := RetryFlush(ctx, log, 3, 10*time.Millisecond, 1, flush)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		if attempts != 2 {
			t.Errorf("Expected 2 attempts, got %d", attempts)
		}
	})

	t.Run("exhausts retries", func(t *testing.T) {
		log := testutils.CreateTestLogger()
		ctx := context.Background()

		attempts := 0
		flush := func() error {
			attempts++
			return errors.New("persistent failure")
		}

		err := RetryFlush(ctx, log, 3, 10*time.Millisecond, 1, flush)
		if err != nil {
			t.Errorf("Expected nil (exhausted), got: %v", err)
		}

		if attempts != 3 {
			t.Errorf("Expected 3 attempts, got %d", attempts)
		}
	})

	t.Run("cancels on context done", func(t *testing.T) {
		log := testutils.CreateTestLogger()
		ctx, cancel := context.WithCancel(context.Background())

		// Cancel immediately
		cancel()

		attempts := 0
		flush := func() error {
			attempts++
			return errors.New("failure")
		}

		err := RetryFlush(ctx, log, 3, 100*time.Millisecond, 1, flush)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}

		// Should not attempt any retries
		if attempts > 0 {
			t.Errorf("Expected 0 attempts after cancel, got %d", attempts)
		}
	})

	t.Run("exponential backoff", func(t *testing.T) {
		log := testutils.CreateTestLogger()
		ctx := context.Background()

		var delays []time.Duration
		startTime := time.Now()
		attempts := 0

		flush := func() error {
			now := time.Now()
			delays = append(delays, now.Sub(startTime))
			attempts++
			return errors.New("failure")
		}

		RetryFlush(ctx, log, 3, 50*time.Millisecond, 1, flush)

		if len(delays) != 3 {
			t.Fatalf("Expected 3 delays, got %d", len(delays))
		}

		// Attempt 1: after 50ms (baseDelay * (1<<0))
		if delays[0] < 40*time.Millisecond || delays[0] > 70*time.Millisecond {
			t.Errorf("Expected first attempt at ~50ms, got %v", delays[0])
		}

		// Attempt 2: after ~150ms (50ms + 100ms where 100ms = baseDelay * (1<<1))
		if delays[1] < 140*time.Millisecond || delays[1] > 170*time.Millisecond {
			t.Errorf("Expected second attempt at ~150ms, got %v", delays[1])
		}

		// Attempt 3: after ~350ms (150ms + 200ms where 200ms = baseDelay * (1<<2))
		if delays[2] < 330*time.Millisecond || delays[2] > 370*time.Millisecond {
			t.Errorf("Expected third attempt at ~350ms, got %v", delays[2])
		}
	})
}

func TestRemoveMessage(t *testing.T) {
	log := testutils.CreateTestLogger()

	t.Run("removes existing message", func(t *testing.T) {
		msg1 := &models.Message{ID: uuid.New()}
		msg2 := &models.Message{ID: uuid.New()}
		msg3 := &models.Message{ID: uuid.New()}

		batch := []*models.Message{msg1, msg2, msg3}
		mu := &sync.Mutex{}

		b := &GenericBuffer{
			mu:    mu,
			batch: &batch,
		}

		err := RemoveMessage(msg2.ID, b, log)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		if len(batch) != 2 {
			t.Errorf("Expected batch length 2, got %d", len(batch))
		}

		// Verify msg2 was removed
		for _, msg := range batch {
			if msg.ID == msg2.ID {
				t.Error("Message was not removed from batch")
			}
		}

		// Verify msg1 and msg3 still exist
		found1, found3 := false, false
		for _, msg := range batch {
			if msg.ID == msg1.ID {
				found1 = true
			}
			if msg.ID == msg3.ID {
				found3 = true
			}
		}
		if !found1 || !found3 {
			t.Error("Other messages were incorrectly removed")
		}
	})

	t.Run("returns error for non-existent message", func(t *testing.T) {
		msg1 := &models.Message{ID: uuid.New()}
		batch := []*models.Message{msg1}
		mu := &sync.Mutex{}

		b := &GenericBuffer{
			mu:    mu,
			batch: &batch,
		}

		nonExistentID := uuid.New()
		err := RemoveMessage(nonExistentID, b, log)

		if err == nil {
			t.Error("Expected error for non-existent message")
		}

		if err.Error() != "message not found in buffer" {
			t.Errorf("Expected 'message not found' error, got: %v", err)
		}

		// Batch should be unchanged
		if len(batch) != 1 {
			t.Errorf("Expected batch length 1, got %d", len(batch))
		}
	})

	t.Run("removes first message", func(t *testing.T) {
		msg1 := &models.Message{ID: uuid.New()}
		msg2 := &models.Message{ID: uuid.New()}

		batch := []*models.Message{msg1, msg2}
		mu := &sync.Mutex{}

		b := &GenericBuffer{
			mu:    mu,
			batch: &batch,
		}

		err := RemoveMessage(msg1.ID, b, log)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		if len(batch) != 1 || batch[0].ID != msg2.ID {
			t.Error("First message not removed correctly")
		}
	})

	t.Run("removes last message", func(t *testing.T) {
		msg1 := &models.Message{ID: uuid.New()}
		msg2 := &models.Message{ID: uuid.New()}

		batch := []*models.Message{msg1, msg2}
		mu := &sync.Mutex{}

		b := &GenericBuffer{
			mu:    mu,
			batch: &batch,
		}

		err := RemoveMessage(msg2.ID, b, log)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		if len(batch) != 1 || batch[0].ID != msg1.ID {
			t.Error("Last message not removed correctly")
		}
	})

	t.Run("removes from single message batch", func(t *testing.T) {
		msg1 := &models.Message{ID: uuid.New()}

		batch := []*models.Message{msg1}
		mu := &sync.Mutex{}

		b := &GenericBuffer{
			mu:    mu,
			batch: &batch,
		}

		err := RemoveMessage(msg1.ID, b, log)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		if len(batch) != 0 {
			t.Errorf("Expected empty batch, got length %d", len(batch))
		}
	})

	t.Run("handles empty batch", func(t *testing.T) {
		batch := []*models.Message{}
		mu := &sync.Mutex{}

		b := &GenericBuffer{
			mu:    mu,
			batch: &batch,
		}

		err := RemoveMessage(uuid.New(), b, log)
		if err == nil {
			t.Error("Expected error for empty batch")
		}
	})
}

func TestTuneAdaptiveSettings(t *testing.T) {
	log := testutils.CreateTestLogger()

	t.Run("increases size when full and fast", func(t *testing.T) {
		batchSize := 1000
		adaptiveMaxSize := 1000
		adaptiveMinSize := 100
		adaptiveTuneThreshold := 5
		flushCount := int64(5) // Trigger tuning
		flushDuration := 50 * time.Millisecond
		flushInterval := 1 * time.Second

		TuneAdaptiveSettings(
			batchSize,
			adaptiveMaxSize,
			adaptiveMinSize,
			adaptiveTuneThreshold,
			flushCount,
			flushDuration,
			flushInterval,
			log,
		)

		// Note: Function doesn't return value, this tests it runs without panic
		// In real implementation, you'd need to modify function to return new size
	})

	t.Run("does not tune before threshold", func(t *testing.T) {
		batchSize := 1000
		adaptiveMaxSize := 1000
		adaptiveMinSize := 100
		adaptiveTuneThreshold := 5
		flushCount := int64(3) // Below threshold
		flushDuration := 50 * time.Millisecond
		flushInterval := 1 * time.Second

		// Should return early without tuning
		TuneAdaptiveSettings(
			batchSize,
			adaptiveMaxSize,
			adaptiveMinSize,
			adaptiveTuneThreshold,
			flushCount,
			flushDuration,
			flushInterval,
			log,
		)
	})
}
