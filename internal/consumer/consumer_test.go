package consumer

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

func TestNewConsumer(t *testing.T) {
	store := &testutils.MockStore{}
	log := testutils.CreateTestLogger()
	nodeID := "test-node-1"

	consumer := NewConsumer(store, nodeID, log)

	if consumer == nil {
		t.Fatal("Expected consumer to be created, got nil")
	}

	if consumer.store != store {
		t.Error("Expected store to be set")
	}

	if consumer.nodeID != nodeID {
		t.Errorf("Expected nodeID to be %s, got %s", nodeID, consumer.nodeID)
	}

	if consumer.log != log {
		t.Error("Expected logger to be set")
	}

	if consumer.isRunning {
		t.Error("Expected consumer to not be running initially")
	}
}

func TestConsumerStartStop(t *testing.T) {
	store := &testutils.MockStore{}
	log := testutils.CreateTestLogger()
	consumer := NewConsumer(store, "test-node", log)

	// Test Start
	err := consumer.Start()
	if err != nil {
		t.Fatalf("Expected no error starting consumer, got: %v", err)
	}

	if !consumer.isRunning {
		t.Error("Expected consumer to be running after Start()")
	}

	if consumer.lastActive.IsZero() {
		t.Error("Expected lastActive to be set after Start()")
	}

	// Test Stop
	err = consumer.Stop()
	if err != nil {
		t.Fatalf("Expected no error stopping consumer, got: %v", err)
	}

	if consumer.isRunning {
		t.Error("Expected consumer to not be running after Stop()")
	}
}

func TestConsumerHealth(t *testing.T) {
	store := &testutils.MockStore{}
	log := testutils.CreateTestLogger()
	nodeID := "test-node-123"
	consumer := NewConsumer(store, nodeID, log)

	t.Run("unhealthy when not running", func(t *testing.T) {
		health := consumer.Health()

		if health.Name != "consumer" {
			t.Errorf("Expected name to be 'consumer', got %s", health.Name)
		}

		if health.Status != models.HealthStatusUnhealthy {
			t.Errorf("Expected status to be unhealthy, got %s", health.Status)
		}

		if health.Message != "Consumer is down" {
			t.Errorf("Expected down message, got %s", health.Message)
		}

		metadata := health.Metadata
		if metadata["is_running"].(bool) {
			t.Error("Expected is_running to be false in metadata")
		}

		if metadata["node_id"].(string) != nodeID {
			t.Errorf("Expected node_id to be %s, got %s", nodeID, metadata["node_id"])
		}
	})

	t.Run("healthy when running", func(t *testing.T) {
		err := consumer.Start()
		if err != nil {
			t.Fatalf("Failed to start consumer: %v", err)
		}

		health := consumer.Health()

		if health.Status != models.HealthStatusHealthy {
			t.Errorf("Expected status to be healthy, got %s", health.Status)
		}

		if health.Message != "Consumer is ready to accept requests" {
			t.Errorf("Expected ready message, got %s", health.Message)
		}

		metadata := health.Metadata
		if !metadata["is_running"].(bool) {
			t.Error("Expected is_running to be true in metadata")
		}
	})
}

func TestConsumerPollMessages(t *testing.T) {
	t.Run("successfully polls messages", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)

		err := consumer.Start()
		if err != nil {
			t.Fatalf("Failed to start consumer: %v", err)
		}

		channelID := "test-channel"
		maxMessages := 10

		ctx := context.Background()
		messages, err := consumer.PollMessages(ctx, channelID, maxMessages, false)

		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		// MockStore returns nil by default (no error)
		if messages == nil {
			t.Error("Expected non-nil messages slice")
		}

		// Verify the method was called
		if store.AcquireNextMessageCalls != 1 {
			t.Errorf(
				"Expected AcquireNextMessage to be called once, got %d calls",
				store.AcquireNextMessageCalls,
			)
		}

		// Verify lastActive was updated
		if consumer.lastActive.IsZero() {
			t.Error("Expected lastActive to be updated")
		}
	})

	t.Run("handles store error", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)

		expectedErr := errors.New("database error")
		store.AcquireNextMessageError = expectedErr

		ctx := context.Background()
		messages, err := consumer.PollMessages(ctx, "test-channel", 10, false)

		if err == nil {
			t.Fatal("Expected error, got nil")
		}

		if !errors.Is(err, expectedErr) {
			t.Errorf("Expected error %v, got %v", expectedErr, err)
		}

		if messages != nil {
			t.Errorf("Expected nil messages, got %v", messages)
		}
	})

	t.Run("updates lastActive timestamp", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)

		consumer.Start()
		time.Sleep(10 * time.Millisecond)

		beforePoll := time.Now()

		ctx := context.Background()
		_, _ = consumer.PollMessages(ctx, "test-channel", 10, false)

		if consumer.lastActive.Before(beforePoll) {
			t.Error("Expected lastActive to be updated after PollMessages")
		}
	})
}

func TestConsumerAckMessage(t *testing.T) {
	t.Run("successfully acknowledges messages", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)

		err := consumer.Start()
		if err != nil {
			t.Fatalf("Failed to start consumer: %v", err)
		}

		messageIDs := []uuid.UUID{uuid.New(), uuid.New()}

		ctx := context.Background()
		err = consumer.AckMessage(ctx, messageIDs)

		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if store.AckMessageCalls != 1 {
			t.Errorf("Expected AckMessage to be called once, got %d calls", store.AckMessageCalls)
		}
	})

	t.Run("handles empty message ID list", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)

		ctx := context.Background()
		err := consumer.AckMessage(ctx, []uuid.UUID{})

		if err != nil {
			t.Errorf("Expected no error for empty list, got: %v", err)
		}
	})

	t.Run("handles store error", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)

		expectedErr := errors.New("ack failed")
		store.AckMessageError = expectedErr

		messageIDs := []uuid.UUID{uuid.New()}
		ctx := context.Background()
		err := consumer.AckMessage(ctx, messageIDs)

		if err == nil {
			t.Fatal("Expected error, got nil")
		}

		if !errors.Is(err, expectedErr) {
			t.Errorf("Expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("updates lastActive timestamp", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)

		consumer.Start()
		time.Sleep(10 * time.Millisecond)

		beforeAck := time.Now()

		messageIDs := []uuid.UUID{uuid.New()}
		ctx := context.Background()
		_ = consumer.AckMessage(ctx, messageIDs)

		if consumer.lastActive.Before(beforeAck) {
			t.Error("Expected lastActive to be updated after AckMessage")
		}
	})
}

func TestConsumerNackMessage(t *testing.T) {
	t.Run("successfully nacks messages", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)

		err := consumer.Start()
		if err != nil {
			t.Fatalf("Failed to start consumer: %v", err)
		}

		messageIDs := []uuid.UUID{uuid.New(), uuid.New()}

		ctx := context.Background()
		err = consumer.NackMessage(ctx, messageIDs)

		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if store.NackMessageCalls != 1 {
			t.Errorf("Expected NackMessage to be called once, got %d calls", store.NackMessageCalls)
		}
	})

	t.Run("handles empty message ID list", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)

		ctx := context.Background()
		err := consumer.NackMessage(ctx, []uuid.UUID{})

		if err != nil {
			t.Errorf("Expected no error for empty list, got: %v", err)
		}
	})

	t.Run("handles store error", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)

		expectedErr := errors.New("nack failed")
		store.NackMessageError = expectedErr

		messageIDs := []uuid.UUID{uuid.New()}
		ctx := context.Background()
		err := consumer.NackMessage(ctx, messageIDs)

		if err == nil {
			t.Fatal("Expected error, got nil")
		}

		if !errors.Is(err, expectedErr) {
			t.Errorf("Expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("updates lastActive timestamp", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)

		consumer.Start()
		time.Sleep(10 * time.Millisecond)

		beforeNack := time.Now()

		messageIDs := []uuid.UUID{uuid.New()}
		ctx := context.Background()
		_ = consumer.NackMessage(ctx, messageIDs)

		if consumer.lastActive.Before(beforeNack) {
			t.Error("Expected lastActive to be updated after NackMessage")
		}
	})
}

func TestConsumerListChannels(t *testing.T) {
	t.Run("successfully lists channels", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)

		err := consumer.Start()
		if err != nil {
			t.Fatalf("Failed to start consumer: %v", err)
		}

		limit := 10
		offset := 0

		ctx := context.Background()
		channels, err := consumer.ListChannels(ctx, limit, offset)

		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if channels == nil {
			t.Error("Expected non-nil channels slice")
		}

		if store.ListChannelsCalls != 1 {
			t.Errorf(
				"Expected ListChannels to be called once, got %d calls",
				store.ListChannelsCalls,
			)
		}
	})

	t.Run("handles store error", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)

		expectedErr := errors.New("list channels failed")
		store.ListChannelsError = expectedErr

		ctx := context.Background()
		channels, err := consumer.ListChannels(ctx, 10, 0)

		if err == nil {
			t.Fatal("Expected error, got nil")
		}

		if !errors.Is(err, expectedErr) {
			t.Errorf("Expected error %v, got %v", expectedErr, err)
		}

		if channels != nil {
			t.Errorf("Expected nil channels, got %v", channels)
		}
	})

	t.Run("handles pagination", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)

		limit := 5
		offset := 10

		ctx := context.Background()
		_, err := consumer.ListChannels(ctx, limit, offset)

		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if store.ListChannelsCalls != 1 {
			t.Errorf(
				"Expected ListChannels to be called once, got %d calls",
				store.ListChannelsCalls,
			)
		}
	})

	t.Run("updates lastActive timestamp", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)

		consumer.Start()
		time.Sleep(10 * time.Millisecond)

		beforeList := time.Now()

		ctx := context.Background()
		_, err := consumer.ListChannels(ctx, 10, 0)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if consumer.lastActive.Before(beforeList) {
			t.Error("Expected lastActive to be updated after ListChannels")
		}
	})
}

func TestConsumerConcurrency(t *testing.T) {
	t.Run("handles concurrent operations safely", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)

		consumer.Start()

		ctx := context.Background()
		var wg sync.WaitGroup

		// Run multiple operations concurrently
		for i := 0; i < 10; i++ {
			wg.Add(3)

			go func() {
				defer wg.Done()
				consumer.PollMessages(ctx, "test", 10, false)
			}()

			go func() {
				defer wg.Done()
				consumer.ListChannels(ctx, 10, 0)
			}()

			go func() {
				defer wg.Done()
				consumer.Health()
			}()
		}

		wg.Wait()

		// If we get here without panic or deadlock, concurrency is handled correctly
	})
}
