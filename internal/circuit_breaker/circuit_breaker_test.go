package circuit_breaker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jaysongiroux/smq/internal/config"
	"github.com/jaysongiroux/smq/internal/logger"
)

func newTestCB() *CircuitBreaker {
	log := logger.New("test", &config.Config{LogLevel: "fatal"})
	return NewCircuitBreaker(Config{
		Name:            "test-cb",
		MaxFailures:     2,
		Timeout:         50 * time.Millisecond, // 50ms timeout
		ResetTimeout:    50 * time.Millisecond, // 50ms reset timeout
		HalfOpenMaxReqs: 1,
		Log:             log,
	})
}

func TestCircuitBreaker_ClosesAfterSuccess(t *testing.T) {
	cb := newTestCB()

	err := cb.Execute(context.Background(), func(ctx context.Context) error {
		return nil
	})

	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}

	if cb.GetState() != StateClosed {
		t.Fatalf("expected closed state")
	}
}

func TestCircuitBreaker_OpensAfterFailures(t *testing.T) {
	cb := newTestCB()

	// Cause 2 failures
	cb.Execute(context.Background(), func(ctx context.Context) error { return errors.New("fail") })
	cb.Execute(context.Background(), func(ctx context.Context) error { return errors.New("fail") })

	if cb.GetState() != StateOpen {
		t.Fatalf("expected open state after failures")
	}
}

func TestCircuitBreaker_BlocksWhenOpen(t *testing.T) {
	cb := newTestCB()

	// open it
	cb.Execute(context.Background(), func(ctx context.Context) error { return errors.New("fail") })
	cb.Execute(context.Background(), func(ctx context.Context) error { return errors.New("fail") })

	err := cb.Execute(context.Background(), func(ctx context.Context) error { return nil })
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}
