package scheduler

import (
	"errors"
	"testing"
	"time"
)

func TestApplyJitter(t *testing.T) {
	tests := []struct {
		name          string
		duration      time.Duration
		jitterPercent int
	}{
		{duration: 100 * time.Millisecond, jitterPercent: 10},
		{duration: 100 * time.Millisecond, jitterPercent: 0},
		{duration: 100 * time.Millisecond, jitterPercent: 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := applyJitter(tt.duration, tt.jitterPercent)
			// got should be within the jitter percentage of the want duration +- percentage
			min := tt.duration - time.Duration(tt.jitterPercent)*time.Millisecond
			max := tt.duration + time.Duration(tt.jitterPercent)*time.Millisecond
			if got < min || got > max {
				t.Errorf("applyJitter() = %v, want within %d%% of %v", got, tt.jitterPercent, tt.duration)
			}
		})
	}
}

func TestFormatError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want interface{}
	}{
		{name: "nil error", err: nil, want: nil},
		{name: "non-nil error", err: errors.New("test error"), want: "test error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatError(tt.err)
			if got != tt.want {
				t.Errorf("formatError() = %v, want %v", got, tt.want)
			}
		})
	}
}
