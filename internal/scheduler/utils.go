package scheduler

import (
	"math/rand"
	"time"
)

// applyJitter applies a random jitter to the given duration
// jitterPercent is the percentage of variation (0-100)
// For example, with 1000ms and 10% jitter, the result will be between 900ms and 1100ms
func applyJitter(duration time.Duration, jitterPercent int) time.Duration {
	if jitterPercent <= 0 || jitterPercent > 100 {
		return duration
	}

	// Calculate jitter range
	jitterAmount := float64(duration) * float64(jitterPercent) / 100.0

	// Generate random jitter between -jitterAmount and +jitterAmount
	randomJitter := (rand.Float64()*2 - 1) * jitterAmount

	// Apply jitter
	result := duration + time.Duration(randomJitter)

	// Ensure result is always positive and reasonable
	if result < duration/2 {
		result = duration / 2
	}

	return result
}

// formatError safely formats an error for JSON
func formatError(err error) interface{} {
	if err == nil {
		return nil
	}
	return err.Error()
}

