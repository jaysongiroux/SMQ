package scheduler

import (
	"math/rand"
	"time"
)

func applyJitter(duration time.Duration, jitterPercent int) time.Duration {
	if jitterPercent <= 0 || jitterPercent > 100 {
		return duration
	}

	jitterAmount := float64(duration) * float64(jitterPercent) / 100.0
	randomJitter := (rand.Float64()*2 - 1) * jitterAmount

	result := duration + time.Duration(randomJitter)

	if result < duration/2 {
		result = duration / 2
	}

	return result
}

func formatError(err error) interface{} {
	if err == nil {
		return nil
	}
	return err.Error()
}
