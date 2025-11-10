package scheduler

import "time"

// SchedulerConfig holds configuration for the scheduler
type SchedulerConfig struct {
	// PollInterval is how often the scheduler checks for pending messages
	PollInterval time.Duration

	// PollJitterPercent is the percentage of jitter to apply to poll interval (0-100)
	PollJitterPercent int

	// StaleAcquiredThreshold is how long a message can be acquired before being marked stale
	// This handles cases where a consumer dies without sending ack/nack
	StaleAcquiredThreshold time.Duration

	// JanitorInterval is how often the janitor runs to clean up stale acquired messages and nodes
	JanitorInterval time.Duration

	// JanitorJitterPercent is the percentage of jitter to apply to janitor interval (0-100)
	JanitorJitterPercent int

	// StaleNodeThreshold is how long a node can be offline before being removed
	StaleNodeThreshold time.Duration
}

