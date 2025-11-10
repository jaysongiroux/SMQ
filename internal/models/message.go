package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// MessageStatus represents the current state of a message in the queue
type MessageStatus string

const (
	// StatusPending indicates the message is scheduled but not yet ready
	StatusPending MessageStatus = "PENDING"
	// StatusReady indicates the message is ready to be consumed
	StatusReady MessageStatus = "READY"
	// StatusAcquired indicates a consumer has acquired the message
	StatusAcquired MessageStatus = "ACQUIRED"
	// StatusFailed indicates the message has exceeded retry limits
	StatusFailed MessageStatus = "FAILED"
)

// Message represents a scheduled message in the queue
type Message struct {
	ID          uuid.UUID       `json:"id"`
	Channel     string          `json:"channel"`
	Payload     json.RawMessage `json:"payload"` // JSON object, max 10kb
	ScheduledAt time.Time       `json:"scheduled_at"`
	Status      MessageStatus   `json:"status"`
	AcquiredAt  *time.Time      `json:"acquired_at,omitempty"`
	RetryCount  int             `json:"retry_count"`
	CreatedAt   time.Time       `json:"created_at"`
	Region      *string         `json:"region,omitempty"` // For multi-region DBs like CRDB
}

// Channel represents a message channel configuration
type Channel struct {
	Name              string    `json:"name"`
	MaxRetries        int       `json:"max_retries"`
	DeadLetterChannel string    `json:"dead_letter_channel"`
	CreatedAt         time.Time `json:"created_at"`
}

// CreateMessageRequest represents the request to create a new message
type CreateMessageRequest struct {
	Channel     string          `json:"channel"`
	Payload     json.RawMessage `json:"payload"`
	ScheduledAt UnixTime        `json:"scheduled_at"`
}

// UnixTime is a custom type that can unmarshal from Unix timestamp (int) or RFC3339 string
type UnixTime struct {
	time.Time
}

// UnmarshalJSON implements custom JSON unmarshaling for Unix timestamps
func (ut *UnixTime) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as integer (Unix timestamp)
	var timestamp int64
	if err := json.Unmarshal(data, &timestamp); err == nil {
		ut.Time = time.Unix(timestamp, 0)
		return nil
	}

	// Try to unmarshal as RFC3339 string
	var timeStr string
	if err := json.Unmarshal(data, &timeStr); err == nil {
		parsed, err := time.Parse(time.RFC3339, timeStr)
		if err != nil {
			return err
		}
		ut.Time = parsed
		return nil
	}

	return json.Unmarshal(data, &ut.Time)
}

// MarshalJSON implements custom JSON marshaling for Unix timestamps
func (ut UnixTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(ut.Unix())
}

// AckRequest represents a message acknowledgment request
type AckRequest struct {
	MessageID uuid.UUID `json:"message_id"`
}

// NackRequest represents a message negative acknowledgment request
type NackRequest struct {
	MessageID uuid.UUID `json:"message_id"`
}

// PollRequest represents parameters for polling messages
type PollRequest struct {
	Channels []string `json:"channels"`
	Max      int      `json:"max"`
}
