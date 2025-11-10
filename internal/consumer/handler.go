package consumer

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/config"
	"github.com/jaysongiroux/smq/internal/logger"
)

// Poll message limits
const (
	DefaultMaxMessages = 1000
	MinMaxMessages     = 1
	MaxMaxMessages     = 500000
)

// Ack/Nack batch limits
const (
	MinBatchSize = 1
	MaxBatchSize = 1000
)

// AckRequest represents the request body for acknowledging messages
type AckRequest struct {
	MessageIDs []string `json:"message_ids"`
}

// NackRequest represents the request body for nacking messages
type NackRequest struct {
	MessageIDs []string `json:"message_ids"`
}

// Handler handles HTTP requests for the consumer layer
type Handler struct {
	consumer *Consumer
	log      *logger.Logger
}

// NewHandler creates a new HTTP handler for the consumer layer
func NewHandler(consumer *Consumer, log *logger.Logger) *Handler {
	return &Handler{
		consumer: consumer,
		log:      log,
	}
}

// RegisterRoutes registers all consumer routes
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/channels", h.handleListChannels)
	mux.HandleFunc("/v1/channels/", h.handleChannelsPoll)
	mux.HandleFunc("/v1/messages/ack", h.handleAck)
	mux.HandleFunc("/v1/messages/nack", h.handleNack)
}

func (h *Handler) handleListChannels(w http.ResponseWriter, r *http.Request) {
	// Only handle exact path match
	if r.URL.Path != "/v1/channels" {
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h.ListChannels(w, r)
}

// handleChannelsPoll routes /v1/channels/{channels}/poll requests
func (h *Handler) handleChannelsPoll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Check if path ends with /poll
	if !strings.HasSuffix(r.URL.Path, "/poll") {
		http.NotFound(w, r)
		return
	}

	h.PollMessages(w, r)
}

// handleAck routes /v1/messages/ack requests
func (h *Handler) handleAck(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/messages/ack" {
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h.AckMessage(w, r)
}

// handleNack routes /v1/messages/nack requests
func (h *Handler) handleNack(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/messages/nack" {
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h.NackMessage(w, r)
}

// ListChannels handles GET /v1/channels?limit=10&offset=0
// Returns a paginated list of all channels
func (h *Handler) ListChannels(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Parse limit and offset from query params
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")

	// Default values
	limit := 100
	offset := 0

	// Parse limit
	if limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err != nil || parsed < 1 {
			h.log.Warn("Invalid limit parameter: %s", limitStr)
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Invalid limit parameter - must be a positive integer",
			})
			return
		}
		limit = parsed
	}

	// Parse offset
	if offsetStr != "" {
		parsed, err := strconv.Atoi(offsetStr)
		if err != nil || parsed < 0 {
			h.log.Warn("Invalid offset parameter: %s", offsetStr)
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Invalid offset parameter - must be a non-negative integer",
			})
			return
		}
		offset = parsed
	}

	// Get channels from database
	ctx := r.Context()
	channels, err := h.consumer.ListChannels(ctx, limit, offset)
	if err != nil {
		h.log.Error("Failed to list channels: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Failed to retrieve channels",
		})
		return
	}

	// Extract channel names for response
	channelNames := make([]string, 0, len(channels))
	for _, ch := range channels {
		channelNames = append(channelNames, ch.Name)
	}

	// Return success response with pagination metadata
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"channels": channelNames,
		"pagination": map[string]int{
			"limit":  limit,
			"offset": offset,
			"count":  len(channelNames),
		},
	})
}

// PollMessages handles GET /v1/channels/:channelId/poll?max=1000&subsidize=false
// Atomically retrieves and locks ready messages for the consumer
func (h *Handler) PollMessages(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Extract channel from path: /v1/channels/{channelId}/poll
	path := r.URL.Path
	path = strings.TrimPrefix(path, "/v1/channels/")
	path = strings.TrimSuffix(path, "/poll")
	channelID := strings.TrimSpace(path)

	// Validate channel
	if channelID == "" {
		h.log.Warn("Poll request missing channel ID")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Channel ID is required",
		})
		return
	}

	// Parse max parameter
	maxStr := r.URL.Query().Get("max")
	max := DefaultMaxMessages
	if maxStr != "" {
		parsed, err := strconv.Atoi(maxStr)
		if err != nil {
			h.log.Warn("Invalid max parameter: %s", maxStr)
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Invalid max parameter - must be an integer",
			})
			return
		}

		// Validate max range
		if err := config.ValidateRange("max", parsed, MinMaxMessages, MaxMaxMessages); err != nil {
			h.log.Warn("Invalid max parameter: %s", maxStr)
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Invalid max parameter - must be between " + strconv.Itoa(MinMaxMessages) + " and " + strconv.Itoa(MaxMaxMessages),
			})
			return
		}

		max = parsed
	}

	// Parse subsidize parameter (for region-based databases)
	subsidizeStr := r.URL.Query().Get("subsidize")
	subsidize := false
	if subsidizeStr != "" {
		parsed, err := strconv.ParseBool(subsidizeStr)
		if err != nil {
			h.log.Warn("Invalid subsidize parameter: %s", subsidizeStr)
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Invalid subsidize parameter - must be true or false",
			})
			return
		}
		subsidize = parsed
	}

	// Poll messages from consumer
	ctx := r.Context()
	messages, err := h.consumer.PollMessages(ctx, channelID, max, subsidize)
	if err != nil {
		// print the messages

		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Failed to poll messages",
		})
		return
	}

	// Return messages
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"messages": func() interface{} {
			if messages == nil {
				return []interface{}{}
			}
			return messages
		}(),
	})
}

// AckMessage handles POST /v1/messages/ack
// Confirms successful message processing and permanently deletes the message
func (h *Handler) AckMessage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Parse request body
	var req AckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.log.Warn("Invalid ack request body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Invalid request body - must be valid JSON",
		})
		return
	}

	// Validate message IDs count
	if len(req.MessageIDs) < MinBatchSize {
		h.log.Warn("Ack request with too few message IDs: %d", len(req.MessageIDs))
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "At least 1 message ID is required",
		})
		return
	}
	if len(req.MessageIDs) > MaxBatchSize {
		h.log.Warn("Ack request with too many message IDs: %d (max: %d)", len(req.MessageIDs), MaxBatchSize)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Cannot ack more than 1000 messages at once",
		})
		return
	}

	// Parse UUIDs
	messageIDs := make([]uuid.UUID, 0, len(req.MessageIDs))
	for _, idStr := range req.MessageIDs {
		id, err := uuid.Parse(idStr)
		if err != nil {
			h.log.Warn("Invalid message ID in ack request: %s", idStr)
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Invalid message ID format: " + idStr,
			})
			return
		}
		messageIDs = append(messageIDs, id)
	}

	// Ack messages
	ctx := r.Context()
	err := h.consumer.AckMessage(ctx, messageIDs)
	if err != nil {
		h.log.Error("Failed to ack messages: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Failed to acknowledge messages",
		})
		return
	}

	// Return success
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"count":   len(messageIDs),
	})
}

// NackMessage handles POST /v1/messages/nack
// Indicates processing failure, marks message for retry or moves to DLQ
func (h *Handler) NackMessage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Parse request body
	var req NackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.log.Warn("Invalid nack request body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Invalid request body - must be valid JSON",
		})
		return
	}

	// Validate message IDs count
	if len(req.MessageIDs) < MinBatchSize {
		h.log.Warn("Nack request with too few message IDs: %d", len(req.MessageIDs))
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "At least 1 message ID is required",
		})
		return
	}
	if len(req.MessageIDs) > MaxBatchSize {
		h.log.Warn("Nack request with too many message IDs: %d (max: %d)", len(req.MessageIDs), MaxBatchSize)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Cannot nack more than 1000 messages at once",
		})
		return
	}

	// Parse UUIDs
	messageIDs := make([]uuid.UUID, 0, len(req.MessageIDs))
	for _, idStr := range req.MessageIDs {
		id, err := uuid.Parse(idStr)
		if err != nil {
			h.log.Warn("Invalid message ID in nack request: %s", idStr)
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Invalid message ID format: " + idStr,
			})
			return
		}
		messageIDs = append(messageIDs, id)
	}

	// Nack messages
	ctx := r.Context()
	err := h.consumer.NackMessage(ctx, messageIDs)
	if err != nil {
		h.log.Error("Failed to nack messages: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Failed to nack messages",
		})
		return
	}

	// Return success
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"count":   len(messageIDs),
	})
}
