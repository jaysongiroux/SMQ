package producer

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/models"
)

// Handler handles HTTP requests for the producer layer
type Handler struct {
	producer *Producer
	log      *logger.Logger
}

// NewHandler creates a new HTTP handler for the producer layer
func NewHandler(producer *Producer, log *logger.Logger) *Handler {
	return &Handler{
		producer: producer,
		log:      log,
	}
}

// RegisterRoutes registers all producer routes
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/message", h.handleMessage)
	mux.HandleFunc("/v1/message/", h.handleMessageWithID)
}

// handleMessage routes requests to /v1/message (no ID in path)
func (h *Handler) handleMessage(w http.ResponseWriter, r *http.Request) {
	// Only handle exact path match
	if r.URL.Path != "/v1/message" {
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case http.MethodPost:
		h.CreateMessage(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleMessageWithID routes requests to /v1/message/{id}
func (h *Handler) handleMessageWithID(w http.ResponseWriter, r *http.Request) {
	// Extract ID from path
	if r.URL.Path == "/v1/message/" {
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case http.MethodDelete:
		h.DeleteMessage(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// CreateMessage handles POST /v1/message
// Creates a new scheduled message
func (h *Handler) CreateMessage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Read and limit request body
	maxBodySize := int64(11 * 1024 * 1024) // 11 MB to account for JSON overhead
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.log.Warn("Failed to read request body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Failed to read request body",
		})
		return
	}

	// Parse request
	var req models.CreateMessageRequest
	if err := json.Unmarshal(body, &req); err != nil {
		h.log.Warn("Failed to parse request body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Invalid JSON format",
		})
		return
	}

	// Create message
	ctx := r.Context()
	messageID, err := h.producer.CreateMessage(ctx, &req)
	if err != nil {
		// Error logging is handled in producer.CreateMessage
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": err.Error(),
		})
		return
	}

	// Return success response with message ID
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message_id":   messageID,
		"channel":      req.Channel,
		"scheduled_at": req.ScheduledAt.Unix(),
		"created_at":   time.Now().Unix(),
	})
}

// DeleteMessage handles DELETE /v1/message/:id
// Deletes a message by ID
func (h *Handler) DeleteMessage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Extract ID from path (everything after /v1/message/)
	idStr := strings.TrimPrefix(r.URL.Path, "/v1/message/")

	// Validate UUID format
	messageID, err := uuid.Parse(idStr)
	if err != nil {
		h.log.Warn("Invalid UUID format: %s", idStr)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Invalid message ID format - must be a valid UUID",
		})
		return
	}

	// Delete message
	ctx := r.Context()
	if err := h.producer.DeleteMessage(ctx, messageID); err != nil {
		// Check if it's a "not found" error
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "no rows") {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Message not found",
			})
			return
		}

		// Other errors
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Failed to delete message",
		})
		return
	}

	// Return success response
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message_id": messageID,
		"deleted_at": time.Now().Unix(),
	})
}
