package health

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/jaysongiroux/smq/internal/logger"
)

// Handler handles HTTP requests for the health layer
type Handler struct {
	checker *HealthChecker
	log     *logger.Logger
}

// NewHandler creates a new HTTP handler for the health layer
func NewHandler(checker *HealthChecker, log *logger.Logger) *Handler {
	return &Handler{
		checker: checker,
		log:     log,
	}
}

// RegisterRoutes registers all health routes
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/health", h.handleHealth)
}

// handleHealth routes /v1/health requests
func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/health" {
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h.GetHealth(w, r)
}

// GetHealth handles GET /v1/health
// Returns paginated list of all nodes' health from the database
func (h *Handler) GetHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Parse pagination parameters
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")

	limit := 100 // default
	offset := 0  // default

	if limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	if offsetStr != "" {
		if parsedOffset, err := strconv.Atoi(offsetStr); err == nil && parsedOffset >= 0 {
			offset = parsedOffset
		}
	}

	h.log.Debug("Fetching cluster health (limit=%d, offset=%d)", limit, offset)

	// Get all nodes from database
	nodes, err := h.checker.Store().ListNodes(ctx, limit, offset)
	if err != nil {
		h.log.Error("Failed to list nodes: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "failed to get cluster health",
		})
		return
	}

	// Build response with pagination info
	response := map[string]interface{}{
		"nodes": nodes,
		"pagination": map[string]interface{}{
			"limit":  limit,
			"offset": offset,
			"count":  len(nodes),
		},
	}

	// Return appropriate HTTP status based on system health
	statusCode := http.StatusOK

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(response)
}
