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
// QUERY PARAMS:
// - limit: number of nodes to return (default: 100)
// - offset: number of nodes to skip (default: 0)
func (h *Handler) GetHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")

	// default values
	limit := 100
	offset := 0

	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err != nil || parsedLimit < 1 {
			h.log.Warn("Invalid limit parameter: %s", limitStr)
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Invalid limit parameter - must be a positive integer",
			})
			return
		}
		limit = parsedLimit
	}

	if offsetStr != "" {
		parsedOffset, err := strconv.Atoi(offsetStr)
		if err != nil || parsedOffset < 0 {
			h.log.Warn("Invalid offset parameter: %s", offsetStr)
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Invalid offset parameter - must be a non-negative integer",
			})
			return
		}
		offset = parsedOffset
	}

	h.log.Debug("Fetching cluster health (limit=%d, offset=%d)", limit, offset)

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

	response := map[string]any{
		"nodes": nodes,
		"pagination": map[string]any{
			"limit":  limit,
			"offset": offset,
			"count":  len(nodes),
		},
	}

	statusCode := http.StatusOK

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(response)
}
