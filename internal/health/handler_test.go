package health

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jaysongiroux/smq/internal/config"
	"github.com/jaysongiroux/smq/internal/models"
	"github.com/jaysongiroux/smq/internal/testutils"
)

func TestNewHandler(t *testing.T) {
	store := &testutils.MockStore{}
	log := testutils.CreateTestLogger()
	cfg := &config.Config{Region: "us-east-1"}
	checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)

	handler := NewHandler(checker, log)

	if handler == nil {
		t.Fatal("Expected handler to be created, got nil")
	}

	if handler.checker != checker {
		t.Error("Expected checker to be set")
	}

	if handler.log != log {
		t.Error("Expected logger to be set")
	}
}

func TestHandlerRegisterRoutes(t *testing.T) {
	store := &testutils.MockStore{}
	log := testutils.CreateTestLogger()
	cfg := &config.Config{Region: "us-east-1"}
	checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
	handler := NewHandler(checker, log)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	// Test that the route is registered
	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	// Should not return 404 if route is registered
	if w.Code == http.StatusNotFound {
		t.Error("Route /v1/health not registered")
	}
}

func TestHandleHealth(t *testing.T) {
	t.Run("accepts GET requests", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
		handler := NewHandler(checker, log)

		store.ListNodesResult = []*models.Node{}

		req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
		w := httptest.NewRecorder()

		handler.handleHealth(w, req)

		if w.Code == http.StatusMethodNotAllowed {
			t.Error("Expected GET to be allowed")
		}
	})

	t.Run("rejects non-GET methods", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
		handler := NewHandler(checker, log)

		methods := []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch}

		for _, method := range methods {
			req := httptest.NewRequest(method, "/v1/health", nil)
			w := httptest.NewRecorder()

			handler.handleHealth(w, req)

			if w.Code != http.StatusMethodNotAllowed {
				t.Errorf("Expected status %d for method %s, got %d", http.StatusMethodNotAllowed, method, w.Code)
			}
		}
	})

	t.Run("returns 404 for incorrect path", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
		handler := NewHandler(checker, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/health/extra", nil)
		w := httptest.NewRecorder()

		handler.handleHealth(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
		}
	})
}

func TestGetHealth(t *testing.T) {
	t.Run("successfully returns health with defaults", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
		handler := NewHandler(checker, log)

		now := time.Now()
		store.ListNodesResult = []*models.Node{
			{
				NodeID:       "node-1",
				Status:       "healthy",
				LastSeen:     now,
				RegisteredAt: now,
				Metadata: map[string]interface{}{
					"region": "us-east-1",
				},
			},
			{
				NodeID:       "node-2",
				Status:       "healthy",
				LastSeen:     now,
				RegisteredAt: now,
				Metadata: map[string]interface{}{
					"region": "us-east-1",
				},
			},
		}

		req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
		w := httptest.NewRecorder()

		handler.GetHealth(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]interface{}
		err := json.NewDecoder(w.Body).Decode(&response)
		if err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		nodes, ok := response["nodes"].([]interface{})
		if !ok {
			t.Fatal("Expected nodes array in response")
		}

		if len(nodes) != 2 {
			t.Errorf("Expected 2 nodes, got %d", len(nodes))
		}

		pagination, ok := response["pagination"].(map[string]interface{})
		if !ok {
			t.Fatal("Expected pagination object in response")
		}

		if pagination["limit"].(float64) != 100 {
			t.Errorf("Expected default limit 100, got %v", pagination["limit"])
		}

		if pagination["offset"].(float64) != 0 {
			t.Errorf("Expected default offset 0, got %v", pagination["offset"])
		}

		if pagination["count"].(float64) != 2 {
			t.Errorf("Expected count 2, got %v", pagination["count"])
		}

		contentType := w.Header().Get("Content-Type")
		if contentType != "application/json" {
			t.Errorf("Expected Content-Type application/json, got %s", contentType)
		}
	})

	t.Run("respects custom limit and offset", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
		handler := NewHandler(checker, log)

		store.ListNodesResult = []*models.Node{}

		req := httptest.NewRequest(http.MethodGet, "/v1/health?limit=50&offset=10", nil)
		w := httptest.NewRecorder()

		handler.GetHealth(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]interface{}
		json.NewDecoder(w.Body).Decode(&response)

		pagination := response["pagination"].(map[string]interface{})
		if pagination["limit"].(float64) != 50 {
			t.Errorf("Expected limit 50, got %v", pagination["limit"])
		}

		if pagination["offset"].(float64) != 10 {
			t.Errorf("Expected offset 10, got %v", pagination["offset"])
		}

		if store.ListNodesCalls != 1 {
			t.Errorf("Expected ListNodes to be called once, got %d calls", store.ListNodesCalls)
		}
	})

	t.Run("rejects invalid limit", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
		handler := NewHandler(checker, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/health?limit=invalid", nil)
		w := httptest.NewRecorder()

		handler.GetHealth(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}

		var response map[string]string
		json.NewDecoder(w.Body).Decode(&response)

		if response["error"] == "" {
			t.Error("Expected error message in response")
		}
	})

	t.Run("rejects negative limit", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
		handler := NewHandler(checker, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/health?limit=-1", nil)
		w := httptest.NewRecorder()

		handler.GetHealth(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("rejects zero limit", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
		handler := NewHandler(checker, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/health?limit=0", nil)
		w := httptest.NewRecorder()

		handler.GetHealth(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("rejects invalid offset", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
		handler := NewHandler(checker, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/health?offset=invalid", nil)
		w := httptest.NewRecorder()

		handler.GetHealth(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("rejects negative offset", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
		handler := NewHandler(checker, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/health?offset=-1", nil)
		w := httptest.NewRecorder()

		handler.GetHealth(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("accepts offset of 0", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
		handler := NewHandler(checker, log)

		store.ListNodesResult = []*models.Node{}

		req := httptest.NewRequest(http.MethodGet, "/v1/health?offset=0", nil)
		w := httptest.NewRecorder()

		handler.GetHealth(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}
	})

	t.Run("handles store error", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
		handler := NewHandler(checker, log)

		store.ListNodesError = errors.New("database error")

		req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
		w := httptest.NewRecorder()

		handler.GetHealth(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("Expected status %d, got %d", http.StatusInternalServerError, w.Code)
		}

		var response map[string]string
		json.NewDecoder(w.Body).Decode(&response)

		if response["error"] == "" {
			t.Error("Expected error message in response")
		}
	})

	t.Run("returns empty array when no nodes", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
		handler := NewHandler(checker, log)

		store.ListNodesResult = []*models.Node{}

		req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
		w := httptest.NewRecorder()

		handler.GetHealth(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]interface{}
		json.NewDecoder(w.Body).Decode(&response)

		nodes, ok := response["nodes"].([]interface{})
		if !ok {
			t.Fatal("Expected nodes array in response")
		}

		if len(nodes) != 0 {
			t.Errorf("Expected empty nodes array, got %d nodes", len(nodes))
		}
	})

	t.Run("returns nodes with metadata", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
		handler := NewHandler(checker, log)

		now := time.Now()
		store.ListNodesResult = []*models.Node{
			{
				NodeID:       "node-1",
				Status:       "healthy",
				LastSeen:     now,
				RegisteredAt: now,
				Metadata: map[string]interface{}{
					"region":  "us-east-1",
					"version": "1.0.0",
					"layers": map[string]interface{}{
						"producer": map[string]interface{}{
							"status": "healthy",
						},
					},
				},
			},
		}

		req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
		w := httptest.NewRecorder()

		handler.GetHealth(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]interface{}
		json.NewDecoder(w.Body).Decode(&response)

		nodes := response["nodes"].([]interface{})
		node := nodes[0].(map[string]interface{})

		if node["node_id"].(string) != "node-1" {
			t.Errorf("Expected node_id to be node-1, got %v", node["node_id"])
		}

		if node["status"].(string) != "healthy" {
			t.Errorf("Expected status to be healthy, got %v", node["status"])
		}

		metadata, ok := node["metadata"].(map[string]interface{})
		if !ok {
			t.Fatal("Expected metadata object in node")
		}

		if metadata["region"].(string) != "us-east-1" {
			t.Errorf("Expected region to be us-east-1, got %v", metadata["region"])
		}
	})

	t.Run("handles pagination with large offset", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
		handler := NewHandler(checker, log)

		// Empty result when offset is beyond available data
		store.ListNodesResult = []*models.Node{}

		req := httptest.NewRequest(http.MethodGet, "/v1/health?limit=10&offset=1000", nil)
		w := httptest.NewRecorder()

		handler.GetHealth(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]interface{}
		json.NewDecoder(w.Body).Decode(&response)

		nodes := response["nodes"].([]interface{})
		if len(nodes) != 0 {
			t.Errorf("Expected 0 nodes with large offset, got %d", len(nodes))
		}

		pagination := response["pagination"].(map[string]interface{})
		if pagination["count"].(float64) != 0 {
			t.Errorf("Expected count 0, got %v", pagination["count"])
		}
	})
}

func TestHandlerIntegration(t *testing.T) {
	t.Run("full workflow: checker updates, handler retrieves", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 50*time.Millisecond, log)
		handler := NewHandler(checker, log)

		// Register a component
		reporter := &testutils.MockHealthReporter{
			SystemHealth: &models.ComponentHealth{
				Name:      "test-component",
				Status:    models.HealthStatusHealthy,
				Message:   "All good",
				CheckedAt: time.Now(),
			},
		}

		checker.RegisterReporter(reporter)

		// Start health checker
		err := checker.Start()
		if err != nil {
			t.Fatalf("Failed to start checker: %v", err)
		}

		// Wait for at least one health check
		time.Sleep(100 * time.Millisecond)

		// Set up nodes result for handler
		now := time.Now()
		store.ListNodesResult = []*models.Node{
			{
				NodeID:       "test-node",
				Status:       "healthy",
				LastSeen:     now,
				RegisteredAt: now,
				Metadata: map[string]interface{}{
					"status": "healthy",
					"region": "us-east-1",
				},
			},
		}

		// Query health endpoint
		req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
		w := httptest.NewRecorder()

		handler.GetHealth(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]interface{}
		json.NewDecoder(w.Body).Decode(&response)

		nodes := response["nodes"].([]interface{})
		if len(nodes) != 1 {
			t.Errorf("Expected 1 node, got %d", len(nodes))
		}

		// Stop checker
		err = checker.Stop()
		if err != nil {
			t.Fatalf("Failed to stop checker: %v", err)
		}

		// Verify RegisterNode was called by the checker
		if store.RegisterNodeCalls == 0 {
			t.Error("Expected RegisterNode to be called by health checker")
		}
	})

	t.Run("handles errors gracefully", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
		handler := NewHandler(checker, log)

		// Set up various error conditions
		store.ListNodesError = errors.New("database connection failed")

		req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
		w := httptest.NewRecorder()

		handler.GetHealth(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("Expected status %d, got %d", http.StatusInternalServerError, w.Code)
		}

		var response map[string]string
		json.NewDecoder(w.Body).Decode(&response)

		if response["error"] != "failed to get cluster health" {
			t.Errorf("Expected specific error message, got %s", response["error"])
		}
	})
}

func TestHandlerContentType(t *testing.T) {
	store := &testutils.MockStore{}
	log := testutils.CreateTestLogger()
	cfg := &config.Config{Region: "us-east-1"}
	checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)
	handler := NewHandler(checker, log)

	store.ListNodesResult = []*models.Node{}

	t.Run("sets correct content type on success", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
		w := httptest.NewRecorder()

		handler.GetHealth(w, req)

		contentType := w.Header().Get("Content-Type")
		if contentType != "application/json" {
			t.Errorf("Expected Content-Type application/json, got %s", contentType)
		}
	})

	t.Run("sets correct content type on error", func(t *testing.T) {
		store.ListNodesError = errors.New("database error")

		req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
		w := httptest.NewRecorder()

		handler.GetHealth(w, req)

		contentType := w.Header().Get("Content-Type")
		if contentType != "application/json" {
			t.Errorf("Expected Content-Type application/json, got %s", contentType)
		}
	})
}
