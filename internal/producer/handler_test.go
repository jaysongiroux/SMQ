package producer

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/testutils"
)

func TestNewHandler(t *testing.T) {
	t.Run("creates handler successfully", func(t *testing.T) {
		store := &testutils.MockStore{}
		buf := &testutils.MockBufferHandler{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		handler := NewHandler(producer, log)

		if handler == nil {
			t.Fatal("Expected handler to be created")
		}

		if handler.producer != producer {
			t.Error("Expected producer to be set")
		}
	})
}

func TestRegisterRoutes(t *testing.T) {
	t.Run("registers routes correctly", func(t *testing.T) {
		store := &testutils.MockStore{}
		buf := &testutils.MockBufferHandler{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		handler := NewHandler(producer, log)

		mux := http.NewServeMux()
		handler.RegisterRoutes(mux)

		// Test that routes are registered by making test requests
		testCases := []struct {
			path           string
			method         string
			expectedStatus int
		}{
			{"/v1/message", "OPTIONS", http.StatusMethodNotAllowed},
			{"/v1/message/123", "GET", http.StatusMethodNotAllowed},
		}

		for _, tc := range testCases {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, req)

			if rr.Code != tc.expectedStatus {
				t.Errorf("Path %s %s: expected status %d, got %d", tc.method, tc.path, tc.expectedStatus, rr.Code)
			}
		}
	})
}

func TestHandlerCreateMessage(t *testing.T) {
	t.Run("successfully creates message", func(t *testing.T) {
		store := &testutils.MockStore{}
		buf := &testutils.MockBufferHandler{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		producer.Start()
		handler := NewHandler(producer, log)

		requestBody := map[string]interface{}{
			"channel":      "test-channel",
			"payload":      map[string]string{"test": "data"},
			"scheduled_at": time.Now().Add(10 * time.Second).Unix(),
		}

		body, _ := json.Marshal(requestBody)
		req := httptest.NewRequest("POST", "/v1/message", bytes.NewBuffer(body))
		rr := httptest.NewRecorder()

		handler.CreateMessage(rr, req)

		if rr.Code != http.StatusCreated {
			t.Errorf("Expected status 201, got %d", rr.Code)
		}

		var response map[string]interface{}
		json.Unmarshal(rr.Body.Bytes(), &response)

		if response["message_id"] == nil {
			t.Error("Expected message_id in response")
		}

		if response["channel"] != "test-channel" {
			t.Errorf("Expected channel 'test-channel', got %v", response["channel"])
		}
	})

	t.Run("fails with invalid JSON", func(t *testing.T) {
		store := &testutils.MockStore{}
		buf := &testutils.MockBufferHandler{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		handler := NewHandler(producer, log)

		req := httptest.NewRequest("POST", "/v1/message", bytes.NewBuffer([]byte("invalid json")))
		rr := httptest.NewRecorder()

		handler.CreateMessage(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", rr.Code)
		}

		var response map[string]string
		json.Unmarshal(rr.Body.Bytes(), &response)

		if response["error"] != "Invalid JSON format" {
			t.Errorf("Expected 'Invalid JSON format' error, got %s", response["error"])
		}
	})

	t.Run("fails with empty channel", func(t *testing.T) {
		store := &testutils.MockStore{}
		buf := &testutils.MockBufferHandler{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		handler := NewHandler(producer, log)

		requestBody := map[string]interface{}{
			"channel":      "",
			"payload":      map[string]string{"test": "data"},
			"scheduled_at": time.Now().Add(10 * time.Second).Unix(),
		}

		body, _ := json.Marshal(requestBody)
		req := httptest.NewRequest("POST", "/v1/message", bytes.NewBuffer(body))
		rr := httptest.NewRecorder()

		handler.CreateMessage(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", rr.Code)
		}

		var response map[string]string
		json.Unmarshal(rr.Body.Bytes(), &response)

		if response["error"] != "channel is required" {
			t.Errorf("Expected 'channel is required' error, got %s", response["error"])
		}
	})

	t.Run("fails when buffer is full", func(t *testing.T) {
		store := &testutils.MockStore{}
		buf := &testutils.MockBufferHandler{
			AddError: errors.New("buffer full"),
		}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		handler := NewHandler(producer, log)

		requestBody := map[string]interface{}{
			"channel":      "test-channel",
			"payload":      map[string]string{"test": "data"},
			"scheduled_at": time.Now().Add(10 * time.Second).Unix(),
		}

		body, _ := json.Marshal(requestBody)
		req := httptest.NewRequest("POST", "/v1/message", bytes.NewBuffer(body))
		rr := httptest.NewRecorder()

		handler.CreateMessage(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", rr.Code)
		}
	})

	t.Run("sets correct content type", func(t *testing.T) {
		store := &testutils.MockStore{}
		buf := &testutils.MockBufferHandler{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		handler := NewHandler(producer, log)

		requestBody := map[string]interface{}{
			"channel":      "test-channel",
			"payload":      map[string]string{"test": "data"},
			"scheduled_at": time.Now().Add(10 * time.Second).Unix(),
		}

		body, _ := json.Marshal(requestBody)
		req := httptest.NewRequest("POST", "/v1/message", bytes.NewBuffer(body))
		rr := httptest.NewRecorder()

		handler.CreateMessage(rr, req)

		contentType := rr.Header().Get("Content-Type")
		if contentType != "application/json" {
			t.Errorf("Expected Content-Type 'application/json', got %s", contentType)
		}
	})
}

func TestHandlerDeleteMessage(t *testing.T) {
	t.Run("successfully deletes message", func(t *testing.T) {
		store := &testutils.MockStore{}
		buf := &testutils.MockBufferHandler{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		producer.Start()
		handler := NewHandler(producer, log)

		messageID := uuid.New()
		req := httptest.NewRequest("DELETE", "/v1/message/"+messageID.String(), nil)
		rr := httptest.NewRecorder()

		handler.DeleteMessage(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", rr.Code)
		}

		var response map[string]interface{}
		json.Unmarshal(rr.Body.Bytes(), &response)

		if response["message_id"] == nil {
			t.Error("Expected message_id in response")
		}

		if response["deleted_at"] == nil {
			t.Error("Expected deleted_at in response")
		}
	})

	t.Run("fails with invalid UUID", func(t *testing.T) {
		store := &testutils.MockStore{}
		buf := &testutils.MockBufferHandler{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		handler := NewHandler(producer, log)

		req := httptest.NewRequest("DELETE", "/v1/message/invalid-uuid", nil)
		rr := httptest.NewRecorder()

		handler.DeleteMessage(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", rr.Code)
		}

		var response map[string]string
		json.Unmarshal(rr.Body.Bytes(), &response)

		if !strings.Contains(response["error"], "Invalid message ID format") {
			t.Errorf("Expected 'Invalid message ID format' error, got %s", response["error"])
		}
	})

	t.Run("returns 404 when message not found", func(t *testing.T) {
		store := &testutils.MockStore{
			DeleteError: errors.New("no rows affected"),
		}
		buf := &testutils.MockBufferHandler{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		handler := NewHandler(producer, log)

		messageID := uuid.New()
		req := httptest.NewRequest("DELETE", "/v1/message/"+messageID.String(), nil)
		rr := httptest.NewRecorder()

		handler.DeleteMessage(rr, req)

		if rr.Code != http.StatusNotFound {
			t.Errorf("Expected status 404, got %d", rr.Code)
		}

		var response map[string]string
		json.Unmarshal(rr.Body.Bytes(), &response)

		if response["error"] != "Message not found" {
			t.Errorf("Expected 'Message not found' error, got %s", response["error"])
		}
	})

	t.Run("returns 500 for other database errors", func(t *testing.T) {
		store := &testutils.MockStore{
			DeleteError: errors.New("database connection lost"),
		}
		buf := &testutils.MockBufferHandler{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		handler := NewHandler(producer, log)

		messageID := uuid.New()
		req := httptest.NewRequest("DELETE", "/v1/message/"+messageID.String(), nil)
		rr := httptest.NewRecorder()

		handler.DeleteMessage(rr, req)

		if rr.Code != http.StatusInternalServerError {
			t.Errorf("Expected status 500, got %d", rr.Code)
		}

		var response map[string]string
		json.Unmarshal(rr.Body.Bytes(), &response)

		if response["error"] != "Failed to delete message" {
			t.Errorf("Expected 'Failed to delete message' error, got %s", response["error"])
		}
	})

	t.Run("sets correct content type", func(t *testing.T) {
		store := &testutils.MockStore{}
		buf := &testutils.MockBufferHandler{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		handler := NewHandler(producer, log)

		messageID := uuid.New()
		req := httptest.NewRequest("DELETE", "/v1/message/"+messageID.String(), nil)
		rr := httptest.NewRecorder()

		handler.DeleteMessage(rr, req)

		contentType := rr.Header().Get("Content-Type")
		if contentType != "application/json" {
			t.Errorf("Expected Content-Type 'application/json', got %s", contentType)
		}
	})
}

func TestHandleMessage(t *testing.T) {
	t.Run("returns 404 for non-exact path", func(t *testing.T) {
		store := &testutils.MockStore{}
		buf := &testutils.MockBufferHandler{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		handler := NewHandler(producer, log)

		req := httptest.NewRequest("POST", "/v1/message/extra", nil)
		rr := httptest.NewRecorder()

		handler.handleMessage(rr, req)

		if rr.Code != http.StatusNotFound {
			t.Errorf("Expected status 404, got %d", rr.Code)
		}
	})

	t.Run("returns 405 for unsupported methods", func(t *testing.T) {
		store := &testutils.MockStore{}
		buf := &testutils.MockBufferHandler{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		handler := NewHandler(producer, log)

		methods := []string{"GET", "PUT", "PATCH", "DELETE", "OPTIONS"}

		for _, method := range methods {
			req := httptest.NewRequest(method, "/v1/message", nil)
			rr := httptest.NewRecorder()

			handler.handleMessage(rr, req)

			if rr.Code != http.StatusMethodNotAllowed {
				t.Errorf("Method %s: expected status 405, got %d", method, rr.Code)
			}
		}
	})
}

func TestHandleMessageWithID(t *testing.T) {
	t.Run("returns 404 for empty ID", func(t *testing.T) {
		store := &testutils.MockStore{}
		buf := &testutils.MockBufferHandler{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		handler := NewHandler(producer, log)

		req := httptest.NewRequest("DELETE", "/v1/message/", nil)
		rr := httptest.NewRecorder()

		handler.handleMessageWithID(rr, req)

		if rr.Code != http.StatusNotFound {
			t.Errorf("Expected status 404, got %d", rr.Code)
		}
	})

	t.Run("returns 405 for unsupported methods", func(t *testing.T) {
		store := &testutils.MockStore{}
		buf := &testutils.MockBufferHandler{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		handler := NewHandler(producer, log)

		messageID := uuid.New()
		methods := []string{"GET", "POST", "PUT", "PATCH", "OPTIONS"}

		for _, method := range methods {
			req := httptest.NewRequest(method, "/v1/message/"+messageID.String(), nil)
			rr := httptest.NewRecorder()

			handler.handleMessageWithID(rr, req)

			if rr.Code != http.StatusMethodNotAllowed {
				t.Errorf("Method %s: expected status 405, got %d", method, rr.Code)
			}
		}
	})
}

func TestHandlerIntegration(t *testing.T) {
	t.Run("full create and delete workflow", func(t *testing.T) {
		store := &testutils.MockStore{}
		buf := &testutils.MockBufferHandler{}
		log := logger.New("test", createTestConfig())

		producer := NewProducer(store, buf, log, 10240, createTestConfig())
		producer.Start()
		handler := NewHandler(producer, log)

		// Create message
		requestBody := map[string]interface{}{
			"channel":      "test-channel",
			"payload":      map[string]string{"test": "data"},
			"scheduled_at": time.Now().Add(10 * time.Second).Unix(),
		}

		body, _ := json.Marshal(requestBody)
		createReq := httptest.NewRequest("POST", "/v1/message", bytes.NewBuffer(body))
		createRR := httptest.NewRecorder()

		handler.CreateMessage(createRR, createReq)

		if createRR.Code != http.StatusCreated {
			t.Fatalf("Create failed with status %d", createRR.Code)
		}

		var createResponse map[string]interface{}
		json.Unmarshal(createRR.Body.Bytes(), &createResponse)

		messageID := createResponse["message_id"].(string)

		// Delete message
		deleteReq := httptest.NewRequest("DELETE", "/v1/message/"+messageID, nil)
		deleteRR := httptest.NewRecorder()

		handler.DeleteMessage(deleteRR, deleteReq)

		if deleteRR.Code != http.StatusOK {
			t.Errorf("Delete failed with status %d", deleteRR.Code)
		}

		var deleteResponse map[string]interface{}
		json.Unmarshal(deleteRR.Body.Bytes(), &deleteResponse)

		if deleteResponse["message_id"] != messageID {
			t.Errorf("Expected message_id %s, got %v", messageID, deleteResponse["message_id"])
		}
	})
}
