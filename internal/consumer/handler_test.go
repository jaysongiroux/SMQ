package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/jaysongiroux/smq/internal/models"
	"github.com/jaysongiroux/smq/internal/testutils"
)

func TestNewHandler(t *testing.T) {
	store := &testutils.MockStore{}
	log := testutils.CreateTestLogger()
	consumer := NewConsumer(store, "test-node", log)

	handler := NewHandler(consumer, log)

	if handler == nil {
		t.Fatal("Expected handler to be created, got nil")
	}

	if handler.consumer != consumer {
		t.Error("Expected consumer to be set")
	}

	if handler.log != log {
		t.Error("Expected logger to be set")
	}
}

func TestHandlerRegisterRoutes(t *testing.T) {
	store := &testutils.MockStore{}
	log := testutils.CreateTestLogger()
	consumer := NewConsumer(store, "test-node", log)
	handler := NewHandler(consumer, log)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	// Test that routes are registered by making test requests
	testCases := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v1/channels"},
		{http.MethodGet, "/v1/channels/test-channel/poll"},
		{http.MethodPost, "/v1/messages/ack"},
		{http.MethodPost, "/v1/messages/nack"},
	}

	for _, tc := range testCases {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		w := httptest.NewRecorder()

		mux.ServeHTTP(w, req)

		// Should not return 404 if route is registered
		if w.Code == http.StatusNotFound {
			t.Errorf("Route %s %s not registered", tc.method, tc.path)
		}
	}
}

func TestHandleListChannels(t *testing.T) {
	t.Run("successfully lists channels with defaults", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		store.ListChannelsResult = []*models.Channel{
			{Name: "channel1"},
			{Name: "channel2"},
			{Name: "channel3"},
		}

		req := httptest.NewRequest(http.MethodGet, "/v1/channels", nil)
		w := httptest.NewRecorder()

		handler.handleListChannels(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]interface{}
		err := json.NewDecoder(w.Body).Decode(&response)
		if err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		channels, ok := response["channels"].([]interface{})
		if !ok {
			t.Fatal("Expected channels array in response")
		}

		if len(channels) != 3 {
			t.Errorf("Expected 3 channels, got %d", len(channels))
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
	})

	t.Run("respects custom limit and offset", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		store.ListChannelsResult = []*models.Channel{}

		req := httptest.NewRequest(http.MethodGet, "/v1/channels?limit=50&offset=10", nil)
		w := httptest.NewRecorder()

		handler.handleListChannels(w, req)

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
	})

	t.Run("rejects invalid limit", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/channels?limit=invalid", nil)
		w := httptest.NewRecorder()

		handler.handleListChannels(w, req)

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
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/channels?limit=-1", nil)
		w := httptest.NewRecorder()

		handler.handleListChannels(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("rejects invalid offset", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/channels?offset=invalid", nil)
		w := httptest.NewRecorder()

		handler.handleListChannels(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("rejects negative offset", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/channels?offset=-1", nil)
		w := httptest.NewRecorder()

		handler.handleListChannels(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("handles store error", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		store.ListChannelsError = errors.New("database error")

		req := httptest.NewRequest(http.MethodGet, "/v1/channels", nil)
		w := httptest.NewRecorder()

		handler.handleListChannels(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("Expected status %d, got %d", http.StatusInternalServerError, w.Code)
		}
	})

	t.Run("rejects non-GET methods", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodPost, "/v1/channels", nil)
		w := httptest.NewRecorder()

		handler.handleListChannels(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("Expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
		}
	})

	t.Run("returns 404 for incorrect path", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/channels/extra", nil)
		w := httptest.NewRecorder()

		handler.handleListChannels(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
		}
	})
}

func TestHandlePollMessages(t *testing.T) {
	t.Run("successfully polls messages with defaults", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		store.AcquireNextMessageResult = []*models.Message{
			{ID: uuid.New(), Channel: "test-channel", Payload: []byte("msg1")},
			{ID: uuid.New(), Channel: "test-channel", Payload: []byte("msg2")},
		}

		req := httptest.NewRequest(http.MethodGet, "/v1/channels/test-channel/poll", nil)
		w := httptest.NewRecorder()

		handler.handleChannelsPoll(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]interface{}
		err := json.NewDecoder(w.Body).Decode(&response)
		if err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		messages, ok := response["messages"].([]interface{})
		if !ok {
			t.Fatal("Expected messages array in response")
		}

		if len(messages) != 2 {
			t.Errorf("Expected 2 messages, got %d", len(messages))
		}
	})

	t.Run("respects max parameter", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		store.AcquireNextMessageResult = []*models.Message{}

		req := httptest.NewRequest(http.MethodGet, "/v1/channels/test-channel/poll?max=50", nil)
		w := httptest.NewRecorder()

		handler.handleChannelsPoll(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}
	})

	t.Run("rejects invalid max parameter", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/channels/test-channel/poll?max=invalid", nil)
		w := httptest.NewRecorder()

		handler.handleChannelsPoll(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("rejects max below minimum", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/channels/test-channel/poll?max=0", nil)
		w := httptest.NewRecorder()

		handler.handleChannelsPoll(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("rejects max above maximum", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/channels/test-channel/poll?max=600000", nil)
		w := httptest.NewRecorder()

		handler.handleChannelsPoll(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("respects subsidize parameter", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		store.AcquireNextMessageResult = []*models.Message{}

		req := httptest.NewRequest(http.MethodGet, "/v1/channels/test-channel/poll?subsidize=true", nil)
		w := httptest.NewRecorder()

		handler.handleChannelsPoll(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}
	})

	t.Run("rejects invalid subsidize parameter", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/channels/test-channel/poll?subsidize=invalid", nil)
		w := httptest.NewRecorder()

		handler.handleChannelsPoll(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("rejects missing channel ID", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/channels//poll", nil)
		w := httptest.NewRecorder()

		handler.handleChannelsPoll(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("handles store error", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		store.AcquireNextMessageError = errors.New("database error")

		req := httptest.NewRequest(http.MethodGet, "/v1/channels/test-channel/poll", nil)
		w := httptest.NewRecorder()

		handler.handleChannelsPoll(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("Expected status %d, got %d", http.StatusInternalServerError, w.Code)
		}
	})

	t.Run("rejects non-GET methods", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodPost, "/v1/channels/test-channel/poll", nil)
		w := httptest.NewRecorder()

		handler.handleChannelsPoll(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("Expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
		}
	})

	t.Run("returns 404 for path without /poll suffix", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/channels/test-channel", nil)
		w := httptest.NewRecorder()

		handler.handleChannelsPoll(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
		}
	})

	t.Run("returns empty array when no messages", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		store.AcquireNextMessageResult = nil

		req := httptest.NewRequest(http.MethodGet, "/v1/channels/test-channel/poll", nil)
		w := httptest.NewRecorder()

		handler.handleChannelsPoll(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]interface{}
		json.NewDecoder(w.Body).Decode(&response)

		messages, ok := response["messages"].([]interface{})
		if !ok {
			t.Fatal("Expected messages array in response")
		}

		if len(messages) != 0 {
			t.Errorf("Expected empty messages array, got %d messages", len(messages))
		}
	})
}

func TestHandleAckMessage(t *testing.T) {
	t.Run("successfully acknowledges messages", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		reqBody := NackAckRequest{
			MessageIDs: []string{uuid.New().String(), uuid.New().String()},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/ack", bytes.NewReader(body))
		w := httptest.NewRecorder()

		handler.handleAck(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]interface{}
		json.NewDecoder(w.Body).Decode(&response)

		if response["success"] != true {
			t.Error("Expected success to be true")
		}

		if response["count"].(float64) != 2 {
			t.Errorf("Expected count 2, got %v", response["count"])
		}

		if store.AckMessageCalls != 1 {
			t.Errorf("Expected AckMessage to be called once, got %d calls", store.AckMessageCalls)
		}
	})

	t.Run("rejects empty message ID list", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		reqBody := NackAckRequest{MessageIDs: []string{}}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/ack", bytes.NewReader(body))
		w := httptest.NewRecorder()

		handler.handleAck(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("rejects message count above maximum", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		// Create 1001 message IDs (above MaxBatchSize of 1000)
		messageIDs := make([]string, 1001)
		for i := 0; i < 1001; i++ {
			messageIDs[i] = uuid.New().String()
		}

		reqBody := NackAckRequest{MessageIDs: messageIDs}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/ack", bytes.NewReader(body))
		w := httptest.NewRecorder()

		handler.handleAck(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("rejects invalid UUID format", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		reqBody := NackAckRequest{
			MessageIDs: []string{"invalid-uuid"},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/ack", bytes.NewReader(body))
		w := httptest.NewRecorder()

		handler.handleAck(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("rejects invalid JSON body", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/ack", bytes.NewReader([]byte("invalid json")))
		w := httptest.NewRecorder()

		handler.handleAck(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("handles store error", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		store.AckMessageError = errors.New("database error")

		reqBody := NackAckRequest{
			MessageIDs: []string{uuid.New().String()},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/ack", bytes.NewReader(body))
		w := httptest.NewRecorder()

		handler.handleAck(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("Expected status %d, got %d", http.StatusInternalServerError, w.Code)
		}
	})

	t.Run("rejects non-POST methods", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/messages/ack", nil)
		w := httptest.NewRecorder()

		handler.handleAck(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("Expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
		}
	})

	t.Run("returns 404 for incorrect path", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/ack/extra", nil)
		w := httptest.NewRecorder()

		handler.handleAck(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
		}
	})
}

func TestHandleNackMessage(t *testing.T) {
	t.Run("successfully nacks messages", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		reqBody := NackAckRequest{
			MessageIDs: []string{uuid.New().String(), uuid.New().String()},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/nack", bytes.NewReader(body))
		w := httptest.NewRecorder()

		handler.handleNack(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]interface{}
		json.NewDecoder(w.Body).Decode(&response)

		if response["success"] != true {
			t.Error("Expected success to be true")
		}

		if response["count"].(float64) != 2 {
			t.Errorf("Expected count 2, got %v", response["count"])
		}

		if store.NackMessageCalls != 1 {
			t.Errorf("Expected NackMessage to be called once, got %d calls", store.NackMessageCalls)
		}
	})

	t.Run("rejects empty message ID list", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		reqBody := NackAckRequest{MessageIDs: []string{}}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/nack", bytes.NewReader(body))
		w := httptest.NewRecorder()

		handler.handleNack(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("rejects invalid UUID format", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		reqBody := NackAckRequest{
			MessageIDs: []string{"invalid-uuid"},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/nack", bytes.NewReader(body))
		w := httptest.NewRecorder()

		handler.handleNack(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("handles store error", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		store.NackMessageError = errors.New("database error")

		reqBody := NackAckRequest{
			MessageIDs: []string{uuid.New().String()},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/nack", bytes.NewReader(body))
		w := httptest.NewRecorder()

		handler.handleNack(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("Expected status %d, got %d", http.StatusInternalServerError, w.Code)
		}
	})

	t.Run("rejects non-POST methods", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodGet, "/v1/messages/nack", nil)
		w := httptest.NewRecorder()

		handler.handleNack(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("Expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
		}
	})

	t.Run("returns 404 for incorrect path", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/nack/extra", nil)
		w := httptest.NewRecorder()

		handler.handleNack(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
		}
	})
}

func TestNackAckMessage(t *testing.T) {
	t.Run("handles ack purpose correctly", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		reqBody := NackAckRequest{
			MessageIDs: []string{uuid.New().String()},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/ack", bytes.NewReader(body))
		w := httptest.NewRecorder()

		handler.NackAckMessage(w, req, NackAckPurposeAck)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}

		if store.AckMessageCalls != 1 {
			t.Errorf("Expected AckMessage to be called, got %d calls", store.AckMessageCalls)
		}

		if store.NackMessageCalls != 0 {
			t.Errorf("Expected NackMessage not to be called, got %d calls", store.NackMessageCalls)
		}
	})

	t.Run("handles nack purpose correctly", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		reqBody := NackAckRequest{
			MessageIDs: []string{uuid.New().String()},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/nack", bytes.NewReader(body))
		w := httptest.NewRecorder()

		handler.NackAckMessage(w, req, NackAckPurposeNack)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}

		if store.NackMessageCalls != 1 {
			t.Errorf("Expected NackMessage to be called, got %d calls", store.NackMessageCalls)
		}

		if store.AckMessageCalls != 0 {
			t.Errorf("Expected AckMessage not to be called, got %d calls", store.AckMessageCalls)
		}
	})

	t.Run("handles invalid purpose", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		reqBody := NackAckRequest{
			MessageIDs: []string{uuid.New().String()},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/invalid", bytes.NewReader(body))
		w := httptest.NewRecorder()

		handler.NackAckMessage(w, req, NackAckPurpose("invalid"))

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("validates batch size limits", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		// Test minimum batch size
		reqBody := NackAckRequest{MessageIDs: []string{}}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/ack", bytes.NewReader(body))
		w := httptest.NewRecorder()

		handler.NackAckMessage(w, req, NackAckPurposeAck)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d for empty batch, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		store.AckMessageError = context.Canceled

		reqBody := NackAckRequest{
			MessageIDs: []string{uuid.New().String()},
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/v1/messages/ack", bytes.NewReader(body))
		w := httptest.NewRecorder()

		handler.NackAckMessage(w, req, NackAckPurposeAck)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("Expected status %d, got %d", http.StatusInternalServerError, w.Code)
		}
	})
}

func TestHandlerIntegration(t *testing.T) {
	t.Run("full workflow: list channels, poll, ack", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		consumer.Start()

		// Step 1: List channels
		store.ListChannelsResult = []*models.Channel{
			{Name: "test-channel"},
		}

		req := httptest.NewRequest(http.MethodGet, "/v1/channels", nil)
		w := httptest.NewRecorder()
		handler.ListChannels(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("Failed to list channels: status %d", w.Code)
		}

		// Step 2: Poll messages
		msg1 := uuid.New()
		store.AcquireNextMessageResult = []*models.Message{
			{ID: msg1, Channel: "test-channel", Payload: []byte("test")},
		}

		req = httptest.NewRequest(http.MethodGet, "/v1/channels/test-channel/poll?max=10", nil)
		w = httptest.NewRecorder()
		handler.PollMessages(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("Failed to poll messages: status %d", w.Code)
		}

		// Step 3: Acknowledge message
		reqBody := NackAckRequest{
			MessageIDs: []string{msg1.String()},
		}
		body, _ := json.Marshal(reqBody)

		req = httptest.NewRequest(http.MethodPost, "/v1/messages/ack", bytes.NewReader(body))
		w = httptest.NewRecorder()
		handler.NackAckMessage(w, req, NackAckPurposeAck)

		if w.Code != http.StatusOK {
			t.Fatalf("Failed to ack message: status %d", w.Code)
		}

		// Verify all operations were called
		if store.ListChannelsCalls != 1 {
			t.Errorf("Expected ListChannels to be called once, got %d", store.ListChannelsCalls)
		}

		if store.AcquireNextMessageCalls != 1 {
			t.Errorf("Expected AcquireNextMessage to be called once, got %d", store.AcquireNextMessageCalls)
		}

		if store.AckMessageCalls != 1 {
			t.Errorf("Expected AckMessage to be called once, got %d", store.AckMessageCalls)
		}
	})

	t.Run("handles errors gracefully throughout workflow", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		consumer := NewConsumer(store, "test-node", log)
		handler := NewHandler(consumer, log)

		// Set up error conditions
		store.ListChannelsError = errors.New("db error")
		store.AcquireNextMessageError = errors.New("db error")
		store.AckMessageError = errors.New("db error")

		// Try each operation and expect errors
		req := httptest.NewRequest(http.MethodGet, "/v1/channels", nil)
		w := httptest.NewRecorder()
		handler.ListChannels(w, req)
		if w.Code != http.StatusInternalServerError {
			t.Error("Expected error when listing channels")
		}

		req = httptest.NewRequest(http.MethodGet, "/v1/channels/test/poll", nil)
		w = httptest.NewRecorder()
		handler.PollMessages(w, req)
		if w.Code != http.StatusInternalServerError {
			t.Error("Expected error when polling messages")
		}

		reqBody := NackAckRequest{MessageIDs: []string{uuid.New().String()}}
		body, _ := json.Marshal(reqBody)
		req = httptest.NewRequest(http.MethodPost, "/v1/messages/ack", bytes.NewReader(body))
		w = httptest.NewRecorder()
		handler.NackAckMessage(w, req, NackAckPurposeAck)
		if w.Code != http.StatusInternalServerError {
			t.Error("Expected error when acking messages")
		}
	})
}

func TestHandlerContentType(t *testing.T) {
	store := &testutils.MockStore{}
	log := testutils.CreateTestLogger()
	consumer := NewConsumer(store, "test-node", log)
	handler := NewHandler(consumer, log)

	testCases := []struct {
		name    string
		handler func(w http.ResponseWriter, r *http.Request)
		method  string
		path    string
		body    []byte
	}{
		{
			name:    "ListChannels",
			handler: handler.ListChannels,
			method:  http.MethodGet,
			path:    "/v1/channels",
		},
		{
			name:    "PollMessages",
			handler: handler.PollMessages,
			method:  http.MethodGet,
			path:    "/v1/channels/test/poll",
		},
		{
			name: "AckMessage",
			handler: func(w http.ResponseWriter, r *http.Request) {
				handler.NackAckMessage(w, r, NackAckPurposeAck)
			},
			method: http.MethodPost,
			path:   "/v1/messages/ack",
			body: func() []byte {
				reqBody := NackAckRequest{MessageIDs: []string{uuid.New().String()}}
				body, _ := json.Marshal(reqBody)
				return body
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name+" sets correct content type", func(t *testing.T) {
			var req *http.Request
			if tc.body != nil {
				req = httptest.NewRequest(tc.method, tc.path, bytes.NewReader(tc.body))
			} else {
				req = httptest.NewRequest(tc.method, tc.path, nil)
			}

			w := httptest.NewRecorder()
			tc.handler(w, req)

			contentType := w.Header().Get("Content-Type")
			if contentType != "application/json" {
				t.Errorf("Expected Content-Type to be application/json, got %s", contentType)
			}
		})
	}
}
