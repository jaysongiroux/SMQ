package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jaysongiroux/smq/internal/config"
	"github.com/jaysongiroux/smq/internal/logger"
)

func createTestConfig() *config.Config {
	cfg := &config.Config{}
	cfg.LogLevel = "info"
	cfg.MinScheduledAtFutureMs = 5000
	return cfg
}

// Mock handler that returns 200 OK
func mockHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
}

// Mock handler that returns custom status code
func mockHandlerWithStatus(statusCode int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusCode)
		w.Write([]byte(`{"status":"custom"}`))
	})
}

// Mock handler that sleeps to test timing
func mockSlowHandler(delay time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"slow"}`))
	})
}

func TestLoggingMiddleware(t *testing.T) {
	t.Run("logs request and response with 200 status", func(t *testing.T) {
		log := logger.New("test", createTestConfig())
		middleware := LoggingMiddleware(log)
		handler := middleware(mockHandler())

		req := httptest.NewRequest("GET", "/test/path", nil)
		req.RemoteAddr = "127.0.0.1:12345"
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}

		body := w.Body.String()
		if !strings.Contains(body, `"status":"ok"`) {
			t.Errorf("Expected response body to contain status ok, got: %s", body)
		}
	})

	t.Run("logs request with POST method", func(t *testing.T) {
		log := logger.New("test", createTestConfig())
		middleware := LoggingMiddleware(log)
		handler := middleware(mockHandler())

		req := httptest.NewRequest("POST", "/api/messages", strings.NewReader(`{"test":"data"}`))
		req.RemoteAddr = "192.168.1.1:54321"
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}
	})

	t.Run("logs request with custom status code", func(t *testing.T) {
		log := logger.New("test", createTestConfig())
		middleware := LoggingMiddleware(log)
		handler := middleware(mockHandlerWithStatus(http.StatusCreated))

		req := httptest.NewRequest("POST", "/api/resource", nil)
		req.RemoteAddr = "10.0.0.1:8080"
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("Expected status 201, got %d", w.Code)
		}
	})

	t.Run("logs request with error status code", func(t *testing.T) {
		log := logger.New("test", createTestConfig())
		middleware := LoggingMiddleware(log)
		handler := middleware(mockHandlerWithStatus(http.StatusInternalServerError))

		req := httptest.NewRequest("DELETE", "/api/resource/123", nil)
		req.RemoteAddr = "172.16.0.1:9000"
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("Expected status 500, got %d", w.Code)
		}
	})

	t.Run("measures request duration", func(t *testing.T) {
		log := logger.New("test", createTestConfig())
		middleware := LoggingMiddleware(log)

		delay := 50 * time.Millisecond
		handler := middleware(mockSlowHandler(delay))

		req := httptest.NewRequest("GET", "/slow", nil)
		w := httptest.NewRecorder()

		start := time.Now()
		handler.ServeHTTP(w, req)
		duration := time.Since(start)

		// Check that the request took at least as long as the delay
		if duration < delay {
			t.Errorf("Expected duration >= %v, got %v", delay, duration)
		}

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}
	})

	t.Run("handles handler that doesn't write status code explicitly", func(t *testing.T) {
		log := logger.New("test", createTestConfig())
		middleware := LoggingMiddleware(log)

		// Handler that doesn't call WriteHeader
		implicitHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"implicit":"status"}`))
		})

		handler := middleware(implicitHandler)

		req := httptest.NewRequest("GET", "/implicit", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		// Should default to 200
		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}
	})
}

func TestAuthMiddleware(t *testing.T) {
	validAPIKey := "test-api-key-12345"

	t.Run("allows request with valid api-key", func(t *testing.T) {
		log := logger.New("test", createTestConfig())
		middleware := AuthMiddleware(validAPIKey, log)
		handler := middleware(mockHandler())

		req := httptest.NewRequest("GET", "/protected", nil)
		req.Header.Set("api-key", validAPIKey)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}

		body := w.Body.String()
		if !strings.Contains(body, `"status":"ok"`) {
			t.Errorf("Expected response body to contain status ok, got: %s", body)
		}
	})

	t.Run("rejects request with missing api-key header", func(t *testing.T) {
		log := logger.New("test", createTestConfig())
		middleware := AuthMiddleware(validAPIKey, log)
		handler := middleware(mockHandler())

		req := httptest.NewRequest("GET", "/protected", nil)
		// Don't set api-key header
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("Expected status 401, got %d", w.Code)
		}

		if w.Header().Get("Content-Type") != "application/json" {
			t.Errorf("Expected Content-Type application/json, got %s", w.Header().Get("Content-Type"))
		}

		var response map[string]string
		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if response["error"] != "missing api-key header" {
			t.Errorf("Expected error 'missing api-key header', got: %s", response["error"])
		}
	})

	t.Run("rejects request with empty api-key header", func(t *testing.T) {
		log := logger.New("test", createTestConfig())
		middleware := AuthMiddleware(validAPIKey, log)
		handler := middleware(mockHandler())

		req := httptest.NewRequest("POST", "/protected", nil)
		req.Header.Set("api-key", "") // Empty string
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("Expected status 401, got %d", w.Code)
		}

		var response map[string]string
		json.NewDecoder(w.Body).Decode(&response)

		if response["error"] != "missing api-key header" {
			t.Errorf("Expected error 'missing api-key header', got: %s", response["error"])
		}
	})

	t.Run("rejects request with invalid api-key", func(t *testing.T) {
		log := logger.New("test", createTestConfig())
		middleware := AuthMiddleware(validAPIKey, log)
		handler := middleware(mockHandler())

		req := httptest.NewRequest("DELETE", "/protected", nil)
		req.Header.Set("api-key", "wrong-api-key")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("Expected status 401, got %d", w.Code)
		}

		if w.Header().Get("Content-Type") != "application/json" {
			t.Errorf("Expected Content-Type application/json, got %s", w.Header().Get("Content-Type"))
		}

		var response map[string]string
		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if response["error"] != "invalid api-key" {
			t.Errorf("Expected error 'invalid api-key', got: %s", response["error"])
		}
	})

	t.Run("rejects request with api-key that differs only in case", func(t *testing.T) {
		log := logger.New("test", createTestConfig())
		middleware := AuthMiddleware("SecretKey123", log)
		handler := middleware(mockHandler())

		req := httptest.NewRequest("GET", "/protected", nil)
		req.Header.Set("api-key", "secretkey123") // Different case
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("Expected status 401, got %d", w.Code)
		}
	})

	t.Run("allows multiple requests with valid api-key", func(t *testing.T) {
		log := logger.New("test", createTestConfig())
		middleware := AuthMiddleware(validAPIKey, log)
		handler := middleware(mockHandler())

		for i := 0; i < 5; i++ {
			req := httptest.NewRequest("GET", "/protected", nil)
			req.Header.Set("api-key", validAPIKey)
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Errorf("Request %d: Expected status 200, got %d", i, w.Code)
			}
		}
	})
}

func TestMiddlewareChaining(t *testing.T) {
	t.Run("chains logging and auth middleware", func(t *testing.T) {
		validAPIKey := "chained-key"
		log := logger.New("test", createTestConfig())

		// Chain: Logging -> Auth -> Handler
		handler := LoggingMiddleware(log)(
			AuthMiddleware(validAPIKey, log)(
				mockHandler(),
			),
		)

		// Test with valid key
		req := httptest.NewRequest("GET", "/chained", nil)
		req.Header.Set("api-key", validAPIKey)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}
	})

	t.Run("chains middleware and rejects unauthorized request", func(t *testing.T) {
		validAPIKey := "chained-key"
		log := logger.New("test", createTestConfig())

		handler := LoggingMiddleware(log)(
			AuthMiddleware(validAPIKey, log)(
				mockHandler(),
			),
		)

		// Test without key
		req := httptest.NewRequest("GET", "/chained", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("Expected status 401, got %d", w.Code)
		}

		var response map[string]string
		json.NewDecoder(w.Body).Decode(&response)

		if response["error"] != "missing api-key header" {
			t.Errorf("Expected error 'missing api-key header', got: %s", response["error"])
		}
	})
}

func TestResponseWriter(t *testing.T) {
	t.Run("captures status code when WriteHeader is called", func(t *testing.T) {
		w := httptest.NewRecorder()
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		rw.WriteHeader(http.StatusCreated)

		if rw.statusCode != http.StatusCreated {
			t.Errorf("Expected statusCode 201, got %d", rw.statusCode)
		}

		if w.Code != http.StatusCreated {
			t.Errorf("Expected underlying recorder code 201, got %d", w.Code)
		}
	})

	t.Run("defaults to 200 when WriteHeader is not called", func(t *testing.T) {
		w := httptest.NewRecorder()
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// Don't call WriteHeader, just write body
		rw.Write([]byte("test"))

		if rw.statusCode != http.StatusOK {
			t.Errorf("Expected statusCode 200, got %d", rw.statusCode)
		}
	})

	t.Run("captures multiple WriteHeader calls (last one wins)", func(t *testing.T) {
		w := httptest.NewRecorder()
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		rw.WriteHeader(http.StatusCreated)
		rw.WriteHeader(http.StatusBadRequest)

		// Your current implementation captures the LAST call, not the first
		// The underlying http.ResponseWriter ignores subsequent calls, but your wrapper doesn't
		if rw.statusCode != http.StatusBadRequest {
			t.Errorf("Expected statusCode 400 (last call captured by wrapper), got %d", rw.statusCode)
		}

		// The underlying recorder should still be 201 (first call)
		if w.Code != http.StatusCreated {
			t.Errorf("Expected underlying recorder code 201 (first call wins), got %d", w.Code)
		}
	})
}
