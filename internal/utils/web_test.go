package utils

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/testutils"
)

func TestStartHTTPServer(t *testing.T) {
	t.Run("starts server successfully", func(t *testing.T) {
		log := logger.New("test", testutils.CreateTestConfig())
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("test response"))
		})

		// Find an available port for testing
		port := findAvailablePort(t)
		addr := fmt.Sprintf("localhost:%d", port)
		server := StartHTTPServer(addr, handler, log)
		defer server.Close()

		// Give server time to start
		time.Sleep(50 * time.Millisecond)

		// Verify server is running by making a request
		resp, err := http.Get(fmt.Sprintf("http://%s", addr))
		if err != nil {
			t.Fatalf("Failed to make request to server: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("Failed to read response body: %v", err)
		}

		expected := "test response"
		if string(body) != expected {
			t.Errorf("Expected body %q, got %q", expected, string(body))
		}
	})

	t.Run("handler is called correctly", func(t *testing.T) {
		log := logger.New("test", testutils.CreateTestConfig())
		handlerCalled := false
		var receivedMethod string
		var receivedPath string

		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handlerCalled = true
			receivedMethod = r.Method
			receivedPath = r.URL.Path
			w.WriteHeader(http.StatusOK)
		})

		port := findAvailablePort(t)
		addr := fmt.Sprintf("localhost:%d", port)
		server := StartHTTPServer(addr, handler, log)
		defer server.Close()

		// Give server time to start
		time.Sleep(50 * time.Millisecond)

		// Make request
		resp, err := http.Get(fmt.Sprintf("http://%s/test/path", addr))
		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}
		defer resp.Body.Close()

		if !handlerCalled {
			t.Error("Handler was not called")
		}

		if receivedMethod != "GET" {
			t.Errorf("Expected method GET, got %s", receivedMethod)
		}

		if receivedPath != "/test/path" {
			t.Errorf("Expected path /test/path, got %s", receivedPath)
		}
	})

	t.Run("server can be shut down gracefully", func(t *testing.T) {
		log := logger.New("test", testutils.CreateTestConfig())
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})

		port := findAvailablePort(t)
		addr := fmt.Sprintf("localhost:%d", port)
		server := StartHTTPServer(addr, handler, log)

		// Give server time to start
		time.Sleep(50 * time.Millisecond)

		// Verify server is running
		resp, err := http.Get(fmt.Sprintf("http://%s", addr))
		if err != nil {
			t.Fatalf("Server not running: %v", err)
		}
		resp.Body.Close()

		// Shutdown server
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("Failed to shutdown server: %v", err)
		}

		// Verify server is no longer accepting connections
		time.Sleep(50 * time.Millisecond)
		_, err = http.Get(fmt.Sprintf("http://%s", addr))
		if err == nil {
			t.Error("Expected error when connecting to stopped server")
		}
	})

	t.Run("server has correct timeout configurations", func(t *testing.T) {
		log := logger.New("test", testutils.CreateTestConfig())
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})

		port := findAvailablePort(t)
		addr := fmt.Sprintf("localhost:%d", port)
		server := StartHTTPServer(addr, handler, log)
		defer server.Close()

		// Verify timeout configurations
		if server.ReadTimeout != 15*time.Second {
			t.Errorf("Expected ReadTimeout 15s, got %v", server.ReadTimeout)
		}

		if server.WriteTimeout != 15*time.Second {
			t.Errorf("Expected WriteTimeout 15s, got %v", server.WriteTimeout)
		}

		if server.IdleTimeout != 60*time.Second {
			t.Errorf("Expected IdleTimeout 60s, got %v", server.IdleTimeout)
		}
	})

	t.Run("multiple requests are handled", func(t *testing.T) {
		log := logger.New("test", testutils.CreateTestConfig())
		requestCount := 0

		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "Request %d", requestCount)
		})

		port := findAvailablePort(t)
		addr := fmt.Sprintf("localhost:%d", port)
		server := StartHTTPServer(addr, handler, log)
		defer server.Close()

		time.Sleep(50 * time.Millisecond)

		for i := 1; i <= 3; i++ {
			resp, err := http.Get(fmt.Sprintf("http://%s", addr))
			if err != nil {
				t.Fatalf("Request %d failed: %v", i, err)
			}

			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			expected := fmt.Sprintf("Request %d", i)
			if string(body) != expected {
				t.Errorf("Request %d: expected %q, got %q", i, expected, string(body))
			}
		}

		if requestCount != 3 {
			t.Errorf("Expected 3 requests, got %d", requestCount)
		}
	})
}

// findAvailablePort finds an available port for testing
func findAvailablePort(t *testing.T) int {
	t.Helper()

	// Create a listener on port 0 to get an available port
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to find available port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	// Small delay to ensure port is released
	time.Sleep(10 * time.Millisecond)

	return port
}
