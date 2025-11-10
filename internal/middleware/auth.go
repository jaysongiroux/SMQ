package middleware

import (
	"net/http"
	"time"

	"github.com/jaysongiroux/smq/internal/logger"
)

// LoggingMiddleware creates a middleware that logs HTTP requests and responses
func LoggingMiddleware(log *logger.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Create a response writer wrapper to capture status code
			wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

			// Log incoming request
			log.Info("→ %s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)

			// Call next handler
			next.ServeHTTP(wrapped, r)

			// Log response
			duration := time.Since(start)
			log.Info("← %s %s [%d] took %v", r.Method, r.URL.Path, wrapped.statusCode, duration)
		})
	}
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// AuthMiddleware creates a middleware that validates API key from headers
func AuthMiddleware(apiKey string, log *logger.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Get API key from header
			requestKey := r.Header.Get("api-key")

			// Check if API key matches
			if requestKey == "" {
				log.Warn("Request rejected: missing api-key header from %s", r.RemoteAddr)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error":"missing api-key header"}`))
				return
			}

			if requestKey != apiKey {
				log.Warn("Request rejected: invalid api-key from %s", r.RemoteAddr)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error":"invalid api-key"}`))
				return
			}

			// API key is valid, proceed to next handler
			log.Debug("Request authenticated from %s", r.RemoteAddr)
			next.ServeHTTP(w, r)
		})
	}
}
