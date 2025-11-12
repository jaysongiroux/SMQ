package middleware

import (
	"net/http"
	"time"

	"github.com/jaysongiroux/smq/internal/logger"
)

func LoggingMiddleware(log *logger.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

			log.Info("→ %s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)

			next.ServeHTTP(wrapped, r)

			duration := time.Since(start)
			log.Info("← %s %s [%d] took %v", r.Method, r.URL.Path, wrapped.statusCode, duration)
		})
	}
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func AuthMiddleware(apiKey string, log *logger.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestKey := r.Header.Get("api-key")

			if requestKey == "" {
				log.Warn("Request rejected: missing api-key header from %s", r.RemoteAddr)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, err := w.Write([]byte(`{"error":"missing api-key header"}`))
				if err != nil {
					log.Error("Failed to write response: %v", err)
				}
				return
			}

			if requestKey != apiKey {
				log.Warn("Request rejected: invalid api-key from %s", r.RemoteAddr)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, err := w.Write([]byte(`{"error":"invalid api-key"}`))
				if err != nil {
					log.Error("Failed to write response: %v", err)
				}
				return
			}

			log.Debug("Request authenticated from %s", r.RemoteAddr)
			next.ServeHTTP(w, r)
		})
	}
}
