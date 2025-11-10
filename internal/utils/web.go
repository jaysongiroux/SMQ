package utils

import (
	"net/http"
	"time"

	"github.com/jaysongiroux/smq/internal/logger"
)

func StartHTTPServer(addr string, handler http.Handler, log *logger.Logger) *http.Server {
	server := &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Info("HTTP server starting on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("HTTP server failed: %v", err)
		}
	}()

	return server
}
