package logger

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"github.com/jaysongiroux/smq/internal/config"
)

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected LogLevel
	}{
		{"debug", DebugLevel},
		{"DEBUG", DebugLevel},
		{"info", InfoLevel},
		{"INFO", InfoLevel},
		{"warn", WarnLevel},
		{"WARN", WarnLevel},
		{"warning", WarnLevel},
		{"error", ErrorLevel},
		{"ERROR", ErrorLevel},
		{"invalid", InfoLevel}, // Default
		{"", InfoLevel},        // Default
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ParseLogLevel(tt.input)
			if result != tt.expected {
				t.Errorf("ParseLogLevel(%q) = %v, expected %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestLogLevelString(t *testing.T) {
	tests := []struct {
		level    LogLevel
		expected string
	}{
		{DebugLevel, "DEBUG"},
		{InfoLevel, "INFO"},
		{WarnLevel, "WARN"},
		{ErrorLevel, "ERROR"},
		{LogLevel(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.level.String()
			if result != tt.expected {
				t.Errorf("LogLevel.String() = %q, expected %q", result, tt.expected)
			}
		})
	}
}

func TestNew(t *testing.T) {
	cfg := &config.Config{LogLevel: "debug"}
	logger := New("test-service", cfg)

	if logger.service != "test-service" {
		t.Errorf("Expected service 'test-service', got %q", logger.service)
	}

	if logger.minLevel != DebugLevel {
		t.Errorf("Expected minLevel DEBUG, got %v", logger.minLevel)
	}

	if logger.logger == nil {
		t.Error("Expected logger to be initialized")
	}
}

func TestShouldLog(t *testing.T) {
	tests := []struct {
		name      string
		minLevel  LogLevel
		testLevel LogLevel
		expected  bool
	}{
		{"debug logs everything", DebugLevel, DebugLevel, true},
		{"debug logs info", DebugLevel, InfoLevel, true},
		{"debug logs warn", DebugLevel, WarnLevel, true},
		{"debug logs error", DebugLevel, ErrorLevel, true},
		{"info blocks debug", InfoLevel, DebugLevel, false},
		{"info logs info", InfoLevel, InfoLevel, true},
		{"warn blocks debug", WarnLevel, DebugLevel, false},
		{"warn blocks info", WarnLevel, InfoLevel, false},
		{"warn logs warn", WarnLevel, WarnLevel, true},
		{"error blocks all except error", ErrorLevel, WarnLevel, false},
		{"error logs error", ErrorLevel, ErrorLevel, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := &Logger{minLevel: tt.minLevel}
			result := logger.shouldLog(tt.testLevel)
			if result != tt.expected {
				t.Errorf("shouldLog(%v) with minLevel %v = %v, expected %v",
					tt.testLevel, tt.minLevel, result, tt.expected)
			}
		})
	}
}

func TestLoggerOutput(t *testing.T) {
	t.Run("debug logs when level is debug", func(t *testing.T) {
		var buf bytes.Buffer
		cfg := &config.Config{LogLevel: "debug"}
		logger := New("test", cfg)
		logger.logger = log.New(&buf, "", 0)

		logger.Debug("test message")

		output := buf.String()
		if !strings.Contains(output, "DEBUG") {
			t.Errorf("Expected DEBUG in output, got: %s", output)
		}
		if !strings.Contains(output, "test message") {
			t.Errorf("Expected 'test message' in output, got: %s", output)
		}
		if !strings.Contains(output, "[test]") {
			t.Errorf("Expected [test] service name in output, got: %s", output)
		}
	})

	t.Run("debug does not log when level is info", func(t *testing.T) {
		var buf bytes.Buffer
		cfg := &config.Config{LogLevel: "info"}
		logger := New("test", cfg)
		logger.logger = log.New(&buf, "", 0)

		logger.Debug("should not appear")

		output := buf.String()
		if output != "" {
			t.Errorf("Expected no output, got: %s", output)
		}
	})

	t.Run("info logs when level is info", func(t *testing.T) {
		var buf bytes.Buffer
		cfg := &config.Config{LogLevel: "info"}
		logger := New("test", cfg)
		logger.logger = log.New(&buf, "", 0)

		logger.Info("info message")

		output := buf.String()
		if !strings.Contains(output, "INFO") {
			t.Errorf("Expected INFO in output, got: %s", output)
		}
		if !strings.Contains(output, "info message") {
			t.Errorf("Expected 'info message' in output, got: %s", output)
		}
	})

	t.Run("warn logs when level is warn", func(t *testing.T) {
		var buf bytes.Buffer
		cfg := &config.Config{LogLevel: "warn"}
		logger := New("test", cfg)
		logger.logger = log.New(&buf, "", 0)

		logger.Warn("warning message")

		output := buf.String()
		if !strings.Contains(output, "WARN") {
			t.Errorf("Expected WARN in output, got: %s", output)
		}
		if !strings.Contains(output, "warning message") {
			t.Errorf("Expected 'warning message' in output, got: %s", output)
		}
	})

	t.Run("error always logs", func(t *testing.T) {
		var buf bytes.Buffer
		cfg := &config.Config{LogLevel: "error"}
		logger := New("test", cfg)
		logger.logger = log.New(&buf, "", 0)

		logger.Error("error message")

		output := buf.String()
		if !strings.Contains(output, "ERROR") {
			t.Errorf("Expected ERROR in output, got: %s", output)
		}
		if !strings.Contains(output, "error message") {
			t.Errorf("Expected 'error message' in output, got: %s", output)
		}
	})

	t.Run("formats message with arguments", func(t *testing.T) {
		var buf bytes.Buffer
		cfg := &config.Config{LogLevel: "info"}
		logger := New("test", cfg)
		logger.logger = log.New(&buf, "", 0)

		logger.Info("user %s logged in with id %d", "alice", 123)

		output := buf.String()
		if !strings.Contains(output, "user alice logged in with id 123") {
			t.Errorf("Expected formatted message in output, got: %s", output)
		}
	})
}

func TestWithService(t *testing.T) {
	cfg := &config.Config{LogLevel: "debug"}
	logger := New("original", cfg)

	newLogger := logger.WithService("new-service")

	if newLogger.service != "new-service" {
		t.Errorf("Expected service 'new-service', got %q", newLogger.service)
	}

	if newLogger.minLevel != logger.minLevel {
		t.Error("Expected minLevel to be preserved")
	}

	if newLogger.logger != logger.logger {
		t.Error("Expected logger instance to be shared")
	}

	// Original logger should be unchanged
	if logger.service != "original" {
		t.Errorf("Expected original service to remain 'original', got %q", logger.service)
	}
}

func TestFormatMessage(t *testing.T) {
	cfg := &config.Config{LogLevel: "info"}
	logger := New("test-service", cfg)

	message := logger.formatMessage(InfoLevel, "test message", 0)

	// Check for expected components
	if !strings.Contains(message, "INFO") {
		t.Errorf("Expected INFO in message, got: %s", message)
	}

	if !strings.Contains(message, "test-service") {
		t.Errorf("Expected test-service in message, got: %s", message)
	}

	if !strings.Contains(message, "test message") {
		t.Errorf("Expected 'test message' in message, got: %s", message)
	}

	// Check for timestamp format (YYYY-MM-DD HH:MM:SS.mmm)
	if !strings.Contains(message, "[202") { // Year starts with 202
		t.Errorf("Expected timestamp in message, got: %s", message)
	}

	// Check for file:line format
	if !strings.Contains(message, ".go:") {
		t.Errorf("Expected file:line info in message, got: %s", message)
	}
}

func TestLoggerLevelFiltering(t *testing.T) {
	tests := []struct {
		name     string
		logLevel string
		logDebug bool
		logInfo  bool
		logWarn  bool
		logError bool
	}{
		{"debug level logs all", "debug", true, true, true, true},
		{"info level skips debug", "info", false, true, true, true},
		{"warn level skips debug and info", "warn", false, false, true, true},
		{"error level only logs errors", "error", false, false, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			cfg := &config.Config{LogLevel: tt.logLevel}
			logger := New("test", cfg)
			logger.logger = log.New(&buf, "", 0)

			logger.Debug("debug")
			debugLogged := strings.Contains(buf.String(), "debug")
			buf.Reset()

			logger.Info("info")
			infoLogged := strings.Contains(buf.String(), "info")
			buf.Reset()

			logger.Warn("warn")
			warnLogged := strings.Contains(buf.String(), "warn")
			buf.Reset()

			logger.Error("error")
			errorLogged := strings.Contains(buf.String(), "error")

			if debugLogged != tt.logDebug {
				t.Errorf("Debug logging = %v, expected %v", debugLogged, tt.logDebug)
			}
			if infoLogged != tt.logInfo {
				t.Errorf("Info logging = %v, expected %v", infoLogged, tt.logInfo)
			}
			if warnLogged != tt.logWarn {
				t.Errorf("Warn logging = %v, expected %v", warnLogged, tt.logWarn)
			}
			if errorLogged != tt.logError {
				t.Errorf("Error logging = %v, expected %v", errorLogged, tt.logError)
			}
		})
	}
}
