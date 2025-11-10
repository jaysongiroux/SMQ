package logger

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/jaysongiroux/smq/internal/config"
)

// LogLevel represents the severity level of a log message
type LogLevel int

const (
	// DebugLevel for detailed debugging information
	DebugLevel LogLevel = iota
	// InfoLevel for general informational messages
	InfoLevel
	// WarnLevel for warning messages
	WarnLevel
	// ErrorLevel for error messages
	ErrorLevel
)

// String returns the string representation of the log level
func (l LogLevel) String() string {
	switch l {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// ParseLogLevel converts a string to a LogLevel
func ParseLogLevel(level string) LogLevel {
	switch strings.ToLower(level) {
	case "debug":
		return DebugLevel
	case "info":
		return InfoLevel
	case "warn", "warning":
		return WarnLevel
	case "error":
		return ErrorLevel
	default:
		return InfoLevel
	}
}

// Logger provides structured logging with context
type Logger struct {
	service  string
	minLevel LogLevel
	logger   *log.Logger
}

// New creates a new logger instance
// It reads the log level from the config service
func New(service string, cfg *config.Config) *Logger {
	return &Logger{
		service:  service,
		minLevel: ParseLogLevel(cfg.LogLevel),
		logger:   log.New(os.Stdout, "", 0),
	}
}

// shouldLog determines if a message should be logged based on its level
func (l *Logger) shouldLog(level LogLevel) bool {
	return level >= l.minLevel
}

// formatMessage creates a formatted log message with timestamp, level, service, and caller info
func (l *Logger) formatMessage(level LogLevel, msg string, skipFrames int) string {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	// Get caller information
	_, file, line, ok := runtime.Caller(skipFrames)
	fileInfo := "unknown"
	if ok {
		fileInfo = fmt.Sprintf("%s:%d", filepath.Base(file), line)
	}

	return fmt.Sprintf("[%s] [%s] [%s] [%s] %s",
		timestamp,
		level.String(),
		l.service,
		fileInfo,
		msg,
	)
}

// Debug logs a debug message
func (l *Logger) Debug(format string, args ...interface{}) {
	if l.shouldLog(DebugLevel) {
		msg := fmt.Sprintf(format, args...)
		l.logger.Println(l.formatMessage(DebugLevel, msg, 2))
	}
}

// Info logs an informational message
func (l *Logger) Info(format string, args ...interface{}) {
	if l.shouldLog(InfoLevel) {
		msg := fmt.Sprintf(format, args...)
		l.logger.Println(l.formatMessage(InfoLevel, msg, 2))
	}
}

// Warn logs a warning message
func (l *Logger) Warn(format string, args ...interface{}) {
	if l.shouldLog(WarnLevel) {
		msg := fmt.Sprintf(format, args...)
		l.logger.Println(l.formatMessage(WarnLevel, msg, 2))
	}
}

// Error logs an error message
func (l *Logger) Error(format string, args ...interface{}) {
	if l.shouldLog(ErrorLevel) {
		msg := fmt.Sprintf(format, args...)
		l.logger.Println(l.formatMessage(ErrorLevel, msg, 2))
	}
}

// Fatal logs an error message and exits the program
func (l *Logger) Fatal(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	l.logger.Println(l.formatMessage(ErrorLevel, msg, 2))
	os.Exit(1)
}

// WithService creates a new logger with a different service name
func (l *Logger) WithService(service string) *Logger {
	return &Logger{
		service:  service,
		minLevel: l.minLevel,
		logger:   l.logger,
	}
}
