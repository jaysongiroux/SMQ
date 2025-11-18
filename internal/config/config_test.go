package config

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestValidatePostgresConfig(t *testing.T) {
	tests := []struct {
		name      string
		config    *Config
		expectErr bool
		errMsg    string
	}{
		{
			name: "valid config",
			config: &Config{
				PostgresURL:          "postgresql://localhost:5432/db",
				PostgresMaxOpenConns: 50,
				PostgresMaxIdleConns: 25,
			},
			expectErr: false,
		},
		{
			name: "missing URL",
			config: &Config{
				PostgresURL:          "",
				PostgresMaxOpenConns: 50,
				PostgresMaxIdleConns: 25,
			},
			expectErr: true,
			errMsg:    "postgres_url",
		},
		{
			name: "max open conns too low",
			config: &Config{
				PostgresURL:          "postgresql://localhost:5432/db",
				PostgresMaxOpenConns: 0,
				PostgresMaxIdleConns: 25,
			},
			expectErr: true,
			errMsg:    "postgres_max_open_conns",
		},
		{
			name: "max open conns too high",
			config: &Config{
				PostgresURL:          "postgresql://localhost:5432/db",
				PostgresMaxOpenConns: 101,
				PostgresMaxIdleConns: 25,
			},
			expectErr: true,
			errMsg:    "postgres_max_open_conns",
		},
		{
			name: "max idle conns too low",
			config: &Config{
				PostgresURL:          "postgresql://localhost:5432/db",
				PostgresMaxOpenConns: 50,
				PostgresMaxIdleConns: 0,
			},
			expectErr: true,
			errMsg:    "postgres_max_idle_conns",
		},
		{
			name: "max idle conns too high",
			config: &Config{
				PostgresURL:          "postgresql://localhost:5432/db",
				PostgresMaxOpenConns: 50,
				PostgresMaxIdleConns: 101,
			},
			expectErr: true,
			errMsg:    "postgres_max_idle_conns",
		},
		{
			name: "boundary values at min",
			config: &Config{
				PostgresURL:          "postgresql://localhost:5432/db",
				PostgresMaxOpenConns: 1,
				PostgresMaxIdleConns: 1,
			},
			expectErr: false,
		},
		{
			name: "boundary values at max",
			config: &Config{
				PostgresURL:          "postgresql://localhost:5432/db",
				PostgresMaxOpenConns: 100,
				PostgresMaxIdleConns: 100,
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.validatePostgresConfig()

			if (err != nil) != tt.expectErr {
				t.Errorf("validatePostgresConfig() error = %v, expectErr %v", err, tt.expectErr)
			}

			if err != nil && tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("Expected error to contain %q, got: %v", tt.errMsg, err)
			}
		})
	}
}

func TestValidateCockroachConfig(t *testing.T) {
	tests := []struct {
		name      string
		config    *Config
		expectErr bool
		errMsg    string
	}{
		{
			name: "valid config",
			config: &Config{
				CockroachURL:          "postgresql://localhost:26257/db",
				CockroachRegion:       "us-east-1",
				CockroachMaxOpenConns: 50,
				CockroachMaxIdleConns: 25,
			},
			expectErr: false,
		},
		{
			name: "missing URL",
			config: &Config{
				CockroachURL:          "",
				CockroachRegion:       "us-east-1",
				CockroachMaxOpenConns: 50,
				CockroachMaxIdleConns: 25,
			},
			expectErr: true,
			errMsg:    "cockroach_url",
		},
		{
			name: "missing region",
			config: &Config{
				CockroachURL:          "postgresql://localhost:26257/db",
				CockroachRegion:       "",
				CockroachMaxOpenConns: 50,
				CockroachMaxIdleConns: 25,
			},
			expectErr: true,
			errMsg:    "cockroach_region",
		},
		{
			name: "max open conns too low",
			config: &Config{
				CockroachURL:          "postgresql://localhost:26257/db",
				CockroachRegion:       "us-east-1",
				CockroachMaxOpenConns: 0,
				CockroachMaxIdleConns: 25,
			},
			expectErr: true,
			errMsg:    "cockroach_max_open_conns",
		},
		{
			name: "max open conns too high",
			config: &Config{
				CockroachURL:          "postgresql://localhost:26257/db",
				CockroachRegion:       "us-east-1",
				CockroachMaxOpenConns: 101,
				CockroachMaxIdleConns: 25,
			},
			expectErr: true,
			errMsg:    "cockroach_max_open_conns",
		},
		{
			name: "max idle conns too low",
			config: &Config{
				CockroachURL:          "postgresql://localhost:26257/db",
				CockroachRegion:       "us-east-1",
				CockroachMaxOpenConns: 50,
				CockroachMaxIdleConns: 0,
			},
			expectErr: true,
			errMsg:    "cockroach_max_idle_conns",
		},
		{
			name: "max idle conns too high",
			config: &Config{
				CockroachURL:          "postgresql://localhost:26257/db",
				CockroachRegion:       "us-east-1",
				CockroachMaxOpenConns: 50,
				CockroachMaxIdleConns: 101,
			},
			expectErr: true,
			errMsg:    "cockroach_max_idle_conns",
		},
		{
			name: "boundary values at min",
			config: &Config{
				CockroachURL:          "postgresql://localhost:26257/db",
				CockroachRegion:       "us-east-1",
				CockroachMaxOpenConns: 1,
				CockroachMaxIdleConns: 1,
			},
			expectErr: false,
		},
		{
			name: "boundary values at max",
			config: &Config{
				CockroachURL:          "postgresql://localhost:26257/db",
				CockroachRegion:       "us-east-1",
				CockroachMaxOpenConns: 100,
				CockroachMaxIdleConns: 100,
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.validateCockroachConfig()

			if (err != nil) != tt.expectErr {
				t.Errorf("validateCockroachConfig() error = %v, expectErr %v", err, tt.expectErr)
			}

			if err != nil && tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("Expected error to contain %q, got: %v", tt.errMsg, err)
			}
		})
	}
}

// Test fixtures - reusable config structures
var (
	validMinimalConfig = JSONConfig{
		NumSchedulerNodes:             1,
		NumSchedulerJanitorNodes:      1,
		ProducerPort:                  8080,
		ConsumerPort:                  8081,
		HealthPort:                    8082,
		MsgTimeoutMs:                  30000,
		MaxRetries:                    3,
		MaxPayloadSizeKb:              10240,
		LogLevel:                      "info",
		Region:                        "us-east-1",
		SchedulerMaxMessagesPerPoll:   1000,
		HealthCheckIntervalMs:         5000,
		BufferFlushIntervalMs:         1000,
		MinScheduledAtFutureMs:        5000,
		SchedulerPollIntervalMs:       1000,
		SchedulerJanitorIntervalMs:    5000,
		SchedulerPollJitterPercent:    10,
		SchedulerJanitorJitterPercent: 10,
		BufferMaxSizeKb:               1000,
		BufferWorkerCount:             2,
		BufferType:                    "memory",
		Datastore:                     "postgres",
		PostgresURL:                   "postgresql://localhost:5432/test",
		PostgresMaxOpenConns:          10,
		PostgresMaxIdleConns:          5,
		ApiKey:                        "test-key-12345",
		CBMaxFailures:                 5,
		CBTimeout:                     30000,
		CBResetTimeout:                60000,
		HalfOpenMaxReqs:               1,
	}

	validCockroachConfig = JSONConfig{
		NumSchedulerNodes:             1,
		NumSchedulerJanitorNodes:      1,
		ProducerPort:                  8080,
		ConsumerPort:                  8081,
		HealthPort:                    8082,
		MsgTimeoutMs:                  30000,
		MaxRetries:                    3,
		MaxPayloadSizeKb:              10240,
		LogLevel:                      "info",
		Region:                        "us-west-2",
		SchedulerMaxMessagesPerPoll:   1000,
		HealthCheckIntervalMs:         5000,
		BufferFlushIntervalMs:         1000,
		MinScheduledAtFutureMs:        5000,
		SchedulerPollIntervalMs:       1000,
		SchedulerJanitorIntervalMs:    5000,
		SchedulerPollJitterPercent:    10,
		SchedulerJanitorJitterPercent: 10,
		BufferMaxSizeKb:               1000,
		BufferWorkerCount:             2,
		BufferType:                    "memory",
		Datastore:                     "cockroach",
		CockroachURL:                  "postgresql://localhost:26257/test",
		CockroachRegion:               "us-west-2",
		CockroachMaxOpenConns:         10,
		CockroachMaxIdleConns:         5,
		ApiKey:                        "test-key-12345",
		CBMaxFailures:                 5,
		CBTimeout:                     30000,
		CBResetTimeout:                60000,
		HalfOpenMaxReqs:               1,
	}

	validDiskBufferConfig = JSONConfig{
		NumSchedulerNodes:             1,
		NumSchedulerJanitorNodes:      1,
		ProducerPort:                  8080,
		ConsumerPort:                  8081,
		HealthPort:                    8082,
		MsgTimeoutMs:                  30000,
		MaxRetries:                    3,
		MaxPayloadSizeKb:              10240,
		LogLevel:                      "info",
		Region:                        "us-east-1",
		SchedulerMaxMessagesPerPoll:   1000,
		HealthCheckIntervalMs:         5000,
		BufferFlushIntervalMs:         1000,
		MinScheduledAtFutureMs:        5000,
		SchedulerPollIntervalMs:       1000,
		SchedulerJanitorIntervalMs:    5000,
		SchedulerPollJitterPercent:    10,
		SchedulerJanitorJitterPercent: 10,
		BufferMaxSizeKb:               1000,
		BufferWorkerCount:             2,
		BufferType:                    "disk",
		BufferWALPath:                 "/tmp/test.wal",
		Datastore:                     "postgres",
		PostgresURL:                   "postgresql://localhost:5432/test",
		PostgresMaxOpenConns:          10,
		PostgresMaxIdleConns:          5,
		ApiKey:                        "test-key-12345",
		CBMaxFailures:                 5,
		CBTimeout:                     30000,
		CBResetTimeout:                60000,
		HalfOpenMaxReqs:               1,
	}
)

// Helper to create a temporary config file
func createTempConfigFile(t *testing.T, cfg JSONConfig) string {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "config-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Failed to marshal config: %v", err)
	}

	if err := os.WriteFile(tmpFile.Name(), data, 0600); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	t.Cleanup(func() {
		os.Remove(tmpFile.Name())
	})

	return tmpFile.Name()
}

// Helper to set env var and clean up
func setEnv(t *testing.T, key, value string) {
	t.Helper()
	old := os.Getenv(key)
	os.Setenv(key, value)
	t.Cleanup(func() {
		if old == "" {
			os.Unsetenv(key)
		} else {
			os.Setenv(key, old)
		}
	})
}

func resetEnv(t *testing.T) {
	t.Helper()
	os.Clearenv()
}

func TestNewConfig(t *testing.T) {
	t.Run("loads valid minimal config", func(t *testing.T) {
		resetEnv(t)
		configPath := createTempConfigFile(t, validMinimalConfig)
		setEnv(t, ConfigPathKey, configPath)
		setEnv(t, ApiKeyKey, "test-key-12345")

		cfg, err := NewConfig()
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if cfg.ProducerPort != 8080 {
			t.Errorf("Expected ProducerPort 8080, got %d", cfg.ProducerPort)
		}
		if cfg.Datastore != "postgres" {
			t.Errorf("Expected datastore postgres, got %s", cfg.Datastore)
		}
	})

	t.Run("loads cockroach config", func(t *testing.T) {
		resetEnv(t)
		configPath := createTempConfigFile(t, validCockroachConfig)
		setEnv(t, ConfigPathKey, configPath)
		setEnv(t, ApiKeyKey, "test-key-12345")

		cfg, err := NewConfig()
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if cfg.Datastore != "cockroach" {
			t.Errorf("Expected datastore cockroach, got %s", cfg.Datastore)
		}
		if cfg.CockroachRegion != "us-west-2" {
			t.Errorf("Expected region us-west-2, got %s", cfg.CockroachRegion)
		}
	})

	t.Run("loads disk buffer config", func(t *testing.T) {
		resetEnv(t)
		configPath := createTempConfigFile(t, validDiskBufferConfig)
		setEnv(t, ConfigPathKey, configPath)
		setEnv(t, ApiKeyKey, "test-key-12345")
		cfg, err := NewConfig()
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if cfg.BufferType != "disk" {
			t.Errorf("Expected buffer type disk, got %s", cfg.BufferType)
		}
		if cfg.BufferWALPath != "/tmp/test.wal" {
			t.Errorf("Expected WAL path /tmp/test.wal, got %s", cfg.BufferWALPath)
		}
	})

	t.Run("fails with missing config file", func(t *testing.T) {
		resetEnv(t)
		setEnv(t, ConfigPathKey, "/nonexistent/config.json")
		setEnv(t, ApiKeyKey, "test-key-12345")
		_, err := NewConfig()
		if err == nil {
			t.Error("Expected error with missing config file")
		}
		if !strings.Contains(err.Error(), "failed to read config file") {
			t.Errorf("Expected read error, got: %v", err)
		}
	})

	t.Run("fails with invalid JSON", func(t *testing.T) {
		resetEnv(t)
		tmpFile, _ := os.CreateTemp("", "config-*.json")
		os.WriteFile(tmpFile.Name(), []byte("invalid json{"), 0600)
		t.Cleanup(func() { os.Remove(tmpFile.Name()) })

		setEnv(t, ConfigPathKey, tmpFile.Name())

		_, err := NewConfig()
		if err == nil {
			t.Error("Expected error with invalid JSON")
		}
		if !strings.Contains(err.Error(), "failed to parse config file") {
			t.Errorf("Expected parse error, got: %v", err)
		}
	})

	t.Run("generates node ID if not provided", func(t *testing.T) {
		resetEnv(t)
		configPath := createTempConfigFile(t, validMinimalConfig)
		setEnv(t, ConfigPathKey, configPath)
		setEnv(t, ApiKeyKey, "test-key-12345")

		cfg, err := NewConfig()
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if cfg.NodeID == "" {
			t.Error("Expected NodeID to be generated")
		}
	})
}

func TestEnvOverrides(t *testing.T) {
	t.Run("overrides int value", func(t *testing.T) {
		resetEnv(t)
		configPath := createTempConfigFile(t, validMinimalConfig)
		setEnv(t, ConfigPathKey, configPath)
		setEnv(t, ProducerPortKey, "9090")
		setEnv(t, ApiKeyKey, "test-key-12345")

		cfg, err := NewConfig()
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if cfg.ProducerPort != 9090 {
			t.Errorf("Expected ProducerPort 9090, got %d", cfg.ProducerPort)
		}
		if len(cfg.overriddenKeys) == 0 {
			t.Error("Expected override to be tracked")
		}
	})

	t.Run("overrides string value", func(t *testing.T) {
		resetEnv(t)
		configPath := createTempConfigFile(t, validMinimalConfig)
		setEnv(t, ConfigPathKey, configPath)
		setEnv(t, LogLevelKey, "debug")
		setEnv(t, ApiKeyKey, "test-key-12345")

		cfg, err := NewConfig()
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if cfg.LogLevel != "debug" {
			t.Errorf("Expected LogLevel debug, got %s", cfg.LogLevel)
		}
	})

	t.Run("overrides bool value", func(t *testing.T) {
		resetEnv(t)
		configPath := createTempConfigFile(t, validMinimalConfig)
		setEnv(t, ConfigPathKey, configPath)
		setEnv(t, MultiRegionSupplementKey, "true")
		setEnv(t, ApiKeyKey, "test-key-12345")

		cfg, err := NewConfig()
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if !cfg.MultiRegionSupplement {
			t.Error("Expected MultiRegionSupplement to be true")
		}
	})

	t.Run("does not override if value matches JSON", func(t *testing.T) {
		resetEnv(t)
		configPath := createTempConfigFile(t, validMinimalConfig)
		setEnv(t, ConfigPathKey, configPath)
		setEnv(t, ProducerPortKey, "8080") // Same as JSON
		setEnv(t, ApiKeyKey, "test-key-12345")

		cfg, err := NewConfig()
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if len(cfg.overriddenKeys) > 0 {
			t.Error("Expected no overrides when value matches JSON")
		}
	})

	t.Run("ignores invalid int override", func(t *testing.T) {
		resetEnv(t)
		configPath := createTempConfigFile(t, validMinimalConfig)
		setEnv(t, ConfigPathKey, configPath)
		setEnv(t, ProducerPortKey, "invalid")
		setEnv(t, ApiKeyKey, "test-key-12345")

		cfg, err := NewConfig()
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		// Should keep JSON value
		if cfg.ProducerPort != 8080 {
			t.Errorf("Expected ProducerPort 8080, got %d", cfg.ProducerPort)
		}
	})
}

func TestValidate(t *testing.T) {
	t.Run("valid config passes", func(t *testing.T) {
		cfg := configFromJSON(validMinimalConfig)
		if err := cfg.Validate(); err != nil {
			t.Errorf("Expected valid config to pass, got: %v", err)
		}
	})

	t.Run("invalid datastore fails", func(t *testing.T) {
		cfg := configFromJSON(validMinimalConfig)
		cfg.Datastore = "invalid"

		err := cfg.Validate()
		if err == nil {
			t.Error("Expected error with invalid datastore")
		}
	})

	t.Run("invalid port fails", func(t *testing.T) {
		cfg := configFromJSON(validMinimalConfig)
		cfg.ProducerPort = 70000

		err := cfg.Validate()
		if err == nil {
			t.Error("Expected error with invalid port")
		}
	})

	t.Run("missing region fails", func(t *testing.T) {
		cfg := configFromJSON(validMinimalConfig)
		cfg.Region = ""

		err := cfg.Validate()
		if err == nil {
			t.Error("Expected error with missing region")
		}
	})

	t.Run("weak API key fails", func(t *testing.T) {
		cfg := configFromJSON(validMinimalConfig)
		cfg.ApiKey = "weak"

		err := cfg.Validate()
		if err == nil {
			t.Error("Expected error with weak API key")
		}
	})

	t.Run("disk buffer without WAL path fails", func(t *testing.T) {
		cfg := configFromJSON(validMinimalConfig)
		cfg.BufferType = "disk"
		cfg.BufferWALPath = ""

		err := cfg.Validate()
		if err == nil {
			t.Error("Expected error with disk buffer and no WAL path")
		}
	})

	t.Run("adaptive buffer with invalid min/max fails", func(t *testing.T) {
		cfg := configFromJSON(validMinimalConfig)
		cfg.BufferAdaptive = true
		cfg.BufferAdaptiveMinSize = 1000
		cfg.BufferAdaptiveMaxSize = 500 // Min > Max

		err := cfg.Validate()
		if err == nil {
			t.Error("Expected error when adaptive min > max")
		}
	})
}

// Helper to create Config from JSONConfig for testing
func configFromJSON(jsonCfg JSONConfig) *Config {
	return &Config{
		overriddenKeys:                make(map[string]string),
		NumSchedulerNodes:             jsonCfg.NumSchedulerNodes,
		NumSchedulerJanitorNodes:      jsonCfg.NumSchedulerJanitorNodes,
		SchedulerPollIntervalMs:       jsonCfg.SchedulerPollIntervalMs,
		SchedulerJanitorIntervalMs:    jsonCfg.SchedulerJanitorIntervalMs,
		SchedulerMaxMessagesPerPoll:   jsonCfg.SchedulerMaxMessagesPerPoll,
		SchedulerPollJitterPercent:    jsonCfg.SchedulerPollJitterPercent,
		SchedulerJanitorJitterPercent: jsonCfg.SchedulerJanitorJitterPercent,
		ProducerPort:                  jsonCfg.ProducerPort,
		ConsumerPort:                  jsonCfg.ConsumerPort,
		HealthPort:                    jsonCfg.HealthPort,
		MsgTimeoutMs:                  jsonCfg.MsgTimeoutMs,
		MaxRetries:                    jsonCfg.MaxRetries,
		MaxPayloadSizeKb:              jsonCfg.MaxPayloadSizeKb,
		MinScheduledAtFutureMs:        jsonCfg.MinScheduledAtFutureMs,
		HealthCheckIntervalMs:         jsonCfg.HealthCheckIntervalMs,
		LogLevel:                      jsonCfg.LogLevel,
		BufferType:                    jsonCfg.BufferType,
		BufferWALPath:                 jsonCfg.BufferWALPath,
		BufferFlushIntervalMs:         jsonCfg.BufferFlushIntervalMs,
		BufferMaxSizeKb:               jsonCfg.BufferMaxSizeKb,
		BufferWorkerCount:             jsonCfg.BufferWorkerCount,
		BufferAdaptive:                jsonCfg.BufferAdaptive,
		BufferAdaptiveMaxSize:         jsonCfg.BufferAdaptiveMaxSize,
		BufferAdaptiveTuneThreshold:   jsonCfg.BufferAdaptiveTuneThreshold,
		BufferAdaptiveMinSize:         jsonCfg.BufferAdaptiveMinSize,
		MultiRegionSupplement:         jsonCfg.MultiRegionSupplement,
		MultiRegionScheduler:          jsonCfg.MultiRegionScheduler,
		MultiRegionJanitor:            jsonCfg.MultiRegionJanitor,
		Region:                        jsonCfg.Region,
		JanitorDeleteFailedMessages:   jsonCfg.JanitorDeleteFailedMessages,
		Datastore:                     jsonCfg.Datastore,
		PostgresURL:                   jsonCfg.PostgresURL,
		CockroachURL:                  jsonCfg.CockroachURL,
		CockroachRegion:               jsonCfg.CockroachRegion,
		PostgresMaxOpenConns:          jsonCfg.PostgresMaxOpenConns,
		PostgresMaxIdleConns:          jsonCfg.PostgresMaxIdleConns,
		CockroachMaxOpenConns:         jsonCfg.CockroachMaxOpenConns,
		CockroachMaxIdleConns:         jsonCfg.CockroachMaxIdleConns,
		ApiKey:                        jsonCfg.ApiKey,
		CBMaxFailures:                 jsonCfg.CBMaxFailures,
		CBTimeout:                     jsonCfg.CBTimeout,
		CBResetTimeout:                jsonCfg.CBResetTimeout,
		HalfOpenMaxReqs:               jsonCfg.HalfOpenMaxReqs,
		NodeID:                        "test-node-id",
	}
}
