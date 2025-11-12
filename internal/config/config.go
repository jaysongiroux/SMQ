package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"

	"github.com/google/uuid"
)

// Config represents the application configuration with strongly-typed fields
type Config struct {
	// Scheduler configuration
	NumSchedulerNodes             int
	NumSchedulerJanitorNodes      int
	SchedulerPollIntervalMs       int
	SchedulerJanitorIntervalMs    int
	SchedulerMaxMessagesPerPoll   int
	SchedulerPollJitterPercent    int
	SchedulerJanitorJitterPercent int

	// Server ports
	ProducerPort int
	ConsumerPort int
	HealthPort   int

	// Message handling
	MsgTimeoutMs           int
	MaxRetries             int
	MaxPayloadSizeKb       int
	MinScheduledAtFutureMs int

	// Health and monitoring
	HealthCheckIntervalMs int
	LogLevel              string

	// Buffer configuration
	BufferType            string
	BufferWALPath         string
	BufferFlushIntervalMs int
	BufferMaxSizeKb       int
	BufferWorkerCount     int

	// Multi-region support
	MultiRegionSupplement bool
	MultiRegionScheduler  bool
	MultiRegionJanitor    bool
	Region                string

	// Janitor configuration
	JanitorDeleteFailedMessages bool

	// Database configuration
	Datastore             string
	PostgresURL           string
	CockroachURL          string
	CockroachRegion       string
	PostgresMaxOpenConns  int
	PostgresMaxIdleConns  int
	CockroachMaxOpenConns int
	CockroachMaxIdleConns int

	// API security
	ApiKey string

	// Internal tracking for overrides
	overriddenKeys map[string]string

	// Internal tracking for config path
	ConfigPath string
	NodeID     string
}

// define all the config keys as constants
const (
	ConfigPathKey                    = "SMQ_CONFIG_PATH"
	BufferMaxSizeKbKey               = "buffer_max_size_kb"
	BufferFlushIntervalKey           = "buffer_flush_interval_ms"
	BufferWorkerCountKey             = "num_buffer_nodes"
	MsgTimeoutMsKey                  = "msg_timeout_ms"
	SchedulerPollIntervalKey         = "scheduler_poll_interval_ms"
	SchedulerJanitorIntervalKey      = "scheduler_janitor_interval_ms"
	NumSchedulerNodesKey             = "num_scheduler_nodes"
	SchedulerMaxMessagesPerPollKey   = "scheduler_max_messages_per_poll"
	NumSchedulerJanitorNodesKey      = "num_scheduler_janitor_nodes"
	ProducerPortKey                  = "producer_port"
	ConsumerPortKey                  = "consumer_port"
	HealthPortKey                    = "health_port"
	HealthCheckIntervalKey           = "health_check_interval_ms"
	SchedulerPollJitterPercentKey    = "scheduler_poll_jitter_percent"
	SchedulerJanitorJitterPercentKey = "scheduler_janitor_jitter_percent"
	JanitorDeleteFailedMessagesKey   = "janitor_delete_failed_messages"
	BufferTypeKey                    = "buffer_type"
	BufferWALPathKey                 = "buffer_wal_path"
	MultiRegionSupplementKey         = "multi_region_supplement"
	MultiRegionSchedulerKey          = "multi_region_scheduler"
	MultiRegionJanitorKey            = "multi_region_janitor"
	RegionKey                        = "region"
	MaxRetriesKey                    = "max_retries"
	MaxPayloadSizeKbKey              = "max_payload_size_kb"
	LogLevelKey                      = "log_level"
	MinScheduledAtFutureMsKey        = "min_scheduled_at_future_ms"
	DatastoreKey                     = "datastore"
	PostgresURLKey                   = "postgres_url"
	CockroachURLKey                  = "cockroach_url"
	CockroachRegionKey               = "cockroach_region"
	PostgresMaxOpenConnsKey          = "postgres_max_open_conns"
	PostgresMaxIdleConnsKey          = "postgres_max_idle_conns"
	CockroachMaxOpenConnsKey         = "cockroach_max_open_conns"
	CockroachMaxIdleConnsKey         = "cockroach_max_idle_conns"
	NodeIDKey                        = "node_id"
	ApiKeyKey                        = "api_key"
)

const (
	LogLevelDebug = "debug"
	LogLevelInfo  = "info"
	LogLevelWarn  = "warn"
	LogLevelError = "error"
)

const (
	DatastorePostgres  = "postgres"
	DatastoreCockroach = "cockroach"
)

const (
	BufferTypeMemory = "memory"
	BufferTypeDisk   = "disk"
)

// JSONConfig represents the structure of config.json
type JSONConfig struct {
	NumSchedulerNodes             int    `json:"num_scheduler_nodes"`
	NumSchedulerJanitorNodes      int    `json:"num_scheduler_janitor_nodes"`
	ProducerPort                  int    `json:"producer_port"`
	ConsumerPort                  int    `json:"consumer_port"`
	HealthPort                    int    `json:"health_port"`
	MsgTimeoutMs                  int    `json:"msg_timeout_ms"`
	MaxRetries                    int    `json:"max_retries"`
	MaxPayloadSizeKb              int    `json:"max_payload_size_kb"`
	LogLevel                      string `json:"log_level"`
	Region                        string `json:"region"`
	SchedulerMaxMessagesPerPoll   int    `json:"scheduler_max_messages_per_poll"`
	HealthCheckIntervalMs         int    `json:"health_check_interval_ms"`
	BufferFlushIntervalMs         int    `json:"buffer_flush_interval_ms"`
	MinScheduledAtFutureMs        int    `json:"min_scheduled_at_future_ms"`
	SchedulerPollIntervalMs       int    `json:"scheduler_poll_interval_ms"`
	SchedulerJanitorIntervalMs    int    `json:"scheduler_janitor_interval_ms"`
	SchedulerPollJitterPercent    int    `json:"scheduler_poll_jitter_percent"`
	SchedulerJanitorJitterPercent int    `json:"scheduler_janitor_jitter_percent"`
	JanitorDeleteFailedMessages   bool   `json:"janitor_delete_failed_messages"`
	BufferMaxSizeKb               int    `json:"buffer_max_size_kb"`
	BufferWorkerCount             int    `json:"num_buffer_nodes"`
	BufferType                    string `json:"buffer_type"`
	BufferWALPath                 string `json:"buffer_wal_path"`
	MultiRegionSupplement         bool   `json:"multi_region_supplement"`
	MultiRegionScheduler          bool   `json:"multi_region_scheduler"`
	MultiRegionJanitor            bool   `json:"multi_region_janitor"`
	PostgresURL                   string `json:"postgres_url"`
	PostgresMaxOpenConns          int    `json:"postgres_max_open_conns"`
	PostgresMaxIdleConns          int    `json:"postgres_max_idle_conns"`
	CockroachURL                  string `json:"cockroach_url"`
	CockroachMaxOpenConns         int    `json:"cockroach_max_open_conns"`
	CockroachMaxIdleConns         int    `json:"cockroach_max_idle_conns"`
	CockroachRegion               string `json:"cockroach_region"`
	Datastore                     string `json:"datastore"`
	ConfigPath                    string `json:"config_path"`
	NodeID                        string `json:"node_id"`
	ApiKey                        string `json:"api_key"`
}

// NewConfig creates a new config instance by loading config.json and applying environment overrides
func NewConfig() (*Config, error) {
	// get config path from env var. default is ./config.json
	configPath := os.Getenv(ConfigPathKey)
	if configPath == "" {
		configPath = "./config.json"
	}

	// Load JSON config file
	file, err := os.ReadFile(filepath.Clean(configPath))
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var jsonCfg JSONConfig
	if err := json.Unmarshal(file, &jsonCfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	nodeID := os.Getenv(NodeIDKey)
	if nodeID == "" {
		nodeID = uuid.New().String()
	}

	// Initialize config with JSON values
	cfg := &Config{
		overriddenKeys: make(map[string]string),

		// Scheduler
		NumSchedulerNodes:             jsonCfg.NumSchedulerNodes,
		NumSchedulerJanitorNodes:      jsonCfg.NumSchedulerJanitorNodes,
		SchedulerPollIntervalMs:       jsonCfg.SchedulerPollIntervalMs,
		SchedulerJanitorIntervalMs:    jsonCfg.SchedulerJanitorIntervalMs,
		SchedulerMaxMessagesPerPoll:   jsonCfg.SchedulerMaxMessagesPerPoll,
		SchedulerPollJitterPercent:    jsonCfg.SchedulerPollJitterPercent,
		SchedulerJanitorJitterPercent: jsonCfg.SchedulerJanitorJitterPercent,

		// Ports
		ProducerPort: jsonCfg.ProducerPort,
		ConsumerPort: jsonCfg.ConsumerPort,
		HealthPort:   jsonCfg.HealthPort,

		// Message handling
		MsgTimeoutMs:           jsonCfg.MsgTimeoutMs,
		MaxRetries:             jsonCfg.MaxRetries,
		MaxPayloadSizeKb:       jsonCfg.MaxPayloadSizeKb,
		MinScheduledAtFutureMs: jsonCfg.MinScheduledAtFutureMs,

		// Health and monitoring
		HealthCheckIntervalMs: jsonCfg.HealthCheckIntervalMs,
		LogLevel:              jsonCfg.LogLevel,

		// Buffer
		BufferType:            jsonCfg.BufferType,
		BufferWALPath:         jsonCfg.BufferWALPath,
		BufferFlushIntervalMs: jsonCfg.BufferFlushIntervalMs,
		BufferMaxSizeKb:       jsonCfg.BufferMaxSizeKb,
		BufferWorkerCount:     jsonCfg.BufferWorkerCount,

		// Multi-region
		MultiRegionSupplement: jsonCfg.MultiRegionSupplement,
		MultiRegionScheduler:  jsonCfg.MultiRegionScheduler,
		MultiRegionJanitor:    jsonCfg.MultiRegionJanitor,
		Region:                jsonCfg.Region,

		// Janitor
		JanitorDeleteFailedMessages: jsonCfg.JanitorDeleteFailedMessages,

		// Database
		Datastore:             jsonCfg.Datastore,
		PostgresURL:           jsonCfg.PostgresURL,
		CockroachURL:          jsonCfg.CockroachURL,
		CockroachRegion:       jsonCfg.CockroachRegion,
		PostgresMaxOpenConns:  jsonCfg.PostgresMaxOpenConns,
		PostgresMaxIdleConns:  jsonCfg.PostgresMaxIdleConns,
		CockroachMaxOpenConns: jsonCfg.CockroachMaxOpenConns,
		CockroachMaxIdleConns: jsonCfg.CockroachMaxIdleConns,

		// Internal tracking for config path
		ConfigPath: configPath,
		NodeID:     nodeID,
	}

	// Apply environment variable overrides
	cfg.applyEnvOverrides(&jsonCfg)

	// Validate the final configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	// Log any overrides
	cfg.LogOverrides()

	return cfg, nil
}

// applyEnvOverrides checks for environment variable overrides and applies them
func (c *Config) applyEnvOverrides(jsonCfg *JSONConfig) {
	// Helper to override int values
	overrideInt := func(field *int, envKey string, jsonVal int) {
		if envVal := os.Getenv(envKey); envVal != "" {
			if intVal, err := strconv.Atoi(envVal); err == nil {
				if intVal != jsonVal {
					c.overriddenKeys[envKey] = envVal
					*field = intVal
				}
			}
		}
	}

	// Helper to override string values
	overrideString := func(field *string, envKey string, jsonVal string) {
		if envVal := os.Getenv(envKey); envVal != "" {
			if envVal != jsonVal {
				c.overriddenKeys[envKey] = envVal
				*field = envVal
			}
		}
	}

	// Helper to override bool values
	overrideBool := func(field *bool, envKey string, jsonVal bool) {
		if envVal := os.Getenv(envKey); envVal != "" {
			if boolVal, err := strconv.ParseBool(envVal); err == nil {
				if boolVal != jsonVal {
					c.overriddenKeys[envKey] = envVal
					*field = boolVal
				}
			}
		}
	}

	// Apply overrides for all fields
	overrideInt(&c.NumSchedulerNodes, NumSchedulerNodesKey, jsonCfg.NumSchedulerNodes)
	overrideInt(&c.NumSchedulerJanitorNodes, NumSchedulerJanitorNodesKey, jsonCfg.NumSchedulerJanitorNodes)
	overrideInt(&c.SchedulerPollIntervalMs, SchedulerPollIntervalKey, jsonCfg.SchedulerPollIntervalMs)
	overrideInt(&c.SchedulerJanitorIntervalMs, SchedulerJanitorIntervalKey, jsonCfg.SchedulerJanitorIntervalMs)
	overrideInt(&c.SchedulerMaxMessagesPerPoll, SchedulerMaxMessagesPerPollKey, jsonCfg.SchedulerMaxMessagesPerPoll)
	overrideInt(&c.SchedulerPollJitterPercent, SchedulerPollJitterPercentKey, jsonCfg.SchedulerPollJitterPercent)
	overrideInt(&c.SchedulerJanitorJitterPercent, SchedulerJanitorJitterPercentKey, jsonCfg.SchedulerJanitorJitterPercent)
	overrideInt(&c.ProducerPort, ProducerPortKey, jsonCfg.ProducerPort)
	overrideInt(&c.ConsumerPort, ConsumerPortKey, jsonCfg.ConsumerPort)
	overrideInt(&c.HealthPort, HealthPortKey, jsonCfg.HealthPort)
	overrideInt(&c.MsgTimeoutMs, MsgTimeoutMsKey, jsonCfg.MsgTimeoutMs)
	overrideInt(&c.MaxRetries, MaxRetriesKey, jsonCfg.MaxRetries)
	overrideInt(&c.MaxPayloadSizeKb, MaxPayloadSizeKbKey, jsonCfg.MaxPayloadSizeKb)
	overrideInt(&c.MinScheduledAtFutureMs, MinScheduledAtFutureMsKey, jsonCfg.MinScheduledAtFutureMs)
	overrideInt(&c.HealthCheckIntervalMs, HealthCheckIntervalKey, jsonCfg.HealthCheckIntervalMs)
	overrideInt(&c.BufferFlushIntervalMs, BufferFlushIntervalKey, jsonCfg.BufferFlushIntervalMs)
	overrideInt(&c.BufferMaxSizeKb, BufferMaxSizeKbKey, jsonCfg.BufferMaxSizeKb)
	overrideInt(&c.BufferWorkerCount, BufferWorkerCountKey, jsonCfg.BufferWorkerCount)
	overrideString(&c.LogLevel, LogLevelKey, jsonCfg.LogLevel)
	overrideString(&c.BufferType, BufferTypeKey, jsonCfg.BufferType)
	overrideString(&c.BufferWALPath, BufferWALPathKey, jsonCfg.BufferWALPath)
	overrideString(&c.Region, RegionKey, jsonCfg.Region)
	overrideString(&c.Datastore, DatastoreKey, jsonCfg.Datastore)
	overrideString(&c.PostgresURL, PostgresURLKey, jsonCfg.PostgresURL)
	overrideString(&c.CockroachURL, CockroachURLKey, jsonCfg.CockroachURL)
	overrideString(&c.CockroachRegion, CockroachRegionKey, jsonCfg.CockroachRegion)
	overrideBool(&c.MultiRegionSupplement, MultiRegionSupplementKey, jsonCfg.MultiRegionSupplement)
	overrideBool(&c.MultiRegionScheduler, MultiRegionSchedulerKey, jsonCfg.MultiRegionScheduler)
	overrideBool(&c.MultiRegionJanitor, MultiRegionJanitorKey, jsonCfg.MultiRegionJanitor)
	overrideBool(&c.JanitorDeleteFailedMessages, JanitorDeleteFailedMessagesKey, jsonCfg.JanitorDeleteFailedMessages)
	overrideInt(&c.PostgresMaxOpenConns, PostgresMaxOpenConnsKey, jsonCfg.PostgresMaxOpenConns)
	overrideInt(&c.PostgresMaxIdleConns, PostgresMaxIdleConnsKey, jsonCfg.PostgresMaxIdleConns)
	overrideInt(&c.CockroachMaxOpenConns, CockroachMaxOpenConnsKey, jsonCfg.CockroachMaxOpenConns)
	overrideInt(&c.CockroachMaxIdleConns, CockroachMaxIdleConnsKey, jsonCfg.CockroachMaxIdleConns)
	overrideString(&c.NodeID, NodeIDKey, jsonCfg.NodeID)
	overrideString(&c.ApiKey, ApiKeyKey, jsonCfg.ApiKey)

	// API key is env-only
	if apiKey := os.Getenv("api_key"); apiKey != "" {
		c.ApiKey = apiKey
	}
}

// LogOverrides prints all configuration keys that were overridden by environment variables
func (c *Config) LogOverrides() {
	if len(c.overriddenKeys) == 0 {
		return
	}

	fmt.Println("⚠️  Configuration overrides detected:")
	for key, envValue := range c.overriddenKeys {
		// keys that are normally overridden by environment variables
		sensitiveKeys := []string{ApiKeyKey, PostgresURLKey, CockroachURLKey}
		// log all other keys
		if slices.Contains(sensitiveKeys, key) {
			continue
		}
		fmt.Printf("   • %s: overridden by environment variable (value: %s)\n", key, envValue)
	}
	fmt.Println()
}

func (c *Config) Validate() error {
	// Validate datastore
	if err := ValidateNonEmpty(DatastoreKey, c.Datastore); err != nil {
		return err
	}
	if err := ValidateOneOf(DatastoreKey, c.Datastore, []string{DatastorePostgres, DatastoreCockroach}); err != nil {
		return err
	}

	// Validate datastore-specific config
	validators := map[string]func() error{
		DatastorePostgres:  c.validatePostgresConfig,
		DatastoreCockroach: c.validateCockroachConfig,
	}
	if validator, ok := validators[c.Datastore]; ok {
		if err := validator(); err != nil {
			return fmt.Errorf("%s config error: %w", c.Datastore, err)
		}
	}

	// Validate scheduler
	minNumSchedulerNodes := 1
	minNumSchedulerJanitorNodes := 1
	if err := ValidateMinValue(NumSchedulerNodesKey, c.NumSchedulerNodes, minNumSchedulerNodes); err != nil {
		return err
	}
	if err := ValidateMinValue(NumSchedulerJanitorNodesKey, c.NumSchedulerJanitorNodes, minNumSchedulerJanitorNodes); err != nil {
		return err
	}

	// Validate ports
	if err := ValidatePort(ProducerPortKey, c.ProducerPort); err != nil {
		return err
	}
	if err := ValidatePort(ConsumerPortKey, c.ConsumerPort); err != nil {
		return err
	}
	if err := ValidatePort(HealthPortKey, c.HealthPort); err != nil {
		return err
	}

	// Validate other numeric fields
	minMsgTimeoutMs := 1000
	minMaxRetries := 0
	minMaxPayloadSizeKb := 1
	if err := ValidateMinValue(MsgTimeoutMsKey, c.MsgTimeoutMs, minMsgTimeoutMs); err != nil {
		return err
	}
	if err := ValidateMinValue(MaxRetriesKey, c.MaxRetries, minMaxRetries); err != nil {
		return err
	}
	if err := ValidateMinValue(MaxPayloadSizeKbKey, c.MaxPayloadSizeKb, minMaxPayloadSizeKb); err != nil {
		return err
	}

	// Validate strings
	if err := ValidateNonEmpty(RegionKey, c.Region); err != nil {
		return err
	}
	if err := ValidateNonEmpty("api_key", c.ApiKey); err != nil {
		return err
	}
	if err := ValidateApiKey("api_key", c.ApiKey); err != nil {
		return err
	}
	if err := ValidateOneOf(LogLevelKey, c.LogLevel,
		[]string{LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError}); err != nil {
		return err
	}
	if err := ValidateOneOf(BufferTypeKey, c.BufferType, []string{BufferTypeMemory, BufferTypeDisk}); err != nil {
		return err
	}

	if c.BufferType == BufferTypeDisk {
		if err := ValidateNonEmpty(BufferWALPathKey, c.BufferWALPath); err != nil {
			return err
		}
	}

	// Validate intervals with minimums
	minHealthCheckIntervalMs := 1000
	minBufferFlushIntervalMs := 1000
	minMinScheduledAtFutureMs := 5000
	minSchedulerPollIntervalMs := 500
	minSchedulerJanitorIntervalMs := 1000

	if err := ValidateMinValue(HealthCheckIntervalKey, c.HealthCheckIntervalMs, minHealthCheckIntervalMs); err != nil {
		return err
	}
	if err := ValidateMinValue(BufferFlushIntervalKey, c.BufferFlushIntervalMs, minBufferFlushIntervalMs); err != nil {
		return err
	}
	if err := ValidateMinValue(MinScheduledAtFutureMsKey, c.MinScheduledAtFutureMs, minMinScheduledAtFutureMs); err != nil {
		return err
	}
	if err := ValidateMinValue(SchedulerPollIntervalKey, c.SchedulerPollIntervalMs, minSchedulerPollIntervalMs); err != nil {
		return err
	}
	if err := ValidateMinValue(SchedulerJanitorIntervalKey, c.SchedulerJanitorIntervalMs, minSchedulerJanitorIntervalMs); err != nil {
		return err
	}

	// Validate jitter percentages
	minJitterPercent := 5
	maxJitterPercent := 100
	if err := ValidateRange(SchedulerPollJitterPercentKey, c.SchedulerPollJitterPercent, minJitterPercent, maxJitterPercent); err != nil {
		return err
	}
	if err := ValidateRange(SchedulerJanitorJitterPercentKey, c.SchedulerJanitorJitterPercent, minJitterPercent, maxJitterPercent); err != nil {
		return err
	}

	// validate ranges
	minBufferMaxSizeKb := 10
	maxBufferMaxSizeKb := 1000000
	if err := ValidateRange(BufferMaxSizeKbKey, c.BufferMaxSizeKb, minBufferMaxSizeKb, maxBufferMaxSizeKb); err != nil {
		return err
	}
	minBufferWorkerCount := 1
	maxBufferWorkerCount := 100
	if err := ValidateRange(BufferWorkerCountKey, c.BufferWorkerCount, minBufferWorkerCount, maxBufferWorkerCount); err != nil {
		return err
	}

	minSchedulerMaxMessagesPerPoll := 100
	maxSchedulerMaxMessagesPerPoll := 1000000
	if err := ValidateRange(SchedulerMaxMessagesPerPollKey, c.SchedulerMaxMessagesPerPoll, minSchedulerMaxMessagesPerPoll, maxSchedulerMaxMessagesPerPoll); err != nil {
		return err
	}

	return nil
}

// validatePostgresConfig validates PostgreSQL specific configuration
func (c *Config) validatePostgresConfig() error {
	if c.PostgresURL == "" {
		return fmt.Errorf("%s is not set", PostgresURLKey)
	}

	// validate open and idle connections
	if err := ValidateRange(PostgresMaxOpenConnsKey, c.PostgresMaxOpenConns, 1, 100); err != nil {
		return err
	}
	if err := ValidateRange(PostgresMaxIdleConnsKey, c.PostgresMaxIdleConns, 1, 100); err != nil {
		return err
	}

	return nil
}

// validateCockroachConfig validates CockroachDB specific configuration
func (c *Config) validateCockroachConfig() error {
	if c.CockroachURL == "" {
		return fmt.Errorf("%s is not set", CockroachURLKey)
	}

	// validate region
	if c.CockroachRegion == "" {
		return fmt.Errorf("%s is not set", CockroachRegionKey)
	}

	// validate open and idle connections
	if err := ValidateRange(CockroachMaxOpenConnsKey, c.CockroachMaxOpenConns, 1, 100); err != nil {
		return err
	}
	if err := ValidateRange(CockroachMaxIdleConnsKey, c.CockroachMaxIdleConns, 1, 100); err != nil {
		return err
	}

	return nil
}
