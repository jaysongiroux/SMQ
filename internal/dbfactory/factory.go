package dbfactory

import (
	"fmt"

	"github.com/jaysongiroux/smq/internal/config"
	"github.com/jaysongiroux/smq/internal/db"
	"github.com/jaysongiroux/smq/internal/db/cockroach"
	"github.com/jaysongiroux/smq/internal/db/postgres"
	"github.com/jaysongiroux/smq/internal/logger"
)

// NewStore creates a new database store based on the configuration
// It reads the "datastore" value from config and returns the appropriate implementation
func NewStore(cfg *config.Config, log *logger.Logger) (db.Store, error) {
	switch cfg.Datastore {
	case config.DatastorePostgres:
		return newPostgresStore(cfg, log)
	case config.DatastoreCockroach:
		return newCockroachStore(cfg, log)
	default:
		return nil, fmt.Errorf("unsupported datastore: %s", cfg.Datastore)
	}
}

// newPostgresStore creates a PostgreSQL store from config
func newPostgresStore(cfg *config.Config, log *logger.Logger) (db.Store, error) {
	dbConfig := &db.PGConfig{
		ConnectionString:            cfg.PostgresURL,
		MaxOpenConns:                cfg.PostgresMaxOpenConns,
		MaxIdleConns:                cfg.PostgresMaxIdleConns,
		Region:                      nil,
		MaxRetries:                  cfg.MaxRetries,
		JanitorDeleteFailedMessages: cfg.JanitorDeleteFailedMessages,
		MaxMessagesPerPoll:          cfg.SchedulerMaxMessagesPerPoll,
	}

	return postgres.NewPostgresStore(dbConfig, log)
}

// newCockroachStore creates a CockroachDB store from config
func newCockroachStore(cfg *config.Config, log *logger.Logger) (db.Store, error) {
	// Get region and throw error if not set
	region := cfg.CockroachRegion
	if region == "" {
		return nil, fmt.Errorf("cockroach_region is not set")
	}

	dbConfig := &db.PGConfig{
		ConnectionString:            cfg.CockroachURL,
		MaxOpenConns:                cfg.CockroachMaxOpenConns,
		MaxIdleConns:                cfg.CockroachMaxIdleConns,
		Region:                      &region,
		MaxRetries:                  cfg.MaxRetries,
		JanitorDeleteFailedMessages: cfg.JanitorDeleteFailedMessages,
		MultiRegionSupplement:       cfg.MultiRegionSupplement,
		MultiRegionScheduler:        cfg.MultiRegionScheduler,
		MultiRegionJanitor:          cfg.MultiRegionJanitor,
		MaxMessagesPerPoll:          cfg.SchedulerMaxMessagesPerPoll,
	}

	return cockroach.NewCockroachStore(dbConfig, log)
}
