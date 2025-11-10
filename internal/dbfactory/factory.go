package dbfactory

import (
	"fmt"

	"github.com/jaysongiroux/smq/internal/config"
	"github.com/jaysongiroux/smq/internal/db"
	"github.com/jaysongiroux/smq/internal/db/cockroach"
	"github.com/jaysongiroux/smq/internal/db/postgres"
	"github.com/jaysongiroux/smq/internal/logger"
)

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

func newCockroachStore(cfg *config.Config, log *logger.Logger) (db.Store, error) {
	dbConfig := &db.PGConfig{
		ConnectionString:            cfg.CockroachURL,
		MaxOpenConns:                cfg.CockroachMaxOpenConns,
		MaxIdleConns:                cfg.CockroachMaxIdleConns,
		Region:                      &cfg.CockroachRegion,
		MaxRetries:                  cfg.MaxRetries,
		JanitorDeleteFailedMessages: cfg.JanitorDeleteFailedMessages,
		MultiRegionSupplement:       cfg.MultiRegionSupplement,
		MultiRegionScheduler:        cfg.MultiRegionScheduler,
		MultiRegionJanitor:          cfg.MultiRegionJanitor,
		MaxMessagesPerPoll:          cfg.SchedulerMaxMessagesPerPoll,
	}

	return cockroach.NewCockroachStore(dbConfig, log)
}
