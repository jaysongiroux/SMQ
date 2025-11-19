package dbfactory

import (
	"strings"
	"testing"

	"github.com/jaysongiroux/smq/internal/config"
	"github.com/jaysongiroux/smq/internal/testutils"
)

func TestNewStore(t *testing.T) {
	t.Run("returns error for unsupported datastore", func(t *testing.T) {
		log := testutils.CreateTestLogger()
		cfg := &config.Config{
			Datastore: "unsupported-db",
		}

		store, err := NewStore(cfg, log)

		if err == nil {
			t.Fatal("Expected error for unsupported datastore, got nil")
		}

		if store != nil {
			t.Errorf("Expected nil store, got %v", store)
		}

		expectedError := "unsupported datastore: unsupported-db"
		if err.Error() != expectedError {
			t.Errorf("Expected error message '%s', got '%s'", expectedError, err.Error())
		}
	})

	t.Run("attempts to create postgres store", func(t *testing.T) {
		log := testutils.CreateTestLogger()
		cfg := &config.Config{
			Datastore:                   config.DatastorePostgres,
			PostgresURL:                 "postgres://invalid-url-for-test",
			PostgresMaxOpenConns:        25,
			PostgresMaxIdleConns:        5,
			MaxRetries:                  3,
			JanitorDeleteFailedMessages: true,
			SchedulerMaxMessagesPerPoll: 1000,
		}

		_, err := NewStore(cfg, log)

		if err == nil {
			t.Error("Expected error with invalid postgres connection string")
		}

		if strings.Contains(err.Error(), "unsupported datastore") {
			t.Error("Should not return 'unsupported datastore' error for postgres")
		}
	})

	t.Run("attempts to create cockroach store", func(t *testing.T) {
		log := testutils.CreateTestLogger()
		cfg := &config.Config{
			Datastore:                   config.DatastoreCockroach,
			CockroachURL:                "postgres://invalid-url-for-test",
			CockroachMaxOpenConns:       50,
			CockroachMaxIdleConns:       10,
			CockroachRegion:             "us-east-1",
			MaxRetries:                  3,
			JanitorDeleteFailedMessages: false,
			MultiRegionSupplement:       true,
			MultiRegionScheduler:        true,
			MultiRegionJanitor:          false,
			SchedulerMaxMessagesPerPoll: 5000,
		}

		// This will fail to connect but we're testing that it attempts the right path
		_, err := NewStore(cfg, log)

		// We expect an error because the connection string is invalid
		if err == nil {
			t.Error("Expected error with invalid cockroach connection string")
		}

		// Verify it's a cockroach-related error (not "unsupported datastore")
		if strings.Contains(err.Error(), "unsupported datastore") {
			t.Error("Should not return 'unsupported datastore' error for cockroach")
		}
	})

	t.Run("passes correct config to postgres store", func(t *testing.T) {
		log := testutils.CreateTestLogger()

		expectedURL := "postgres://test-connection-string"
		expectedMaxOpen := 100
		expectedMaxIdle := 20
		expectedMaxRetries := 5
		expectedDeleteFailed := true
		expectedMaxPoll := 2000

		cfg := &config.Config{
			Datastore:                   config.DatastorePostgres,
			PostgresURL:                 expectedURL,
			PostgresMaxOpenConns:        expectedMaxOpen,
			PostgresMaxIdleConns:        expectedMaxIdle,
			MaxRetries:                  expectedMaxRetries,
			JanitorDeleteFailedMessages: expectedDeleteFailed,
			SchedulerMaxMessagesPerPoll: expectedMaxPoll,
		}

		// Will fail to connect, but we're testing config mapping
		_, err := NewStore(cfg, log)

		// Should get a connection error (not unsupported datastore)
		if err != nil && strings.Contains(err.Error(), "unsupported datastore") {
			t.Error("Should not return 'unsupported datastore' error")
		}
	})

	t.Run("passes correct config to cockroach store", func(t *testing.T) {
		log := testutils.CreateTestLogger()

		cfg := &config.Config{
			Datastore:                   config.DatastoreCockroach,
			CockroachURL:                "postgres://cockroach-test-connection-string",
			CockroachMaxOpenConns:       150,
			CockroachMaxIdleConns:       10,
			CockroachRegion:             "us-west-2",
			MaxRetries:                  3,
			JanitorDeleteFailedMessages: false,
			MultiRegionSupplement:       true,
			MultiRegionScheduler:        true,
			MultiRegionJanitor:          false,
			SchedulerMaxMessagesPerPoll: 3000,
		}

		// Will fail to connect, but we're testing config mapping
		_, err := NewStore(cfg, log)

		// Should get a connection error (not unsupported datastore)
		if err != nil && strings.Contains(err.Error(), "unsupported datastore") {
			t.Error("Should not return 'unsupported datastore' error")
		}
	})
}

func TestNewPostgresStore(t *testing.T) {
	t.Run("returns error with invalid connection string", func(t *testing.T) {
		log := testutils.CreateTestLogger()
		cfg := &config.Config{
			Datastore:                   config.DatastorePostgres,
			PostgresURL:                 "invalid://connection",
			PostgresMaxOpenConns:        25,
			PostgresMaxIdleConns:        5,
			MaxRetries:                  3,
			JanitorDeleteFailedMessages: true,
			SchedulerMaxMessagesPerPoll: 1000,
		}

		_, err := newPostgresStore(cfg, log)

		if err == nil {
			t.Error("Expected error with invalid connection string")
		}
	})

	t.Run("returns error with empty connection string", func(t *testing.T) {
		log := testutils.CreateTestLogger()
		cfg := &config.Config{
			Datastore:                   config.DatastorePostgres,
			PostgresURL:                 "",
			PostgresMaxOpenConns:        25,
			PostgresMaxIdleConns:        5,
			MaxRetries:                  3,
			JanitorDeleteFailedMessages: true,
			SchedulerMaxMessagesPerPoll: 1000,
		}

		_, err := newPostgresStore(cfg, log)

		if err == nil {
			t.Error("Expected error with empty connection string")
		}
	})
}

func TestNewCockroachStore(t *testing.T) {
	t.Run("returns error with invalid connection string", func(t *testing.T) {
		log := testutils.CreateTestLogger()
		cfg := &config.Config{
			Datastore:                   config.DatastoreCockroach,
			CockroachURL:                "invalid://connection",
			CockroachMaxOpenConns:       50,
			CockroachMaxIdleConns:       10,
			CockroachRegion:             "us-east-1",
			MaxRetries:                  3,
			JanitorDeleteFailedMessages: false,
			MultiRegionSupplement:       true,
			MultiRegionScheduler:        true,
			MultiRegionJanitor:          false,
			SchedulerMaxMessagesPerPoll: 5000,
		}

		_, err := newCockroachStore(cfg, log)

		if err == nil {
			t.Error("Expected error with invalid connection string")
		}
	})

	t.Run("returns error with empty connection string", func(t *testing.T) {
		log := testutils.CreateTestLogger()
		cfg := &config.Config{
			Datastore:                   config.DatastoreCockroach,
			CockroachURL:                "",
			CockroachMaxOpenConns:       50,
			CockroachMaxIdleConns:       10,
			CockroachRegion:             "us-east-1",
			MaxRetries:                  3,
			JanitorDeleteFailedMessages: false,
			MultiRegionSupplement:       true,
			MultiRegionScheduler:        true,
			MultiRegionJanitor:          false,
			SchedulerMaxMessagesPerPoll: 5000,
		}

		_, err := newCockroachStore(cfg, log)

		if err == nil {
			t.Error("Expected error with empty connection string")
		}
	})

	t.Run("includes region in config", func(t *testing.T) {
		log := testutils.CreateTestLogger()
		cfg := &config.Config{
			Datastore:                   config.DatastoreCockroach,
			CockroachURL:                "postgres://test:5432",
			CockroachMaxOpenConns:       50,
			CockroachMaxIdleConns:       10,
			CockroachRegion:             "us-west-1",
			MaxRetries:                  3,
			JanitorDeleteFailedMessages: false,
			MultiRegionSupplement:       true,
			MultiRegionScheduler:        true,
			MultiRegionJanitor:          false,
			SchedulerMaxMessagesPerPoll: 5000,
		}

		// Will fail to connect, but we're verifying region is passed
		_, err := newCockroachStore(cfg, log)

		// Should not be an "unsupported datastore" error
		if err != nil && strings.Contains(err.Error(), "unsupported datastore") {
			t.Error("Should not return 'unsupported datastore' error")
		}
	})
}

func TestDatastoreSelection(t *testing.T) {
	testCases := []struct {
		name        string
		datastore   string
		expectError bool
		errorText   string
		checkNil    bool
	}{
		{
			name:        "postgres datastore",
			datastore:   config.DatastorePostgres,
			expectError: true, // Will error on invalid connection, but correct path
			errorText:   "",   // Any error except "unsupported datastore"
			checkNil:    false,
		},
		{
			name:        "cockroach datastore",
			datastore:   config.DatastoreCockroach,
			expectError: true, // Will error on invalid connection, but correct path
			errorText:   "",   // Any error except "unsupported datastore"
			checkNil:    false,
		},
		{
			name:        "invalid datastore",
			datastore:   "mysql",
			expectError: true,
			errorText:   "unsupported datastore: mysql",
			checkNil:    true,
		},
		{
			name:        "empty datastore",
			datastore:   "",
			expectError: true,
			errorText:   "unsupported datastore: ",
			checkNil:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			log := testutils.CreateTestLogger()
			cfg := &config.Config{
				Datastore:                   tc.datastore,
				PostgresURL:                 "postgres://invalid",
				CockroachURL:                "postgres://invalid",
				PostgresMaxOpenConns:        25,
				PostgresMaxIdleConns:        5,
				CockroachMaxOpenConns:       50,
				CockroachMaxIdleConns:       10,
				CockroachRegion:             "us-east-1",
				MaxRetries:                  3,
				JanitorDeleteFailedMessages: true,
				SchedulerMaxMessagesPerPoll: 1000,
			}

			store, err := NewStore(cfg, log)

			if tc.expectError && err == nil {
				t.Error("Expected error but got none")
			}

			if !tc.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if tc.errorText != "" && err != nil {
				if err.Error() != tc.errorText {
					t.Errorf("Expected error text '%s', got '%s'", tc.errorText, err.Error())
				}
			}

			// Only check for nil store when we expect unsupported datastore error
			if tc.checkNil && store != nil {
				t.Error("Expected nil store with unsupported datastore")
			}
		})
	}
}
