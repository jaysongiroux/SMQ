package health

import (
	"errors"
	"testing"
	"time"

	"github.com/jaysongiroux/smq/internal/config"
	"github.com/jaysongiroux/smq/internal/models"
	"github.com/jaysongiroux/smq/internal/testutils"
)

func TestNewHealthChecker(t *testing.T) {
	store := &testutils.MockStore{}
	log := testutils.CreateTestLogger()
	cfg := &config.Config{Region: "us-east-1"}
	nodeID := "test-node-123"
	checkInterval := 1 * time.Second

	checker := NewHealthChecker(cfg, store, nodeID, checkInterval, log)

	if checker == nil {
		t.Fatal("Expected health checker to be created, got nil")
	}

	if checker.store != store {
		t.Error("Expected store to be set")
	}

	if checker.nodeID != nodeID {
		t.Errorf("Expected nodeID to be %s, got %s", nodeID, checker.nodeID)
	}

	if checker.checkInterval != checkInterval {
		t.Errorf("Expected checkInterval to be %v, got %v", checkInterval, checker.checkInterval)
	}

	if checker.config != cfg {
		t.Error("Expected config to be set")
	}

	if checker.log != log {
		t.Error("Expected logger to be set")
	}

	if len(checker.reporters) != 0 {
		t.Errorf("Expected no reporters initially, got %d", len(checker.reporters))
	}

	if checker.previousStatus != models.HealthStatusHealthy {
		t.Errorf("Expected initial status to be healthy, got %s", checker.previousStatus)
	}

	if checker.ctx == nil {
		t.Error("Expected context to be initialized")
	}
}

func TestHealthCheckerStore(t *testing.T) {
	store := &testutils.MockStore{}
	log := testutils.CreateTestLogger()
	cfg := &config.Config{Region: "us-east-1"}
	checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)

	if checker.Store() != store {
		t.Error("Expected Store() to return the configured store")
	}
}

func TestHealthCheckerRegisterReporter(t *testing.T) {
	t.Run("registers single reporter", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)

		reporter := &testutils.MockHealthReporter{
			SystemHealth: &models.ComponentHealth{
				Name:      "test-component",
				Status:    models.HealthStatusHealthy,
				Message:   "All good",
				CheckedAt: time.Now(),
			},
		}

		checker.RegisterReporter(reporter)

		if len(checker.reporters) != 1 {
			t.Errorf("Expected 1 reporter, got %d", len(checker.reporters))
		}

		if checker.reporters[0] != reporter {
			t.Error("Expected reporter to be registered")
		}
	})

	t.Run("registers multiple reporters", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)

		reporter1 := &testutils.MockHealthReporter{
			SystemHealth: &models.ComponentHealth{Name: "component1", Status: models.HealthStatusHealthy},
		}
		reporter2 := &testutils.MockHealthReporter{
			SystemHealth: &models.ComponentHealth{Name: "component2", Status: models.HealthStatusHealthy},
		}

		checker.RegisterReporter(reporter1)
		checker.RegisterReporter(reporter2)

		if len(checker.reporters) != 2 {
			t.Errorf("Expected 2 reporters, got %d", len(checker.reporters))
		}
	})
}

func TestHealthCheckerRegisterSchedulerHealth(t *testing.T) {
	store := &testutils.MockStore{}
	log := testutils.CreateTestLogger()
	cfg := &config.Config{Region: "us-east-1"}
	checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)

	schedulerHealthFunc := func() map[string]*models.ComponentHealth {
		return map[string]*models.ComponentHealth{
			"scheduler-1": {
				Name:    "scheduler-1",
				Status:  models.HealthStatusHealthy,
				Message: "Running",
			},
		}
	}

	checker.RegisterSchedulerHealth(schedulerHealthFunc)

	if checker.schedulerHealth == nil {
		t.Error("Expected scheduler health function to be registered")
	}

	// Verify it can be called
	result := checker.schedulerHealth()
	if len(result) != 1 {
		t.Errorf("Expected 1 scheduler node, got %d", len(result))
	}
}

func TestHealthCheckerStartStop(t *testing.T) {
	store := &testutils.MockStore{}
	log := testutils.CreateTestLogger()
	cfg := &config.Config{Region: "us-east-1"}
	checker := NewHealthChecker(cfg, store, "test-node", 100*time.Millisecond, log)

	err := checker.Start()
	if err != nil {
		t.Fatalf("Expected no error starting health checker, got: %v", err)
	}

	// Give it time to run at least once
	time.Sleep(150 * time.Millisecond)

	err = checker.Stop()
	if err != nil {
		t.Fatalf("Expected no error stopping health checker, got: %v", err)
	}

	// Verify context is cancelled
	select {
	case <-checker.ctx.Done():
		// Context properly cancelled
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected context to be cancelled after Stop()")
	}
}

func TestCheckAndStoreHealth(t *testing.T) {
	t.Run("reports healthy system with all healthy components", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)

		reporter := &testutils.MockHealthReporter{
			SystemHealth: &models.ComponentHealth{
				Name:      "test-component",
				Status:    models.HealthStatusHealthy,
				Message:   "All good",
				CheckedAt: time.Now(),
			},
		}

		checker.RegisterReporter(reporter)
		checker.checkAndStoreHealth()

		if checker.systemHealth == nil {
			t.Fatal("Expected systemHealth to be set")
		}

		if checker.systemHealth.Status != models.HealthStatusHealthy {
			t.Errorf("Expected status to be healthy, got %s", checker.systemHealth.Status)
		}

		if checker.systemHealth.Region != "us-east-1" {
			t.Errorf("Expected region to be us-east-1, got %s", checker.systemHealth.Region)
		}

		if store.RegisterNodeCalls != 1 {
			t.Errorf("Expected RegisterNode to be called once, got %d calls", store.RegisterNodeCalls)
		}

		if !checker.lastCheck.IsZero() {
			// lastCheck should be updated
		} else {
			t.Error("Expected lastCheck to be updated")
		}
	})

	t.Run("reports degraded system with degraded component", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)

		reporter := &testutils.MockHealthReporter{
			SystemHealth: &models.ComponentHealth{
				Name:    "test-component",
				Status:  models.HealthStatusDegraded,
				Message: "Slow responses",
			},
		}

		checker.RegisterReporter(reporter)
		checker.checkAndStoreHealth()

		if checker.systemHealth.Status != models.HealthStatusDegraded {
			t.Errorf("Expected status to be degraded, got %s", checker.systemHealth.Status)
		}
	})

	t.Run("reports unhealthy system with unhealthy component", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)

		reporter := &testutils.MockHealthReporter{
			SystemHealth: &models.ComponentHealth{
				Name:    "test-component",
				Status:  models.HealthStatusUnhealthy,
				Message: "Database connection failed",
			},
		}

		checker.RegisterReporter(reporter)
		checker.checkAndStoreHealth()

		if checker.systemHealth.Status != models.HealthStatusUnhealthy {
			t.Errorf("Expected status to be unhealthy, got %s", checker.systemHealth.Status)
		}
	})

	t.Run("unhealthy takes precedence over degraded", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)

		healthyReporter := &testutils.MockHealthReporter{
			SystemHealth: &models.ComponentHealth{
				Name:   "healthy-component",
				Status: models.HealthStatusHealthy,
			},
		}

		degradedReporter := &testutils.MockHealthReporter{
			SystemHealth: &models.ComponentHealth{
				Name:   "degraded-component",
				Status: models.HealthStatusDegraded,
			},
		}

		unhealthyReporter := &testutils.MockHealthReporter{
			SystemHealth: &models.ComponentHealth{
				Name:   "unhealthy-component",
				Status: models.HealthStatusUnhealthy,
			},
		}

		checker.RegisterReporter(healthyReporter)
		checker.RegisterReporter(degradedReporter)
		checker.RegisterReporter(unhealthyReporter)
		checker.checkAndStoreHealth()

		if checker.systemHealth.Status != models.HealthStatusUnhealthy {
			t.Errorf("Expected status to be unhealthy, got %s", checker.systemHealth.Status)
		}
	})

	t.Run("includes scheduler health when registered", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)

		schedulerHealthFunc := func() map[string]*models.ComponentHealth {
			return map[string]*models.ComponentHealth{
				"scheduler-1": {
					Name:    "scheduler-1",
					Status:  models.HealthStatusHealthy,
					Message: "Running",
				},
				"scheduler-2": {
					Name:    "scheduler-2",
					Status:  models.HealthStatusHealthy,
					Message: "Running",
				},
			}
		}

		checker.RegisterSchedulerHealth(schedulerHealthFunc)
		checker.checkAndStoreHealth()

		if checker.systemHealth.Layers["scheduler"] == nil {
			t.Fatal("Expected scheduler layer to be present")
		}

		schedulerLayer := checker.systemHealth.Layers["scheduler"]
		if len(schedulerLayer.Nodes) != 2 {
			t.Errorf("Expected 2 scheduler nodes, got %d", len(schedulerLayer.Nodes))
		}

		if schedulerLayer.Status != models.HealthStatusHealthy {
			t.Errorf("Expected scheduler layer to be healthy, got %s", schedulerLayer.Status)
		}
	})

	t.Run("detects unhealthy scheduler nodes", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)

		schedulerHealthFunc := func() map[string]*models.ComponentHealth {
			return map[string]*models.ComponentHealth{
				"scheduler-1": {
					Name:    "scheduler-1",
					Status:  models.HealthStatusUnhealthy,
					Message: "Node crashed",
				},
			}
		}

		checker.RegisterSchedulerHealth(schedulerHealthFunc)
		checker.checkAndStoreHealth()

		if checker.systemHealth.Status != models.HealthStatusUnhealthy {
			t.Errorf("Expected system to be unhealthy, got %s", checker.systemHealth.Status)
		}

		schedulerLayer := checker.systemHealth.Layers["scheduler"]
		if schedulerLayer.Status != models.HealthStatusUnhealthy {
			t.Errorf("Expected scheduler layer to be unhealthy, got %s", schedulerLayer.Status)
		}
	})

	t.Run("handles store error gracefully", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)

		store.RegisterNodeError = errors.New("database error")

		reporter := &testutils.MockHealthReporter{
			SystemHealth: &models.ComponentHealth{
				Name:   "test-component",
				Status: models.HealthStatusHealthy,
			},
		}

		checker.RegisterReporter(reporter)

		// Should not panic
		checker.checkAndStoreHealth()

		// Health should still be computed even if store fails
		if checker.systemHealth == nil {
			t.Error("Expected systemHealth to be computed despite store error")
		}
	})

	t.Run("tracks status changes", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)

		reporter := &testutils.MockHealthReporter{
			SystemHealth: &models.ComponentHealth{
				Name:   "test-component",
				Status: models.HealthStatusHealthy,
			},
		}

		checker.RegisterReporter(reporter)
		checker.checkAndStoreHealth()

		if checker.previousStatus != models.HealthStatusHealthy {
			t.Errorf("Expected previous status to be healthy, got %s", checker.previousStatus)
		}

		// Change status to unhealthy
		reporter.SetStatus(models.HealthStatusUnhealthy)
		checker.checkAndStoreHealth()

		if checker.previousStatus != models.HealthStatusUnhealthy {
			t.Errorf("Expected previous status to be unhealthy, got %s", checker.previousStatus)
		}
	})
}

func TestStoreHealthInDatabase(t *testing.T) {
	t.Run("successfully stores health data", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		nodeID := "test-node-123"
		checker := NewHealthChecker(cfg, store, nodeID, 1*time.Second, log)

		checker.systemHealth = &models.SystemHealth{
			Status:    models.HealthStatusHealthy,
			Layers:    make(map[string]*models.LayerHealth),
			UpdatedAt: time.Now(),
			Region:    "us-east-1",
		}

		checker.storeHealthInDatabase()

		if store.RegisterNodeCalls != 1 {
			t.Errorf("Expected RegisterNode to be called once, got %d calls", store.RegisterNodeCalls)
		}

		if store.LastRegisteredNode == nil {
			t.Fatal("Expected node to be registered")
		}

		if store.LastRegisteredNode.NodeID != nodeID {
			t.Errorf("Expected nodeID to be %s, got %s", nodeID, store.LastRegisteredNode.NodeID)
		}

		if store.LastRegisteredNode.Status != string(models.HealthStatusHealthy) {
			t.Errorf("Expected status to be healthy, got %s", store.LastRegisteredNode.Status)
		}

		if store.LastRegisteredNode.Metadata == nil {
			t.Error("Expected metadata to be set")
		}
	})

	t.Run("handles nil system health", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		nodeID := "test-node-123"
		checker := NewHealthChecker(cfg, store, nodeID, 1*time.Second, log)

		checker.systemHealth = nil

		// Should not panic
		checker.storeHealthInDatabase()
	})

	t.Run("handles RegisterNode error", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)

		store.RegisterNodeError = errors.New("database error")

		checker.systemHealth = &models.SystemHealth{
			Status:    models.HealthStatusHealthy,
			Layers:    make(map[string]*models.LayerHealth),
			UpdatedAt: time.Now(),
			Region:    "us-east-1",
		}

		// Should not panic
		checker.storeHealthInDatabase()

		if store.RegisterNodeCalls != 1 {
			t.Errorf("Expected RegisterNode to be called once despite error, got %d calls", store.RegisterNodeCalls)
		}
	})
}

func TestHealthCheckerIntegration(t *testing.T) {
	t.Run("periodic health checks update database", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 50*time.Millisecond, log)

		reporter := &testutils.MockHealthReporter{
			SystemHealth: &models.ComponentHealth{
				Name:   "test-component",
				Status: models.HealthStatusHealthy,
			},
		}

		checker.RegisterReporter(reporter)

		err := checker.Start()
		if err != nil {
			t.Fatalf("Failed to start health checker: %v", err)
		}

		// Wait for multiple health checks
		time.Sleep(200 * time.Millisecond)

		err = checker.Stop()
		if err != nil {
			t.Fatalf("Failed to stop health checker: %v", err)
		}

		// Should have called RegisterNode multiple times
		if store.RegisterNodeCalls < 2 {
			t.Errorf("Expected at least 2 RegisterNode calls, got %d", store.RegisterNodeCalls)
		}
	})

	t.Run("detects status changes over time", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 50*time.Millisecond, log)

		reporter := &testutils.MockHealthReporter{
			SystemHealth: &models.ComponentHealth{
				Name:   "test-component",
				Status: models.HealthStatusHealthy,
			},
		}

		checker.RegisterReporter(reporter)

		err := checker.Start()
		if err != nil {
			t.Fatalf("Failed to start health checker: %v", err)
		}

		// Wait for first check
		time.Sleep(75 * time.Millisecond)

		// Change status
		reporter.SetStatus(models.HealthStatusUnhealthy)

		// Wait for next check
		time.Sleep(75 * time.Millisecond)

		err = checker.Stop()
		if err != nil {
			t.Fatalf("Failed to stop health checker: %v", err)
		}

		// Verify status changed
		if checker.previousStatus != models.HealthStatusUnhealthy {
			t.Errorf("Expected final status to be unhealthy, got %s", checker.previousStatus)
		}
	})
}

func TestHealthCheckerConcurrency(t *testing.T) {
	t.Run("handles concurrent reporter registration", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 1*time.Second, log)

		// Register reporters concurrently
		done := make(chan bool, 10)
		for i := 0; i < 10; i++ {
			go func(id int) {
				reporter := &testutils.MockHealthReporter{
					SystemHealth: &models.ComponentHealth{
						Name:   "component",
						Status: models.HealthStatusHealthy,
					},
				}
				checker.RegisterReporter(reporter)
				done <- true
			}(i)
		}

		// Wait for all registrations
		for i := 0; i < 10; i++ {
			<-done
		}

		if len(checker.reporters) != 10 {
			t.Errorf("Expected 10 reporters, got %d", len(checker.reporters))
		}
	})

	t.Run("health checks run safely while registering reporters", func(t *testing.T) {
		store := &testutils.MockStore{}
		log := testutils.CreateTestLogger()
		cfg := &config.Config{Region: "us-east-1"}
		checker := NewHealthChecker(cfg, store, "test-node", 10*time.Millisecond, log)

		err := checker.Start()
		if err != nil {
			t.Fatalf("Failed to start health checker: %v", err)
		}

		// Register reporters while health checks are running
		for i := 0; i < 5; i++ {
			reporter := &testutils.MockHealthReporter{
				SystemHealth: &models.ComponentHealth{
					Name:   "component",
					Status: models.HealthStatusHealthy,
				},
			}
			checker.RegisterReporter(reporter)
			time.Sleep(20 * time.Millisecond)
		}

		time.Sleep(50 * time.Millisecond)

		err = checker.Stop()
		if err != nil {
			t.Fatalf("Failed to stop health checker: %v", err)
		}
	})
}
