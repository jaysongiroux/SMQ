package health

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jaysongiroux/smq/internal/config"
	"github.com/jaysongiroux/smq/internal/db"
	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/models"
)

// HealthChecker monitors and reports cluster health
// It runs as its own routine, checking all layers and storing aggregated health in the database
type HealthChecker struct {
	config          *config.Config
	store           db.Store
	nodeID          string
	log             *logger.Logger
	mu              sync.RWMutex
	reporters       []models.HealthReporter
	schedulerHealth func() map[string]*models.ComponentHealth
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	checkInterval   time.Duration
	lastCheck       time.Time
	systemHealth    *models.SystemHealth
	previousStatus  models.HealthStatus
}

func (h *HealthChecker) Store() db.Store {
	return h.store
}

func NewHealthChecker(
	config *config.Config,
	store db.Store,
	nodeID string,
	checkInterval time.Duration,
	log *logger.Logger,
) *HealthChecker {
	ctx, cancel := context.WithCancel(context.Background())

	// check if nodeID is empty
	if nodeID == "" {
		log.Error("nodeID is required, exiting")
		cancel()
		return nil
	}

	return &HealthChecker{
		config:         config,
		store:          store,
		nodeID:         nodeID,
		log:            log,
		ctx:            ctx,
		cancel:         cancel,
		checkInterval:  checkInterval,
		reporters:      make([]models.HealthReporter, 0),
		previousStatus: models.HealthStatusHealthy,
	}
}

func (h *HealthChecker) RegisterReporter(reporter models.HealthReporter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reporters = append(h.reporters, reporter)

	componentHealth := reporter.Health()
	h.log.Debug("Registered health reporter: %s", componentHealth.Name)
}

func (h *HealthChecker) RegisterSchedulerHealth(
	healthFunc func() map[string]*models.ComponentHealth,
) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.schedulerHealth = healthFunc
	h.log.Debug("Registered scheduler health reporter (multi-node)")
}

func (h *HealthChecker) Start() error {
	h.log.Info("Starting health checker routine (interval: %v)", h.checkInterval)
	h.wg.Add(1)
	go h.healthCheckLoop()
	return nil
}

func (h *HealthChecker) Stop() error {
	h.log.Info("Stopping health checker routine")
	h.cancel()
	h.wg.Wait()
	h.log.Info("Health checker stopped")
	return nil
}

func (h *HealthChecker) healthCheckLoop() {
	defer h.wg.Done()

	ticker := time.NewTicker(h.checkInterval)
	defer ticker.Stop()

	h.checkAndStoreHealth()

	for {
		select {
		case <-ticker.C:
			h.checkAndStoreHealth()

		case <-h.ctx.Done():
			return
		}
	}
}

func (h *HealthChecker) checkAndStoreHealth() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.log.Debug("Running health check")

	// Collect health from all layers
	systemHealth := &models.SystemHealth{
		Status:    models.HealthStatusHealthy,
		Layers:    make(map[string]*models.LayerHealth),
		UpdatedAt: time.Now(),
		Region:    h.config.Region,
	}

	for _, reporter := range h.reporters {
		componentHealth := reporter.Health()
		layerName := componentHealth.Name

		h.log.Debug(
			"Component %s: %s - %s",
			layerName,
			componentHealth.Status,
			componentHealth.Message,
		)

		if systemHealth.Layers[layerName] == nil {
			systemHealth.Layers[layerName] = &models.LayerHealth{
				Name:   layerName,
				Status: componentHealth.Status,
				Nodes:  make(map[string]*models.ComponentHealth),
			}
		}

		systemHealth.Layers[layerName].Nodes[componentHealth.Name] = componentHealth

		if componentHealth.Status == models.HealthStatusUnhealthy {
			systemHealth.Layers[layerName].Status = models.HealthStatusUnhealthy
			h.log.Warn("Layer %s is unhealthy: %s", layerName, componentHealth.Message)
		} else if componentHealth.Status == models.HealthStatusDegraded &&
			systemHealth.Layers[layerName].Status != models.HealthStatusUnhealthy {
			systemHealth.Layers[layerName].Status = models.HealthStatusDegraded
			h.log.Warn("Layer %s is degraded: %s", layerName, componentHealth.Message)
		}
	}

	if h.schedulerHealth != nil {
		schedulerNodes := h.schedulerHealth()

		systemHealth.Layers["scheduler"] = &models.LayerHealth{
			Name:   "scheduler",
			Status: models.HealthStatusHealthy,
			Nodes:  schedulerNodes,
		}

		for nodeName, nodeHealth := range schedulerNodes {
			h.log.Debug(
				"Scheduler node %s: %s - %s",
				nodeName,
				nodeHealth.Status,
				nodeHealth.Message,
			)

			if nodeHealth.Status == models.HealthStatusUnhealthy {
				systemHealth.Layers["scheduler"].Status = models.HealthStatusUnhealthy
				h.log.Warn("Scheduler node %s is unhealthy: %s", nodeName, nodeHealth.Message)
			} else if nodeHealth.Status == models.HealthStatusDegraded &&
				systemHealth.Layers["scheduler"].Status != models.HealthStatusUnhealthy {
				systemHealth.Layers["scheduler"].Status = models.HealthStatusDegraded
				h.log.Warn("Scheduler node %s is degraded: %s", nodeName, nodeHealth.Message)
			}
		}
	}

	var unhealthyComponents []string
	var degradedComponents []string

	for layerName, layer := range systemHealth.Layers {
		if layer.Status == models.HealthStatusUnhealthy {
			systemHealth.Status = models.HealthStatusUnhealthy
			for nodeName, node := range layer.Nodes {
				if node.Status == models.HealthStatusUnhealthy {
					unhealthyComponents = append(
						unhealthyComponents,
						fmt.Sprintf("%s/%s", layerName, nodeName),
					)
				}
			}
		} else if layer.Status == models.HealthStatusDegraded &&
			systemHealth.Status != models.HealthStatusUnhealthy {
			systemHealth.Status = models.HealthStatusDegraded
			for nodeName, node := range layer.Nodes {
				if node.Status == models.HealthStatusDegraded {
					degradedComponents = append(degradedComponents, fmt.Sprintf("%s/%s", layerName, nodeName))
				}
			}
		}
	}

	if h.previousStatus != systemHealth.Status {
		h.log.Info("System health status changed: %s -> %s", h.previousStatus, systemHealth.Status)
		h.previousStatus = systemHealth.Status
	}

	// Log overall system health with specific component details
	switch systemHealth.Status {
	case models.HealthStatusHealthy:
		h.log.Info("System health: HEALTHY (all layers operational)")
	case models.HealthStatusDegraded:
		if len(degradedComponents) > 0 {
			h.log.Warn("System health: DEGRADED - Components with issues: %v", degradedComponents)
		} else {
			h.log.Warn("System health: DEGRADED (some components experiencing issues)")
		}
	case models.HealthStatusUnhealthy:
		if len(unhealthyComponents) > 0 {
			h.log.Error(
				"System health: UNHEALTHY - Critical components down: %v",
				unhealthyComponents,
			)
		} else {
			h.log.Error("System health: UNHEALTHY (critical components down)")
		}
	}

	// Store in database
	h.systemHealth = systemHealth
	h.lastCheck = time.Now()
	h.storeHealthInDatabase()
}

func (h *HealthChecker) storeHealthInDatabase() {
	healthJSON, err := json.Marshal(h.systemHealth)
	if err != nil {
		h.log.Error("Failed to marshal health data: %v", err)
		return
	} else if healthJSON == nil {
		h.log.Debug("Health data is empty, skipping storage")
		return
	}

	var metadata map[string]interface{}
	if err := json.Unmarshal(healthJSON, &metadata); err != nil {
		h.log.Error("Failed to unmarshal health data to metadata: %v", err)
		return
	}

	// Upsert node in database with health metadata
	// Using RegisterNode (upsert) instead of UpdateNode allows nodes to re-register
	// themselves if they were previously removed by the janitor
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	//check if systemHealth.Status is set
	if h.systemHealth == nil {
		h.log.Error("systemHealth is nil, skipping health store")
		return
	}

	statusStr := string(h.systemHealth.Status)
	node := &models.Node{
		NodeID:       h.nodeID,
		Status:       statusStr,
		LastSeen:     time.Now(),
		RegisteredAt: time.Now(), // Will be ignored on update due to ON CONFLICT
		Metadata:     metadata,
	}

	if err := h.store.RegisterNode(ctx, node); err != nil {
		h.log.Error("Failed to store health in database: %v", err)
		return
	}

	h.log.Debug("Health data stored in database for node %s", h.nodeID)
}
