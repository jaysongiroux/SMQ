package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jaysongiroux/smq/internal/circuit_breaker"
	"github.com/jaysongiroux/smq/internal/db"
	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/models"
)

type Scheduler struct {
	config         *SchedulerConfig
	store          db.Store
	log            *logger.Logger
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
	schedulerNodes []*SchedulerNode
	janitorNodes   []*JanitorNode
	mu             sync.RWMutex
}

type SchedulerNode struct {
	id             int
	circuitBreaker *circuit_breaker.CircuitBreaker
	isRunning      bool
	lastRun        time.Time
	messagesMarked int64
	mu             sync.RWMutex
}

func NewScheduler(
	config *SchedulerConfig,
	store db.Store,
	numSchedulerNodes,
	numJanitorNodes int,
	log *logger.Logger,
) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())

	schedulerNodes := make([]*SchedulerNode, numSchedulerNodes)

	for i := 0; i < numSchedulerNodes; i++ {
		// Create circuit breaker for this scheduler
		cb := circuit_breaker.NewCircuitBreaker(
			circuit_breaker.Config{
				Name:            "scheduler",
				MaxFailures:     config.CBMaxFailures,
				Timeout:         config.CBTimeout,
				ResetTimeout:    config.CBResetTimeout,
				HalfOpenMaxReqs: config.HalfOpenMaxReqs,
				Log:             log,
			},
		)
		schedulerNodes[i] = &SchedulerNode{
			id:             i,
			circuitBreaker: cb,
		}
	}

	janitorNodes := make([]*JanitorNode, numJanitorNodes)
	for i := 0; i < numJanitorNodes; i++ {

		// Create circuit breaker for this scheduler
		cb := circuit_breaker.NewCircuitBreaker(
			circuit_breaker.Config{
				Name:            "scheduler",
				MaxFailures:     config.CBMaxFailures,
				Timeout:         config.CBTimeout,
				ResetTimeout:    config.CBResetTimeout,
				HalfOpenMaxReqs: config.HalfOpenMaxReqs,
				Log:             log,
			},
		)

		janitorNodes[i] = &JanitorNode{
			id:             i,
			circuitBreaker: cb,
		}
	}

	return &Scheduler{
		config:         config,
		store:          store,
		log:            log,
		ctx:            ctx,
		cancel:         cancel,
		schedulerNodes: schedulerNodes,
		janitorNodes:   janitorNodes,
	}
}

func (s *Scheduler) Start() {
	s.log.Info("Starting scheduler with %d scheduler nodes and %d janitor nodes",
		len(s.schedulerNodes), len(s.janitorNodes))

	for _, node := range s.schedulerNodes {
		node.mu.Lock()
		node.isRunning = true
		node.mu.Unlock()

		s.wg.Add(1)
		go s.schedulerLoop(node)
		s.log.Debug("Started scheduler node %d (poll interval: %v)", node.id, s.config.PollInterval)
	}

	for _, node := range s.janitorNodes {
		node.mu.Lock()
		node.isRunning = true
		node.mu.Unlock()

		s.wg.Add(1)
		go s.janitorLoop(node)
		s.log.Debug("Started janitor node %d (interval: %v, stale message threshold: %v, stale node threshold: %v)",
			node.id, s.config.JanitorInterval, s.config.StaleAcquiredThreshold, s.config.StaleNodeThreshold)
	}

	s.log.Info("Scheduler started successfully")
}

func (s *Scheduler) Stop() error {
	s.log.Info("Stopping scheduler...")
	s.cancel()
	s.wg.Wait()

	for _, node := range s.schedulerNodes {
		node.mu.Lock()
		s.log.Info("Scheduler node %d stopped (total messages marked: %d)", node.id, node.messagesMarked)
		node.isRunning = false
		node.mu.Unlock()
	}
	for _, node := range s.janitorNodes {
		node.mu.Lock()
		s.log.Info("Janitor node %d stopped (messages cleaned: %d, nodes cleaned: %d)",
			node.id, node.messagesCleanedUp, node.nodesCleanedUp)
		node.isRunning = false
		node.mu.Unlock()
	}

	s.log.Info("Scheduler stopped successfully")
	return nil
}

// Health returns health status for all scheduler and janitor nodes
func (s *Scheduler) Health() map[string]*models.ComponentHealth {
	s.mu.RLock()
	defer s.mu.RUnlock()

	health := make(map[string]*models.ComponentHealth)

	schedulerHealth := s.getSchedulerHealth()
	for k, v := range schedulerHealth {
		health[k] = v
	}

	janitorHealth := s.getJanitorHealth()
	for k, v := range janitorHealth {
		health[k] = v
	}

	return health
}

// getSchedulerHealth returns health for all scheduler nodes including circuit breaker state
func (s *Scheduler) getSchedulerHealth() map[string]*models.ComponentHealth {
	health := make(map[string]*models.ComponentHealth)

	for _, node := range s.schedulerNodes {
		node.mu.RLock()

		// Get circuit breaker metrics
		cbMetrics := node.circuitBreaker.GetMetrics()

		// Determine health status based on node state and circuit breaker
		status := models.HealthStatusHealthy
		message := "Scheduler node is operational"

		if !node.isRunning {
			status = models.HealthStatusUnhealthy
			message = "Scheduler node is not running"
		} else if cbMetrics.State == circuit_breaker.StateOpen {
			status = models.HealthStatusUnhealthy
			message = fmt.Sprintf("Circuit breaker open - paused after %d failures (last failure: %v ago)",
				cbMetrics.ConsecutiveFailures,
				time.Since(cbMetrics.LastFailureTime).Round(time.Second))
		} else if cbMetrics.State == circuit_breaker.StateHalfOpen {
			status = models.HealthStatusDegraded
			message = "Circuit breaker testing recovery - limited requests"
		} else if time.Since(node.lastRun) > s.config.PollInterval*3 {
			status = models.HealthStatusDegraded
			message = fmt.Sprintf("Scheduler node delayed - last run %v ago",
				time.Since(node.lastRun).Round(time.Second))
		} else if cbMetrics.SuccessRate < 95 && cbMetrics.TotalRequests > 10 {
			status = models.HealthStatusDegraded
			message = fmt.Sprintf("Low success rate: %.1f%%", cbMetrics.SuccessRate)
		}

		health[fmt.Sprintf("scheduler-node-%d", node.id)] = &models.ComponentHealth{
			Name:      fmt.Sprintf("scheduler-node-%d", node.id),
			Status:    status,
			Message:   message,
			CheckedAt: time.Now(),
			Metadata: map[string]interface{}{
				"is_running":      node.isRunning,
				"last_run":        node.lastRun,
				"messages_marked": node.messagesMarked,

				// Circuit breaker metrics
				"circuit_breaker": map[string]interface{}{
					"state":                string(cbMetrics.State),
					"total_requests":       cbMetrics.TotalRequests,
					"total_successes":      cbMetrics.TotalSuccesses,
					"total_failures":       cbMetrics.TotalFailures,
					"total_timeouts":       cbMetrics.TotalTimeouts,
					"success_rate":         fmt.Sprintf("%.1f%%", cbMetrics.SuccessRate),
					"consecutive_failures": cbMetrics.ConsecutiveFailures,
					"total_circuit_opens":  cbMetrics.TotalCircuitOpens,
					"last_failure_time":    cbMetrics.LastFailureTime,
				},
			},
		}
		node.mu.RUnlock()
	}

	return health
}

func (s *Scheduler) genericSchedulerCircuitBreaker(node *SchedulerNode, fn func(ctx context.Context, node *SchedulerNode) error) {
	s.log.Debug("Scheduler node %d checking for pending messages", node.id)

	err := node.circuitBreaker.Execute(s.ctx, func(ctx context.Context) error {
		err := fn(ctx, node)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		if errors.Is(err, circuit_breaker.ErrCircuitOpen) {
			s.log.Debug("Scheduler node %d: circuit breaker is open, skipping poll", node.id)
			return
		}
		s.log.Error("Scheduler node %d failed to mark pending messages: %v", node.id, err)
		return
	}
}

func (s *Scheduler) schedulerLoop(node *SchedulerNode) {
	defer s.wg.Done()

	jitteredInterval := applyJitter(s.config.PollInterval, s.config.PollJitterPercent)
	s.log.Debug("Scheduler node %d using interval %v (base: %v, jitter: %d%%)",
		node.id, jitteredInterval, s.config.PollInterval, s.config.PollJitterPercent)

	ticker := time.NewTicker(jitteredInterval)
	defer ticker.Stop()

	// Initial run
	s.genericSchedulerCircuitBreaker(node, s.markPendingMessagesReady)

	for {
		select {
		case <-ticker.C:
			s.genericSchedulerCircuitBreaker(node, s.markPendingMessagesReady)

		case <-s.ctx.Done():
			node.mu.Lock()
			node.isRunning = false
			node.mu.Unlock()
			return
		}
	}
}

func (s *Scheduler) markPendingMessagesReady(ctx context.Context, node *SchedulerNode) error {
	count, err := s.store.MarkPendingMessagesAsReady(ctx, time.Now())

	// Update node state
	node.mu.Lock()
	node.lastRun = time.Now()
	if err == nil {
		node.messagesMarked += count
	}
	node.mu.Unlock()

	if err != nil {
		return fmt.Errorf("failed to mark messages: %w", err)
	}

	if count > 0 {
		s.log.Info("Scheduler node %d marked %d messages as ready", node.id, count)
	}

	return nil
}
