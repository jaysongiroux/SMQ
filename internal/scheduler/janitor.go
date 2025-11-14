package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jaysongiroux/smq/internal/circuit_breaker"
	"github.com/jaysongiroux/smq/internal/models"
)

type JanitorNode struct {
	id                int
	isRunning         bool
	lastRun           time.Time
	messagesCleanedUp int64
	nodesCleanedUp    int64
	lastError         error
	mu                sync.RWMutex
	circuitBreaker    *circuit_breaker.CircuitBreaker
}

func (s *Scheduler) janitorLoop(node *JanitorNode) {
	defer s.wg.Done()

	jitteredInterval := applyJitter(s.config.JanitorInterval, s.config.JanitorJitterPercent)
	s.log.Debug("Janitor node %d using interval %v (base: %v, jitter: %d%%)",
		node.id, jitteredInterval, s.config.JanitorInterval, s.config.JanitorJitterPercent)

	ticker := time.NewTicker(jitteredInterval)
	defer ticker.Stop()

	// initial run
	s.genericJanitorCircuitBreaker(node, s.cleanupStaleMessages)
	s.genericJanitorCircuitBreaker(node, s.cleanupStaleNodes)
	s.genericJanitorCircuitBreaker(node, s.cleanupFailedMessages)

	for {
		select {
		case <-ticker.C:
			s.genericJanitorCircuitBreaker(node, s.cleanupStaleMessages)
			s.genericJanitorCircuitBreaker(node, s.cleanupStaleNodes)
			s.genericJanitorCircuitBreaker(node, s.cleanupFailedMessages)

		case <-s.ctx.Done():
			s.log.Debug("Janitor node %d loop exiting", node.id)
			return
		}
	}
}

// generic circuit breaker wrapper for janitor operations
func (s *Scheduler) genericJanitorCircuitBreaker(node *JanitorNode, fn func(node *JanitorNode)) {
	s.log.Debug("Janitor node %d checking for stale messages (threshold: %v)", node.id, s.config.StaleAcquiredThreshold)

	err := node.circuitBreaker.Execute(s.ctx, func(ctx context.Context) error {
		fn(node)
		return nil
	})

	if err != nil {
		if err == circuit_breaker.ErrCircuitOpen {
			s.log.Debug("Janitor node %d: circuit breaker is open, skipping poll", node.id)
			return
		}

		s.log.Error("Janitor node %d failed to cleanup stale messages: %v", node.id, err)
		return
	}

}

func (s *Scheduler) cleanupStaleMessages(node *JanitorNode) {
	s.log.Debug("Janitor node %d checking for stale messages (threshold: %v)",
		node.id, s.config.StaleAcquiredThreshold)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	count, err := s.store.MarkStaleAcquiredMessagesAsReady(ctx, s.config.StaleAcquiredThreshold)

	// Update node stats
	node.mu.Lock()
	node.lastRun = time.Now()
	if err == nil {
		node.messagesCleanedUp += count
		node.lastError = nil
	} else {
		node.lastError = err
	}
	node.mu.Unlock()

	if err != nil {
		s.log.Error("Janitor node %d failed to cleanup stale messages: %v", node.id, err)
		return
	}

	s.log.Info("Janitor node %d recovered %d stale messages", node.id, count)
}

func (s *Scheduler) cleanupStaleNodes(node *JanitorNode) {
	s.log.Debug("Janitor node %d checking for stale nodes (threshold: %v)",
		node.id, s.config.StaleNodeThreshold)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	count, err := s.store.DeleteStaleNodes(ctx, s.config.StaleNodeThreshold)

	// Update node stats
	node.mu.Lock()
	if err == nil {
		node.nodesCleanedUp += count
		node.lastError = nil
	} else {
		node.lastError = err
	}
	node.mu.Unlock()

	if err != nil {
		s.log.Error("Janitor node %d failed to cleanup stale nodes: %v", node.id, err)
		return
	}

	s.log.Info("Janitor node %d removed %d stale nodes from cluster", node.id, count)
}

func (s *Scheduler) cleanupFailedMessages(node *JanitorNode) {
	s.log.Debug("Janitor node %d checking for failed messages (threshold: %v)",
		node.id, s.config.StaleAcquiredThreshold)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	count, err := s.store.CleanFailedMessages(ctx)

	if err != nil {
		s.log.Error("Janitor node %d failed to cleanup failed messages: %v", node.id, err)
		return
	}

	s.log.Info("Janitor node %d cleaned up %d failed messages", node.id, count)
}

func (s *Scheduler) getJanitorHealth() map[string]*models.ComponentHealth {
	health := make(map[string]*models.ComponentHealth)

	for _, node := range s.janitorNodes {
		node.mu.RLock()
		status := models.HealthStatusHealthy
		message := "Janitor node is operational"

		cbMetrics := node.circuitBreaker.GetMetrics()

		if !node.isRunning {
			status = models.HealthStatusUnhealthy
			message = "Janitor node is not running"
		} else if node.lastError != nil {
			status = models.HealthStatusDegraded
			message = "Janitor node experiencing errors: " + node.lastError.Error()
		} else if time.Since(node.lastRun) > s.config.JanitorInterval*3 {
			status = models.HealthStatusDegraded
			message = "Janitor node is delayed - last run was " + time.Since(node.lastRun).String() + " ago"
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

		health[fmt.Sprintf("janitor-node-%d", node.id)] = &models.ComponentHealth{
			Name:      fmt.Sprintf("janitor-node-%d", node.id),
			Status:    status,
			Message:   message,
			CheckedAt: time.Now(),
			Metadata: map[string]interface{}{
				"is_running":          node.isRunning,
				"last_run":            node.lastRun,
				"messages_cleaned_up": node.messagesCleanedUp,
				"nodes_cleaned_up":    node.nodesCleanedUp,
				"last_error":          formatError(node.lastError),

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
