package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

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
}

func (s *Scheduler) janitorLoop(node *JanitorNode) {
	defer s.wg.Done()

	jitteredInterval := applyJitter(s.config.JanitorInterval, s.config.JanitorJitterPercent)
	s.log.Debug("Janitor node %d using interval %v (base: %v, jitter: %d%%)",
		node.id, jitteredInterval, s.config.JanitorInterval, s.config.JanitorJitterPercent)

	ticker := time.NewTicker(jitteredInterval)
	defer ticker.Stop()

	s.cleanupStaleMessages(node)
	s.cleanupStaleNodes(node)
	s.cleanupFailedMessages(node)

	for {
		select {
		case <-ticker.C:
			s.cleanupStaleMessages(node)
			s.cleanupStaleNodes(node)

		case <-s.ctx.Done():
			s.log.Debug("Janitor node %d loop exiting", node.id)
			return
		}
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

		if !node.isRunning {
			status = models.HealthStatusUnhealthy
			message = "Janitor node is not running"
		} else if node.lastError != nil {
			status = models.HealthStatusDegraded
			message = "Janitor node experiencing errors: " + node.lastError.Error()
		} else if time.Since(node.lastRun) > s.config.JanitorInterval*3 {
			status = models.HealthStatusDegraded
			message = "Janitor node is delayed - last run was " + time.Since(node.lastRun).String() + " ago"
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
			},
		}
		node.mu.RUnlock()
	}

	return health
}
