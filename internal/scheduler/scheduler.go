package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

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
	isRunning      bool
	lastRun        time.Time
	messagesMarked int64
	mu             sync.RWMutex
}

func NewScheduler(config *SchedulerConfig, store db.Store, numSchedulerNodes, numJanitorNodes int, log *logger.Logger) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())

	schedulerNodes := make([]*SchedulerNode, numSchedulerNodes)
	for i := 0; i < numSchedulerNodes; i++ {
		schedulerNodes[i] = &SchedulerNode{
			id: i,
		}
	}

	janitorNodes := make([]*JanitorNode, numJanitorNodes)
	for i := 0; i < numJanitorNodes; i++ {
		janitorNodes[i] = &JanitorNode{
			id: i,
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

func (s *Scheduler) getSchedulerHealth() map[string]*models.ComponentHealth {
	health := make(map[string]*models.ComponentHealth)

	for _, node := range s.schedulerNodes {
		node.mu.RLock()
		status := models.HealthStatusHealthy
		message := "Scheduler node is operational"

		if !node.isRunning {
			status = models.HealthStatusUnhealthy
			message = "Scheduler node is not running"
		} else if time.Since(node.lastRun) > s.config.PollInterval*3 {
			status = models.HealthStatusDegraded
			message = "Scheduler node is delayed"
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
			},
		}
		node.mu.RUnlock()
	}

	return health
}

func (s *Scheduler) schedulerLoop(node *SchedulerNode) {
	defer s.wg.Done()

	jitteredInterval := applyJitter(s.config.PollInterval, s.config.PollJitterPercent)
	s.log.Debug("Scheduler node %d using interval %v (base: %v, jitter: %d%%)",
		node.id, jitteredInterval, s.config.PollInterval, s.config.PollJitterPercent)

	ticker := time.NewTicker(jitteredInterval)
	defer ticker.Stop()

	s.markPendingMessagesReady(node)

	for {
		select {
		case <-ticker.C:
			s.markPendingMessagesReady(node)

		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Scheduler) markPendingMessagesReady(node *SchedulerNode) {
	s.log.Debug("Scheduler node %d checking for pending messages", node.id)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	count, err := s.store.MarkPendingMessagesAsReady(ctx, time.Now())

	node.mu.Lock()
	node.lastRun = time.Now()
	if err == nil {
		node.messagesMarked += count
	}
	node.mu.Unlock()

	if err != nil {
		s.log.Error("Scheduler node %d failed to mark pending messages: %v", node.id, err)
		return
	}

	s.log.Info("Scheduler node %d marked %d messages as ready", node.id, count)
}
