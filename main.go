package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jaysongiroux/smq/internal/bufferfactory"
	"github.com/jaysongiroux/smq/internal/config"
	"github.com/jaysongiroux/smq/internal/consumer"
	"github.com/jaysongiroux/smq/internal/dbfactory"
	"github.com/jaysongiroux/smq/internal/health"
	"github.com/jaysongiroux/smq/internal/logger"
	"github.com/jaysongiroux/smq/internal/middleware"
	"github.com/jaysongiroux/smq/internal/models"
	"github.com/jaysongiroux/smq/internal/producer"
	"github.com/jaysongiroux/smq/internal/scheduler"
	"github.com/jaysongiroux/smq/internal/utils"
)

func main() {
	// Load configuration
	cfg, err := config.NewConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Validate configuration before starting
	if err := cfg.Validate(); err != nil {
		fmt.Printf("Configuration validation failed: %v\n", err)
		os.Exit(1)
	}

	// Log any configuration overrides from environment variables
	cfg.LogOverrides()

	// Initialize logger with config
	log := logger.New("main", cfg)

	// Generate unique node ID for this instance
	log.Info("Starting SMQ node: %s", cfg.NodeID)
	log.Info("Using datastore: %s", cfg.Datastore)
	log.Debug("Log level configured from %s", cfg.ConfigPath)

	// Create database store using factory (automatically selects correct implementation)
	dbLog := log.WithService("database")
	store, err := dbfactory.NewStore(cfg, dbLog)
	if err != nil {
		log.Fatal("Failed to initialize database: %v", err)
	}
	defer func() {
		err = store.Close()
		if err != nil {
			log.Error("Failed to close database: %v", err)
		}
	}()
	log.Info("Database connection established")

	// Register this node in the database (upsert)
	node := &models.Node{
		NodeID:       cfg.NodeID,
		Status:       "starting",
		LastSeen:     time.Now(),
		RegisteredAt: time.Now(),
		Metadata:     map[string]interface{}{},
	}

	ctx := context.Background()
	if err := store.RegisterNode(ctx, node); err != nil {
		log.Fatal("Failed to register node in database: %v", err)
	}
	log.Info("Node registered in database with status: starting")

	// Initialize buffer for batching messages (memory or disk-backed)
	bufferLog := log.WithService("buffer")
	messageBuffer, err := bufferfactory.NewBuffer(cfg, store, bufferLog)
	if err != nil {
		log.Fatal("Failed to initialize buffer: %v", err)
	}
	messageBuffer.Start()
	defer func() {
		err = messageBuffer.Stop()
		if err != nil {
			log.Error("Failed to stop buffer: %v", err)
		}
	}()

	// Initialize scheduler for background processing
	schedulerConfig := &scheduler.SchedulerConfig{
		PollInterval:           time.Duration(cfg.SchedulerPollIntervalMs) * time.Millisecond,
		PollJitterPercent:      cfg.SchedulerPollJitterPercent,
		StaleAcquiredThreshold: time.Duration(cfg.MsgTimeoutMs) * time.Millisecond,
		JanitorInterval:        time.Duration(cfg.SchedulerJanitorIntervalMs) * time.Millisecond,
		JanitorJitterPercent:   cfg.SchedulerJanitorJitterPercent,
		StaleNodeThreshold:     time.Duration(cfg.HealthCheckIntervalMs) * time.Millisecond * 2, // Remove nodes after 2x health check interval
	}
	schedulerLog := log.WithService("scheduler")
	msgScheduler := scheduler.NewScheduler(schedulerConfig, store, cfg.NumSchedulerNodes, cfg.NumSchedulerJanitorNodes, schedulerLog)
	msgScheduler.Start()
	defer func() {
		err = msgScheduler.Stop()
		if err != nil {
			log.Error("Failed to stop scheduler: %v", err)
		}
	}()
	log.Info("Scheduler layer initialized with %d scheduler nodes and %d janitor nodes", cfg.NumSchedulerNodes, cfg.NumSchedulerJanitorNodes)

	// Initialize producer layer
	producerLog := log.WithService("producer")
	prod := producer.NewProducer(store, messageBuffer, producerLog, cfg.MaxPayloadSizeKb, cfg)
	if err := prod.Start(); err != nil {
		log.Fatal("Failed to start producer: %v", err)
	}
	defer func() {
		err = prod.Stop()
		if err != nil {
			log.Error("Failed to stop producer: %v", err)
		}
	}()
	log.Info("Producer layer initialized")

	// Initialize consumer layer
	consumerLog := log.WithService("consumer")
	cons := consumer.NewConsumer(store, cfg.NodeID, consumerLog)
	if err := cons.Start(); err != nil {
		log.Fatal("Failed to start consumer: %v", err)
	}
	defer func() {
		err = cons.Stop()
		if err != nil {
			log.Error("Failed to stop consumer: %v", err)
		}
	}()
	log.Info("Consumer layer initialized")

	// Initialize health checker with check interval
	healthLog := log.WithService("health")
	healthChecker := health.NewHealthChecker(
		cfg,
		store,
		cfg.NodeID,
		time.Duration(cfg.HealthCheckIntervalMs)*time.Millisecond,
		healthLog,
	)

	// Register health reporters for each layer
	healthChecker.RegisterReporter(prod)
	healthChecker.RegisterReporter(cons)
	healthChecker.RegisterReporter(messageBuffer)
	healthChecker.RegisterSchedulerHealth(msgScheduler.Health)

	if err := healthChecker.Start(); err != nil {
		log.Fatal("Failed to start health checker: %v", err)
	}
	defer func() {
		err = healthChecker.Stop()
		if err != nil {
			log.Error("Failed to stop health checker: %v", err)
		}
	}()
	log.Info("Health checker initialized")

	// Get API key for auth middleware
	if cfg.ApiKey == "" {
		log.Fatal("api_key not configured - cannot start servers")
	}
	authLog := log.WithService("auth")
	authMiddleware := middleware.AuthMiddleware(cfg.ApiKey, authLog)
	log.Info("Auth middleware configured")

	var servers []*http.Server

	// Start producer server
	producerPort := cfg.ProducerPort
	producerMux := http.NewServeMux()
	producerHandler := producer.NewHandler(prod, producerLog)
	producerHandler.RegisterRoutes(producerMux)
	producerWithMiddleware := middleware.LoggingMiddleware(producerLog)(authMiddleware(producerMux))
	producerServer := utils.StartHTTPServer(fmt.Sprintf(":%d", producerPort), producerWithMiddleware, producerLog)
	servers = append(servers, producerServer)
	log.Info("Producer server started on port %d", producerPort)

	// Start a consumer server
	consumerMux := http.NewServeMux()
	consumerHandler := consumer.NewHandler(cons, consumerLog)
	consumerHandler.RegisterRoutes(consumerMux)
	consumerWithMiddleware := middleware.LoggingMiddleware(consumerLog)(authMiddleware(consumerMux))
	consumerServer := utils.StartHTTPServer(fmt.Sprintf(":%d", cfg.ConsumerPort), consumerWithMiddleware, consumerLog)
	servers = append(servers, consumerServer)
	log.Info("Consumer server started on port %d", cfg.ConsumerPort)

	// Start a health server
	healthMux := http.NewServeMux()
	healthHandler := health.NewHandler(healthChecker, healthLog)
	healthHandler.RegisterRoutes(healthMux)
	healthWithMiddleware := middleware.LoggingMiddleware(healthLog)(authMiddleware(healthMux))
	healthServer := utils.StartHTTPServer(fmt.Sprintf(":%d", cfg.HealthPort), healthWithMiddleware, healthLog)
	servers = append(servers, healthServer)
	log.Info("Health server started on port %d", cfg.HealthPort)

	// Wait for interrupt signal to gracefully shutdown all servers
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down all servers...")

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Shutdown all servers gracefully
	for _, server := range servers {
		if err := server.Shutdown(ctx); err != nil {
			log.Warn("Server forced to shutdown: %v", err)
		}
	}

	log.Info("All servers stopped")
}
