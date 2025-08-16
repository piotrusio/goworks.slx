package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/salesworks/s-works/slx/internal/database"
	"github.com/salesworks/s-works/slx/internal/dispatcher"
	"github.com/salesworks/s-works/slx/internal/messaging"
	"github.com/salesworks/s-works/slx/internal/repository"
	"github.com/salesworks/s-works/slx/internal/tracker"
	"gopkg.in/natefinch/lumberjack.v2"
)

type config struct {
	db      dbConfig
	pg		pgConfig
	disp    dispatcherConfig
	aggPath string
}

type pgConfig struct {
	uri string
}

type dbConfig struct {
	uri          string
	maxOpenConns int
	maxIdleConns int
	maxIdleTime  time.Duration
	path         string
}

type dispatcherConfig struct {
	numWorkers   int
	jobQueueSize int
}

func Run(env string, logPath string) error {
	cfg := loadConfig()

	logger := newLogger(env, logPath)
	logger.Info("SLX Service starting", "env", env)

	appCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// context for external services startup
	startupCtx, startupCancel := context.WithTimeout(appCtx, 200*time.Second)
	defer startupCancel()

	// Initialize the SQL Server database connection pool
	db, err := database.New(
		startupCtx,
		cfg.db.uri,
		cfg.db.maxOpenConns,
		cfg.db.maxIdleConns,
		cfg.db.maxIdleTime,
		logger,
	)
	if err != nil {
		logger.Error("failed to initialized sqlserver database", "error", err)
		return fmt.Errorf("failed to connect to sqlserver database: %w", err)
	}
	defer func() {
		db.Close()
		logger.Info("database connection pool closed")
	}()
	logger.Info("SQL Server database initialized")

	// Initialize Postgres
	postgres, err := database.NewPostgres(startupCtx, cfg.pg.uri, logger)
	if err != nil {
		logger.Error("failed to initialize postgres database", "error", err)
		return fmt.Errorf("failed to connect to postgres database %w", err)
	}
	defer func() {
		postgres.Close()
		logger.Info("postgres database connection pool closed")
	}()
	logger.Info("succesfully connected to postgres database")

	publisher := messaging.NewPostgresPublisher(postgres.Pool, logger)
	defer publisher.Close()
	logger.Info("Postgres publisher initialized")


	// Initialize Dispatcher
	disp := dispatcher.NewDispatcher(cfg.disp.numWorkers, cfg.disp.jobQueueSize, publisher, logger)
	disp.Start()
	defer func() {
		logger.Info("stopping dispatcher...")
		disp.Stop()
		logger.Info("dispatcher stopped")
	}()
	logger.Info("dispatcher initialized", "numWorkers", cfg.disp.numWorkers, "jobQueueSize", cfg.disp.jobQueueSize)

	// Register Aggregates
	repo, err := repository.NewBBoltRepository(cfg.db.path, logger)
	if err != nil {
		logger.Error("failed to initialize repository", "error", err)
		return fmt.Errorf("failed to initialize repository: %w", err)
	}
	defer func() {
		logger.Info("closing repository...")
		repo.Close()
		logger.Info("repository closed")
	}()

	// Initialize Tracker
	trackerInstance, err := tracker.NewTracker(startupCtx, cfg.aggPath, repo, logger, db.Pool, disp)
	if err != nil {
		logger.Error("failed to initialize tracker", "error", err)
		return fmt.Errorf("failed to initialize tracker: %w", err)
	}
	err = trackerInstance.Start(appCtx)
	if err != nil {
		logger.Error("failed to start tracker", "error", err)
		return fmt.Errorf("failed to start tracker: %w", err)
	}
	logger.Info("tracker started")

	// Wait for shutdown signal
	<-appCtx.Done()
	logger.Info("SLX Service shutdown initiated", "reason", appCtx.Err())

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	logger.Info("waiting for graceful shutdown...")
	
	// Give some time for ongoing operations to complete
	select {
	case <-time.After(2 * time.Second):
		logger.Info("graceful shutdown period completed")
	case <-shutdownCtx.Done():
		logger.Warn("shutdown timeout reached")
	}

	logger.Info("All background processes have finished. Application shut down gracefully.")
	return nil
}

func newLogger(env string, logPath string) *slog.Logger {
	var handler slog.Handler

	if env == "development" {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
	} else {
		lumberjackLogger := &lumberjack.Logger{
			Filename:   logPath,
			MaxSize:    100,
			MaxBackups: 3,
			MaxAge:     28,
			Compress:   true,
		}

		handler = slog.NewTextHandler(lumberjackLogger, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
	}
	return slog.New(handler)
}

func loadConfig() config {
	var cfg config

	cfg.pg.uri = os.Getenv("POSTGRES_URI")
	if cfg.pg.uri == "" {
		panic("POSTGRES_URI must be set in production environment")
	}

	cfg.db.uri = os.Getenv("SQLSERVER_URI")
	if cfg.db.uri == "" {
		panic("SQLSERVER_URI must be set in production environment")
	}

	openConns, _ := strconv.Atoi(os.Getenv("DB_MAX_OPEN_CONNS"))
	if openConns == 0 {
		openConns = 10
	}
	cfg.db.maxOpenConns = openConns

	idleConns, _ := strconv.Atoi(os.Getenv("DB_MAX_IDLE_CONNS"))
	if idleConns == 0 {
		idleConns = 10
	}
	cfg.db.maxIdleConns = idleConns

	idleTime, err := time.ParseDuration(os.Getenv("DB_MAX_IDLE_TIME"))
	if err != nil {
		idleTime = 5 * time.Minute
	}
	cfg.db.maxIdleTime = idleTime

	numberWorkers, err := strconv.Atoi(os.Getenv("DISPATCHER_NUM_WORKERS"))
	if err != nil || numberWorkers <= 0 {
		numberWorkers = 10
	}
	cfg.disp.numWorkers = numberWorkers

	jobQueueSize, err := strconv.Atoi(os.Getenv("DISPATCHER_JOB_QUEUE_SIZE"))
	if err != nil || jobQueueSize <= 0 {
		jobQueueSize = 100
	}
	cfg.disp.jobQueueSize = jobQueueSize

	cfg.db.path = os.Getenv("DB_PATH")
	if cfg.db.path == "" {
		panic("DB_PATH must be set in production environment")
	}

	cfg.aggPath = os.Getenv("AGG_PATH")
	if cfg.aggPath == "" {
		panic("AGG_PATH must be set in production environment")
	}

	return cfg
}
