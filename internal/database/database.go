package database

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

// SQLServer implements the storage.StateStore interface for Microsoft SQL Server.
type SQLServer struct {
	Pool   *sql.DB
	logger *slog.Logger
}

// New creates a new SQL Server database connection pool.
func New(
	ctx context.Context, uri string, maxOpenConns, maxIdleConns int,
	maxIdleTime time.Duration, logger *slog.Logger,
) (*SQLServer, error) {
	logger = logger.With("component", "database", "type", "sqlserver")

	if uri == "" {
		return nil, fmt.Errorf("database uri string is empty")
	}

	maxRetries := 5
	baseDelay := 5 * time.Second

	logger.Info("Attempting to establish database connection...",
		"max_attempts", maxRetries,
		"base_delay", baseDelay)

	var pool *sql.DB
	var err error

	for attempt := 0; attempt < maxRetries; attempt++ {
		logger.Info("Database connection attempt", "attempt", attempt+1)

		pool, err = sql.Open("sqlserver", uri)
		if err != nil {
			logger.Warn("sql.Open failed", "attempt", attempt+1, "error", err.Error())

			if attempt < maxRetries-1 {
				delay := baseDelay * time.Duration(1<<attempt) // 2s, 4s, 8s, 16s...
				logger.Info("Retrying after delay", "delay", delay)

				select {
				case <-time.After(delay):
					continue
				case <-ctx.Done():
					return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
				}
			}
			break
		}

		pool.SetMaxOpenConns(maxOpenConns)
		pool.SetMaxIdleConns(maxIdleConns)
		pool.SetConnMaxIdleTime(maxIdleTime)

		pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		pingErr := pool.PingContext(pingCtx)
		cancel()

		if pingErr != nil {
			pool.Close()
			logger.Warn("ping failed", "attempt", attempt+1, "error", pingErr.Error())

			if attempt < maxRetries-1 {
				delay := baseDelay * time.Duration(1<<attempt)
				logger.Info("Retrying after delay", "delay", delay)

				select {
				case <-time.After(delay):
					continue
				case <-ctx.Done():
					return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
				}
			}
			err = pingErr
			break
		}

		logger.Info("Database connection established successfully")
		return &SQLServer{Pool: pool, logger: logger}, nil
	}

	return nil, fmt.Errorf("database connection failed after %d attempts: %w", maxRetries, err)
}

func (s *SQLServer) Close() {
	if s.Pool != nil {
		s.logger.Info("closing database connection pool.")
		// sql.DB.Close() waits for connections to be returned before closing.
		if err := s.Pool.Close(); err != nil {
			s.logger.Error("error closing database connection pool",
				"error", err)
		}
	}
}
