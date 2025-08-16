package database

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Postgres struct {
	Pool *sql.DB
	logger *slog.Logger
}

func NewPostgres(ctx context.Context, uri string, logger *slog.Logger) (*Postgres, error) {
	if uri  == "" {
		return nil, fmt.Errorf("postgres uri is empty")
	}

	pool, err := sql.Open("pgx", uri)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connecttion %w", err)
	}

	pool.SetConnMaxIdleTime(5 * time.Minute)
	pool.SetMaxOpenConns(10)
	pool.SetMaxIdleConns(10)

	// verify the connection with ping and timeout
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = pool.PingContext(pingCtx)
	if err != nil {
		// ensure pool is closed if ping fails to prvent leaks
		if closeErr := pool.Close(); closeErr != nil {
			logger.Error("Failed to close pool after ping error", "close error", closeErr)
		}
		logger.Error("Database ping failed", "error", err)
		// skip wrappeing the original driver error to avoid leaking details
		return nil, fmt.Errorf("unable to verify database connection")
	}

	logger.Info("postgres connection pool established")
	return &Postgres{Pool: pool, logger: logger}, nil
}

// close gracefully the database connection pool
func(db *Postgres) Close() {
	if db.Pool != nil {
		db.logger.Info("Closing database connection pool")
		if err := db.Pool.Close(); err != nil {
			db.logger.Error("Error closing database connection pool", "error", err)
		}
	}
}