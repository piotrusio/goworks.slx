package messaging

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
)

// PostgresPublisher is an implementation of the Publisher interface that stores events
// in a PostgreSQL database table.
type PostgresPublisher struct {
	db     *sql.DB
	logger *slog.Logger
}

// NewPostgresPublisher creates a new PostgreSQL event publisher
func NewPostgresPublisher(db *sql.DB, logger *slog.Logger) *PostgresPublisher {
	return &PostgresPublisher{
		db:     db,
		logger: logger.With("component", "PostgresPublisher"),
	}
}

// Publish stores an event envelope in the PostgreSQL events table
func (p *PostgresPublisher) Publish(ctx context.Context, subject string, envelope *EventEnvelope) error {
	// Validate the envelope
	if err := envelope.Validate(); err != nil {
		return fmt.Errorf("invalid event envelope: %w", err)
	}

	// Serialize the payload to JSON
	payloadBytes, err := json.Marshal(envelope.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Parse event ID as UUID
	eventUUID, err := uuid.Parse(envelope.EventID)
	if err != nil {
		return fmt.Errorf("invalid event ID format: %w", err)
	}

	// Insert the event into the database
	query := `
		INSERT INTO erp_events (
			event_id, event_type, event_version, aggregate_key, 
			change_version, timestamp, correlation_id, causation_id, 
			user_id, payload
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`

	_, err = p.db.ExecContext(
		ctx,
		query,
		eventUUID,
		envelope.EventType,
		envelope.EventVersion,
		envelope.AggregateKey,
		envelope.ChangeVersion,
		envelope.Timestamp,
		nullStringFromPtr(envelope.CorrelationID),
		nullStringFromPtr(envelope.CausationID),
		nullStringFromPtr(envelope.UserID),
		payloadBytes,
	)

	if err != nil {
		return fmt.Errorf("failed to insert event into database: %w", err)
	}

	p.logger.Debug(
		"event stored in PostgreSQL",
		"subject", subject,
		"event_type", envelope.EventType,
		"aggregate_key", envelope.AggregateKey,
	)

	return nil
}

func (p *PostgresPublisher) Close() error {
	if p.db != nil {
		p.logger.Info("closing PostgreSQL connection")
		return p.db.Close()
	}
	return nil
}

// Helper function to convert string to sql.NullString
func nullStringFromPtr(s string) sql.NullString {
	if s == "" {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: s, Valid: true}
}