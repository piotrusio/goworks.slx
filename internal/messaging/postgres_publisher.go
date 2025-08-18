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
    if err := envelope.Validate(); err != nil {
        return fmt.Errorf("invalid event envelope: %w", err)
    }

    payloadJSON, err := normalizePayload(envelope.Payload)
    if err != nil {
        return fmt.Errorf("normalize payload: %w", err)
    }

    eventUUID, err := uuid.Parse(envelope.EventID)
    if err != nil {
        return fmt.Errorf("invalid event ID format: %w", err)
    }

    query := `
        INSERT INTO events (
            event_id, event_type, event_version, aggregate_key,
            change_version, timestamp, correlation_id, causation_id,
            user_id, payload
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
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
        payloadJSON,
    )
    if err != nil {
        return fmt.Errorf("failed to insert event: %w", err)
    }

    p.logger.Debug("event stored in PostgreSQL",
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

// normalizePayload ensures we store canonical JSON (no double-encoding)
func normalizePayload(v any) ([]byte, error) {
    switch t := v.(type) {
    case json.RawMessage:
        if !json.Valid(t) {
            return nil, fmt.Errorf("invalid json raw message")
        }
        return t, nil
    case []byte:
        if json.Valid(t) {
            return t, nil
        }
        // treat as plain value; marshal to JSON string
        return json.Marshal(string(t))
    case string:
        b := []byte(t)
        if json.Valid(b) {
            // already JSON object/array/primitive
            return b, nil
        }
        // marshal as JSON string
        return json.Marshal(t)
    default:
        return json.Marshal(v)
    }
}