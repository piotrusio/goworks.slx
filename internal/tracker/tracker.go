package tracker

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/salesworks/s-works/slx/internal/dispatcher"
	"github.com/salesworks/s-works/slx/internal/messaging"
	"gopkg.in/yaml.v3"
)

// TrackerRepository defines the interface for the tracker repository
type TrackerRepository interface {
	// RegisterAggregates registers the names of aggregates in the repository and sets change_version
	RegisterAggregates(ctx context.Context, aggregates []string) error
	// GetChangeVersion returns the last change version for the given aggregate name
	GetChangeVersion(ctx context.Context, aggregateName string) (int64, error)
	// UpdateChangeVersion updates the change version for the given aggregate name
	UpdateChangeVersion(ctx context.Context, aggregateName string, newVersion int64) error
}

// Config represents the configuration structure for loading aggregates from YAML
type Config struct {
	Aggregates []Aggregate `yaml:"aggregates"`
}

// Aggregate represents an aggregate with its name and query
type Aggregate struct {
	Name     string `yaml:"name"`
	Interval int    `yaml:"interval"`
	GetQuery string `yaml:"get_query"`
	// InsertCommand string `yaml:"insert_command"`
	// UpdateCommand string `yaml:"update_command"`
	// DeleteCommand string `yaml:"delete_command"`
}

// ChangeEvent represents a change event from the ERP system
type ChangeEvent struct {
	ChangeOperation string `json:"change_operation"`
	ChangeVersion   int64  `json:"change_version"`
	AggregateKey    string `json:"aggregate_key"`
	Payload         string `json:"payload"`
}

type Tracker struct {
	aggregates []Aggregate
	repository TrackerRepository
	logger     *slog.Logger
	db         *sql.DB
	dispatcher *dispatcher.Dispatcher
}

func NewTracker(
	ctx context.Context, aggregatesPath string, repo TrackerRepository,
	logger *slog.Logger, db *sql.DB, dispatcher *dispatcher.Dispatcher,
) (*Tracker, error) {
	yamlFile, err := os.ReadFile(aggregatesPath)
	if err != nil {
		logger.Error("failed to read aggregates file", "path", aggregatesPath, "error", err)
		return nil, fmt.Errorf("failed to read aggregates file: %w", err)
	}

	tracker := &Tracker{
		repository: repo,
		logger:     logger,
		db:         db,
		dispatcher: dispatcher,
	}

	var config Config

	decoder := yaml.NewDecoder(bytes.NewReader(yamlFile))
	decoder.KnownFields(true)
	err = decoder.Decode(&config)

	if err != nil {
		logger.Error("failed to unmarshal aggregates file", "path", aggregatesPath, "error", err)
		return nil, fmt.Errorf("failed to unmarshal aggregates file: %w", err)
	}

	tracker.aggregates = config.Aggregates

	aggregateNames := make([]string, len(tracker.aggregates))
	for i, aggregate := range tracker.aggregates {
		aggregateNames[i] = aggregate.Name
	}

	err = tracker.repository.RegisterAggregates(ctx, aggregateNames)
	if err != nil {
		logger.Error("failed to save aggregates", "error", err)
		return nil, fmt.Errorf("failed to save aggregates: %w", err)
	}

	logger.Info("tracker initialized", "aggregates_count", len(tracker.aggregates))
	return tracker, nil
}

func (t *Tracker) Start(ctx context.Context) error {
	for _, aggregate := range t.aggregates {

		// Start a goroutine for each aggregate to run erp changes cycle
		go func(ctx context.Context, agg Aggregate) {
			ticker := time.NewTicker(time.Duration(agg.Interval) * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					t.logger.Info("tracker stopping", "reason", ctx.Err())
					return
				case <-ticker.C:
					// runErpChangesCycle(agg.Name, agg.GetQuery)
					if err := t.runErpCycle(ctx, agg.Name, agg.GetQuery); err != nil {
						t.logger.Error(
							"erp change tracking cycle failed", "aggregate", agg.Name, "error", err,
						)
					}
				}
			}
		}(ctx, aggregate)

		// Start a goroutine for each aggregate to run app changes cycle
		go func(ctx context.Context, agg Aggregate) {
			ticker := time.NewTicker(time.Duration(agg.Interval) * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					t.logger.Info("tracker stopping", "reason", ctx.Err())
					return
				case <-ticker.C:
					// t.runAppChangesCycle(agg)
					t.logger.Debug("running app cycle for aggregate", "name", agg.Name)
				}
			}
		}(ctx, aggregate)
	}
	return nil
}

func (t *Tracker) runErpCycle(ctx context.Context, agregateName, getQuery string) error {
	lastVersion, err := t.repository.GetChangeVersion(ctx, agregateName)
	if err != nil {
		t.logger.Error("failed to get last change version", "aggregate", agregateName, "error", err)
		return fmt.Errorf("failed to get last change version: %w", err)
	}

	count, version, err := t.fetchErpChanges(ctx, agregateName, getQuery, lastVersion)
	if err != nil {
		t.logger.Error("failed to fetch ERP changes", "aggregate", agregateName, "error", err)
		return fmt.Errorf("failed to fetch ERP changes: %w", err)
	}
	if count == 0 {
		t.logger.Info("no changes found for aggregate", "name", agregateName)
		return nil
	}

	err = t.repository.UpdateChangeVersion(ctx, agregateName, version)
	if err != nil {
		t.logger.Error("failed to update change version", "aggregate", agregateName, "error", err)
		return fmt.Errorf("failed to update change version: %w", err)
	}

	t.logger.Info(
		"ERP cycle completed",
		"aggregate", agregateName,
		"change version", lastVersion,
		"records fetched", count,
		"updated change version", version,
	)

	return nil
}

func (t *Tracker) fetchErpChanges(ctx context.Context, name, query string, version int64) (int, int64, error) {
	// TODO: limit the number of records that can be returned, but do not cross the version boundary
	// version represenets the transaction in the erp system, if we set up blind limit to the select
	// statement we can crate a gap as one cycle will be limited to fetch only a part of the version
	// changes, update the version to the maxVersion and the next cycle will start from the Next
	// version
	rows, err := t.db.QueryContext(ctx, query, sql.Named("version", version))
	if err != nil {
		t.logger.Error("failed to execute query", "query", query, "error", err)
		return 0, 0, fmt.Errorf("query execution failed for query '%s': %w", query, err)
	}
	defer rows.Close()

	var counter int
	var maxVersion int64 = version
	for rows.Next() {
		var event ChangeEvent
		if err := rows.Scan(
			&event.ChangeOperation,
			&event.ChangeVersion,
			&event.AggregateKey,
			&event.Payload,
		); err != nil {
			t.logger.Error("failed to scan row", "error", err)
			return 0, 0, fmt.Errorf("row scan failed: %w", err)
		}
		if event.ChangeVersion > maxVersion {
			maxVersion = event.ChangeVersion
		}
		// dispatch the change event
		err := t.dispatchErpChange(event, name)
		if err != nil {
			t.logger.Error("failed to dispatch ERP change", "event", event, "error", err)
			return 0, 0, fmt.Errorf("failed to dispatch ERP change: %w", err)
		}
		counter++
	}
	if err := rows.Err(); err != nil {
		t.logger.Error("error encountered during row iteration", "error", err)
		return 0, 0, fmt.Errorf("row iteration error: %w", err)
	}

	return counter, maxVersion, nil
}

func (t *Tracker) dispatchErpChange(event ChangeEvent, agggergateName string) error {
	eventType := fmt.Sprintf("erp.%s.%s", agggergateName, event.ChangeOperation)
	eventChannel := fmt.Sprintf("erp.%s", agggergateName)

	envelope := messaging.NewEventEnvelope(
		eventType,
		event.AggregateKey,
		event.ChangeVersion,
		event.Payload,
	)

	err := envelope.Validate()
	if err != nil {
		return fmt.Errorf("invalid event envelope: %w", err)
	}

	job := dispatcher.Job{
		EventChannel:  eventChannel,
		EventEnvelope: envelope,
	}

	t.dispatcher.Dispatch(job)
	return nil
}
