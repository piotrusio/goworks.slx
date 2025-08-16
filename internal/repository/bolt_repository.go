package repository

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"go.etcd.io/bbolt"
)

// BBoltRepository implements TrackerRepository using BBolt
type BBoltRepository struct {
	db     *bbolt.DB
	logger *slog.Logger
}

// NewSimpleBBoltRepository creates a new BBolt repository
func NewBBoltRepository(dbPath string, logger *slog.Logger) (*BBoltRepository, error) {
	// Open database (creates if doesn't exist)
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	repo := &BBoltRepository{
		db:     db,
		logger: logger,
	}

	return repo, nil
}

// RegisterAggregates inserts aggregate names with counter = 0
func (r *BBoltRepository) RegisterAggregates(ctx context.Context, aggregates []string) error {
	return r.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("aggregates"))
		if err != nil {
			return err
		}

		for _, name := range aggregates {
			existing := b.Get([]byte(name))
			if existing == nil {
				b.Put([]byte(name), []byte("0"))
				r.logger.Info("aggregate registered", "name", name, "version", "0")
			} else {
				r.logger.Info("aggregate registered", "name", name, "version", string(existing))
			}
		}
		return nil
	})
}

// GetChangeVersion returns the last change version for the given aggregate name
func (r *BBoltRepository) GetChangeVersion(ctx context.Context, aggregateName string) (int64, error) {
	var version int64

	err := r.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("aggregates"))
		if b == nil {
			return fmt.Errorf("aggregates bucket not found")
		}

		v := b.Get([]byte(aggregateName))
		if v == nil {
			return fmt.Errorf("aggregate '%s' not found", aggregateName)
		}

		// Parse string value to int64
		parsedVersion, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse version for aggregate '%s': %w", aggregateName, err)
		}

		version = parsedVersion
		return nil
	})

	return version, err
}

// UpdateChangeVersion updates the change version for the given aggregate name
func (r *BBoltRepository) UpdateChangeVersion(ctx context.Context, aggregateName string, newVersion int64) error {
	return r.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("aggregates"))
		if b == nil {
			return fmt.Errorf("aggregates bucket not found")
		}

		// Check if aggregate exists
		existing := b.Get([]byte(aggregateName))
		if existing == nil {
			return fmt.Errorf("aggregate '%s' not found", aggregateName)
		}

		// Convert version to string and store
		versionStr := strconv.FormatInt(newVersion, 10)
		err := b.Put([]byte(aggregateName), []byte(versionStr))
		if err != nil {
			return fmt.Errorf("failed to update version for aggregate '%s': %w", aggregateName, err)
		}

		return nil
	})
}

// Close closes the database
func (r *BBoltRepository) Close() error {
	return r.db.Close()
}
