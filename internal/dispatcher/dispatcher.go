package dispatcher

import (
	"context"
	"log/slog"
	"sync"

	"github.com/salesworks/s-works/slx/internal/messaging"
)

// Publisher defines the interface for any service that can publish an event to an external system
type Publisher interface {
	Publish(ctx context.Context, subject string, envelope *messaging.EventEnvelope) error
	Close() error
}

// Job represents a unit of work for the dispatcher.
// It wraps the AggregateEvent with routing information.
type Job struct {
	EventChannel  string
	EventEnvelope *messaging.EventEnvelope
}

// Dispatcher manages a pool of workers to process jobs from a queue.
type Dispatcher struct {
	numWorkers   int
	jobQueue     chan Job
	publisher    Publisher
	workerWg     sync.WaitGroup
	shutdownOnce sync.Once
	logger       *slog.Logger
}

// NewDispatcher creates and initializes a new Dispatcher.
func NewDispatcher(numWorkers, queSize int, publisher Publisher, logger *slog.Logger) *Dispatcher {
	return &Dispatcher{
		numWorkers: numWorkers,
		jobQueue:   make(chan Job, queSize),
		publisher:  publisher,
		logger:     logger.With("component", "dispatcher"),
	}
}

// Start launches the worker pool.
func (d *Dispatcher) Start() {
	d.workerWg.Add(d.numWorkers)
	for i := 0; i < d.numWorkers; i++ {
		go d.worker(i + 1)
	}
}

// Worker is the core logic for a single worker goroutine.
// It takes NewTextHandler payload directly from the job and sends it to the publisher.
func (d *Dispatcher) worker(id int) {
	defer d.workerWg.Done()
	d.logger.Debug("worker started", "worker_id", id)

	ctx := context.Background()

	for job := range d.jobQueue {
		if err := d.publisher.Publish(ctx, job.EventChannel, job.EventEnvelope); err != nil {
			d.logger.Error(
				"failed to publish event",
				"error", err,
				"channel", job.EventChannel,
				"event", job.EventEnvelope,
			)
			continue
		}
	}
	d.logger.Debug("worker finished", "worker_id", id)
}

// Dispatch adds a new job to the processing queue.
func (d *Dispatcher) Dispatch(job Job) {
	d.jobQueue <- job
}

// Stop initiates a graceful shutdown of the dispatcher.
func (d *Dispatcher) Stop() {
	d.shutdownOnce.Do(func() {
		d.logger.Info("dispatcher stopping... waiting for workers to finish.")
		close(d.jobQueue)
		d.workerWg.Wait()
		d.logger.Info("all workers have finished, dispatcher stopped")
	})
}
