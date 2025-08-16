package dispatcher

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/salesworks/s-works/slx/internal/messaging"
)

// mockPublisher records Publish invocations and signals a WaitGroup.
type mockPublisher struct {
	mu    sync.Mutex
	wg    *sync.WaitGroup
	calls int
}

func (m *mockPublisher) Publish(_ context.Context, _ string, _ *messaging.EventEnvelope) error {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	if m.wg != nil {
		m.wg.Done()
	}
	return nil
}

func (m *mockPublisher) Close() error { return nil }

// TestDispatcherProcessesJobs verifies that all dispatched jobs are published.
func TestDispatcher_ProcessesJobs(t *testing.T) {
	const (
		numWorkers = 3
		queueSize  = 10
		numJobs    = 8
	)

	var wg sync.WaitGroup
	wg.Add(numJobs)

	mp := &mockPublisher{wg: &wg}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	d := NewDispatcher(numWorkers, queueSize, mp, logger)
	d.Start()

	event := messaging.NewEventEnvelope(
		"test.created",
		"C4CA4238A0B923820DCC509A6F75849A",
		1,
		"{}",
	)
	// Dispatch jobs.
	for i := 0; i < numJobs; i++ {
		d.Dispatch(Job{EventChannel: "test", EventEnvelope: event})
	}

	// Wait for all jobs or timeout.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("dispatcher did not process all jobs in time")
	}

	d.Stop()
	d.Stop() // should not panic or deadlock

	mp.mu.Lock()
	defer mp.mu.Unlock()
	if mp.calls != numJobs {
		t.Fatalf("expected %d Publish calls, got %d", numJobs, mp.calls)
	}
}
