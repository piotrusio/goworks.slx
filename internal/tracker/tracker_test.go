package tracker

import (
	"context"
	"io"
	"log/slog"
	"os"
	"regexp"
	"runtime"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/salesworks/s-works/slx/internal/dispatcher"
	"github.com/salesworks/s-works/slx/internal/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockPublisher struct {
	PublishCalled bool
	PublishCalls  int
	errToReturn   error
}

func (m *mockPublisher) Publish(
	ctx context.Context, subject string, envelope *messaging.EventEnvelope,
) error {
	if m.errToReturn != nil {
		return m.errToReturn
	}
	m.PublishCalled = true
	m.PublishCalls++
	return nil
}

func (m *mockPublisher) Close() error {
	return nil
}

type mockTrackerRepository struct {
	RegisterAggregatesCalled  bool
	GetChangeVersionCalled    bool
	UpdateChangeVersionCalled bool
	errToReturn               error
}

func (m *mockTrackerRepository) RegisterAggregates(ctx context.Context, aggregates []string) error {
	if m.errToReturn != nil {
		return m.errToReturn
	}
	m.RegisterAggregatesCalled = true
	return nil
}

func (m *mockTrackerRepository) GetChangeVersion(
	ctx context.Context, aggregateName string,
) (int64, error) {
	if m.errToReturn != nil {
		return 0, m.errToReturn
	}
	m.GetChangeVersionCalled = true
	return 1, nil
}

func (m *mockTrackerRepository) UpdateChangeVersion(
	ctx context.Context, aggregateName string, newVersion int64,
) error {
	if m.errToReturn != nil {
		return m.errToReturn
	}
	m.UpdateChangeVersionCalled = true
	return nil
}

func TestTracker_NewTracker_HappyPath(t *testing.T) {
	// --- Arrange ---
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()
	publisher := &mockPublisher{}
	trackerRepo := &mockTrackerRepository{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	tempDir := t.TempDir()
	testFile := tempDir + "/aggregates.yaml"
	testContent := `aggregates:
  - name: "fabric"
    interval: 60
    get_query: |
      SELECT *
      FROM CHANGETABLE(CHANGES ERPXL_GO.CDN.TwrKarty, @version) AS c

  - name: "customer"
    interval: 30
    get_query: |
      SELECT *
      FROM CHANGETABLE(CHANGES ERPXL_GO.CDN.KntKarty, @version) AS c
`

	err = os.WriteFile(testFile, []byte(testContent), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	dispatcher := dispatcher.NewDispatcher(1, 10, publisher, logger)
	tracker, err := NewTracker(ctx, testFile, trackerRepo, logger, db, dispatcher)
	require.NoError(t, err, "NewTracker should not return an error")

	// --- Assert ---
	assert.NotNil(t, tracker, "ChangeTracker should not be nil")
	assert.Equal(t, trackerRepo, tracker.repository, "expected tracker.repository to be set")
	assert.True(t, trackerRepo.RegisterAggregatesCalled, "registerAggregates should be called")
	assert.Equal(t, 2, len(tracker.aggregates), "expected 2 aggregates")
	assert.Equal(t, "fabric", tracker.aggregates[0].Name, "expected aggregate name to be 'fabric'")
	assert.Equal(t, "customer", tracker.aggregates[1].Name, "expected aggregate name to be 'customer'")
	assert.Equal(
		t,
		"SELECT *\nFROM CHANGETABLE(CHANGES ERPXL_GO.CDN.TwrKarty, @version) AS c\n",
		tracker.aggregates[0].GetQuery,
		"Expected first aggregate get_query to match",
	)
	assert.Equal(
		t,
		"SELECT *\nFROM CHANGETABLE(CHANGES ERPXL_GO.CDN.KntKarty, @version) AS c\n",
		tracker.aggregates[1].GetQuery,
		"Expected second aggregate get_query to match",
	)
}

// TestTracker_NewTracker_InvalidFile tests the error handling when the aggregates file is invalid.
func TestTracker_NewTracker_InvalidFile(t *testing.T) {
	// --- Arrange ---
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()
	publisher := &mockPublisher{}
	trackerRepo := &mockTrackerRepository{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	tempDir := t.TempDir()
	testFile := tempDir + "/invalid_aggregates.yaml"
	testContent := `aggregates:
  - name: "fabric"
  - name: "customer"
    extra_field: "unexpected"  # This should cause an error
`

	err = os.WriteFile(testFile, []byte(testContent), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// --- Act ---
	dispatcher := dispatcher.NewDispatcher(1, 10, publisher, logger)
	tracker, err := NewTracker(ctx, testFile, trackerRepo, logger, db, dispatcher)

	// --- Assert ---
	assert.Nil(t, tracker, "changeTracker should be nil")
	assert.Error(t, err, "expected an error due to invalid YAML structure")
}

func TestTracker_Start_TrackErpChanges(t *testing.T) {
	// --- Arrange ---
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()
	publisher := &mockPublisher{}
	trackerRepo := &mockTrackerRepository{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	dispatcher := dispatcher.NewDispatcher(1, 10, publisher, logger)
	tracker := &Tracker{
		aggregates: []Aggregate{
			{
				Name:     "fabric",
				Interval: 1,
				GetQuery: "SELECT * FROM ERPXL_GO.CDN.TwrKarty",
			},
			{
				Name:     "customer",
				Interval: 1,
				GetQuery: "SELECT * FROM ERPXL_GO.CDN.KntKarty",
			},
		},
		repository: trackerRepo,
		logger:     logger,
		db:         db,
		dispatcher: dispatcher,
	}

	// --- Act ---
	initialCount := runtime.NumGoroutine()
	err = tracker.Start(ctx)
	require.NoError(t, err, "Start should not return an error")
	// Allow some time for goroutines to start
	time.Sleep(100 * time.Millisecond)
	afterCount := runtime.NumGoroutine()
	<-ctx.Done()

	// --- Assert ---
	expectedIncrease := len(tracker.aggregates) * 2
	actualIncrease := afterCount - initialCount
	assert.Equal(
		t, expectedIncrease, actualIncrease,
		"expected goroutines to increase by %d, got %d", expectedIncrease, actualIncrease,
	)
}

func TestTracker_RunErpCycle(t *testing.T) {
	// --- Arrange ---
	publisher := &mockPublisher{}
	trackerRepo := &mockTrackerRepository{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	dispatcher := dispatcher.NewDispatcher(1, 10, publisher, logger)
	tracker := &Tracker{
		aggregates: []Aggregate{{Name: "fabric"}, {Name: "customer"}},
		repository: trackerRepo,
		logger:     logger,
		db:         db,
		dispatcher: dispatcher,
	}
	query := "SELECT * FROM changes WHERE version > @version"
	// version := int64(0)

	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(sqlmock.AnyArg()).WillReturnRows(
		sqlmock.NewRows([]string{"change_operation", "change_version", "aggregate_key", "payload"}).
			AddRow("I", 1, "C4CA4238A0B923820DCC509A6F75849A", `{}`).
			AddRow("U", 1, "C4CA4238A0B923820DCC509A6F75849B", `{}`).
			AddRow("D", 1, "C4CA4238A0B923820DCC509A6F75849C", `{}`),
	)
	// --- Act ---
	err = tracker.runErpCycle(ctx, "fabric", query)
	require.NoError(t, err, "runErpCycle should start without error")

	// --- Assert ---
	assert.True(t, trackerRepo.GetChangeVersionCalled, "GetChangeVersion should be called")
	assert.True(t, trackerRepo.UpdateChangeVersionCalled, "UpdateChangeVersion should not be called")
}

func TestTracker_FetchErpChanges(t *testing.T) {
	// --- Arrange ---
	publisher := &mockPublisher{}
	trackerRepo := &mockTrackerRepository{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	dispatcher := dispatcher.NewDispatcher(1, 10, publisher, logger)
	tracker := &Tracker{
		aggregates: []Aggregate{{Name: "fabric"}, {Name: "customer"}},
		repository: trackerRepo,
		logger:     logger,
		db:         db,
		dispatcher: dispatcher,
	}
	query := "SELECT * FROM changes WHERE version > @version"
	version := int64(0)

	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(sqlmock.AnyArg()).WillReturnRows(
		sqlmock.NewRows([]string{"change_operation", "change_version", "aggregate_key", "payload"}).
			AddRow("I", 1, "C4CA4238A0B923820DCC509A6F75849A", `{}`).
			AddRow("U", 1, "C4CA4238A0B923820DCC509A6F75849B", `{}`).
			AddRow("D", 1, "C4CA4238A0B923820DCC509A6F75849C", `{}`),
	)

	// trackerRepo := &mockTrackerRepository{}
	// logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	// --- Act ---
	counter, ver, err := tracker.fetchErpChanges(ctx, tracker.aggregates[0].Name, query, version)
	require.NoError(t, err, "runErpCycle should start without error")

	// --- Assert ---
	assert.Equal(t, counter, 3, "fetchErpChanges should return 3 change events")
	assert.Equal(t, int64(1), ver, "fetchErpChanges should return new version 1")
}

func TestTracker_ErpDispatcher(t *testing.T) {
	// --- Arrange ---
	publisher := &mockPublisher{}
	trackerRepo := &mockTrackerRepository{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dispatcher := dispatcher.NewDispatcher(1, 10, publisher, logger)

	tracker := &Tracker{
		aggregates: []Aggregate{{Name: "fabric"}, {Name: "customer"}},
		repository: trackerRepo,
		logger:     logger,
		dispatcher: dispatcher,
	}

	correctEvent := ChangeEvent{
		ChangeOperation: "created",
		ChangeVersion:   1,
		AggregateKey:    "C4CA4238A0B923820DCC509A6F75849A",
		Payload:         `{}`,
	}

	corruptedEvent := ChangeEvent{}

	// --- Act & Assert ---
	err := tracker.dispatchErpChange(correctEvent, "test")
	require.NoError(t, err)

	err = tracker.dispatchErpChange(corruptedEvent, "test")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid event envelope")
}
