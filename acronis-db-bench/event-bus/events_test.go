package event_bus

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"
	"github.com/acronis/perfkit/logger"

	// List of required database drivers
	_ "github.com/acronis/perfkit/db/sql" // sql drivers
)

const (
	mariaDBConnString    = "mysql://user:password@tcp(localhost:3306)/perfkit_db_ci"                             // example value of a secret
	postgresqlConnString = "postgresql://root:password@localhost:5432/perfkit_pg_vector_db_ci?sslmode=disable"   // example value of a secret
	sqlServerConnString  = "sqlserver://perfkit_db_runner:MyP%40ssw0rd123@localhost:1433?database=perfkit_db_ci" // example value of a secret
)

func TestEventBusSuiteMariaDB(t *testing.T) {
	suite.Run(t, &EventBusTestSuite{ConnString: mariaDBConnString})
}

func TestEventBusSuitePostgreSQL(t *testing.T) {
	// suite.Run(t, &EventBusTestSuite{ConnString: postgresqlConnString})
}

func TestEventBusSuiteSQLServer(t *testing.T) {
	suite.Run(t, &EventBusTestSuite{ConnString: sqlServerConnString})
}

type mockLogger struct {
	logger.Logger
}

func (m *mockLogger) Debug(format string, args ...interface{})                       {}
func (m *mockLogger) Info(format string, args ...interface{})                        {}
func (m *mockLogger) Warn(format string, args ...interface{})                        {}
func (m *mockLogger) Error(format string, args ...interface{})                       {}
func (m *mockLogger) Trace(format string, args ...interface{})                       {}
func (m *mockLogger) Log(level logger.LogLevel, message string, args ...interface{}) {}
func (m *mockLogger) GetLevel() logger.LogLevel                                      { return logger.LevelDebug }
func (m *mockLogger) SetLevel(level logger.LogLevel)                                 {}
func (m *mockLogger) GetLastMessage() *logger.LogMessage                             { return nil }
func (m *mockLogger) Clone() logger.Logger                                           { return m }

type EventBusTestSuite struct {
	suite.Suite
	ConnString string
	eventBus   *EventBus
	conn       db.Database
	mockLog    *mockLogger
}

func (suite *EventBusTestSuite) SetupSuite() {
	var err error
	suite.conn, err = db.Open(db.Config{
		ConnString: suite.ConnString,
	})
	require.NoError(suite.T(), err)
	suite.mockLog = &mockLogger{}
}

func (suite *EventBusTestSuite) TearDownSuite() {
	if suite.conn != nil {
		suite.conn.Close()
	}
}

func (suite *EventBusTestSuite) SetupTest() {
	suite.eventBus = NewEventBus(suite.conn, suite.mockLog)
}

func (suite *EventBusTestSuite) TearDownTest() {
	if suite.eventBus != nil && suite.eventBus.workerStarted {
		suite.eventBus.Stop()
		// Give some time for the worker to stop
		time.Sleep(100 * time.Millisecond)
	}
}

func (suite *EventBusTestSuite) TestNewEventBus() {
	require.NoError(suite.T(), suite.eventBus.CreateTables())
	defer func() {
		_ = suite.eventBus.DropTables()
	}()

	assert.NotNil(suite.T(), suite.eventBus)
	assert.Equal(suite.T(), suite.conn, suite.eventBus.workerConn)
	assert.Equal(suite.T(), false, suite.eventBus.workerStarted)
	assert.Equal(suite.T(), 500, suite.eventBus.batchSize)
	assert.Equal(suite.T(), 10, suite.eventBus.sleepMsec)
}

func (suite *EventBusTestSuite) TestCreateTables() {
	require.NoError(suite.T(), suite.eventBus.CreateTables())
	defer func() {
		_ = suite.eventBus.DropTables()
	}()

	// Verify tables were created
	tables := []string{
		"acronis_db_bench_eventbus_events",
		"acronis_db_bench_eventbus_topics",
		"acronis_db_bench_eventbus_event_types",
		"acronis_db_bench_eventbus_sequences",
	}

	for _, table := range tables {
		exists, err := suite.conn.TableExists(table)
		require.NoError(suite.T(), err)
		assert.True(suite.T(), exists, "Table %s should exist", table)
	}

	// Verify initial data
	var count int
	ctx := suite.conn.Context(context.Background(), false)
	err := suite.conn.Session(ctx).QueryRow("SELECT COUNT(*) FROM acronis_db_bench_eventbus_topics").Scan(&count)
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), MaxTopics, count, "Should have created MaxTopics entries")
}

func (suite *EventBusTestSuite) TestDropTables() {
	require.NoError(suite.T(), suite.eventBus.CreateTables())
	require.NoError(suite.T(), suite.eventBus.DropTables())

	// Verify tables were dropped
	tables := []string{
		"acronis_db_bench_eventbus_events",
		"acronis_db_bench_eventbus_topics",
		"acronis_db_bench_eventbus_event_types",
		"acronis_db_bench_eventbus_sequences",
	}

	for _, table := range tables {
		exists, err := suite.conn.TableExists(table)
		require.NoError(suite.T(), err)
		assert.False(suite.T(), exists, "Table %s should not exist", table)
	}
}

func (suite *EventBusTestSuite) TestInsertEvent() {
	require.NoError(suite.T(), suite.eventBus.CreateTables())
	defer func() {
		_ = suite.eventBus.DropTables()
	}()

	rz := benchmark.NewRandomizer(time.Now().UnixNano(), 0)
	subjectUUID := "test-subject-uuid"

	ctx := suite.conn.Context(context.Background(), false)
	session := suite.conn.Session(ctx)
	err := suite.eventBus.InsertEvent(rz, session, subjectUUID)
	require.NoError(suite.T(), err)

	// Verify event was inserted
	var count int
	err = session.QueryRow("SELECT COUNT(*) FROM acronis_db_bench_eventbus_events").Scan(&count)
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), 1, count, "Should have inserted one event")
}

func (suite *EventBusTestSuite) TestStartStop() {
	require.NoError(suite.T(), suite.eventBus.CreateTables())
	defer func() {
		_ = suite.eventBus.DropTables()
	}()

	// Verify tables exist before proceeding
	exists, err := suite.conn.TableExists("acronis_db_bench_eventbus_events")
	require.NoError(suite.T(), err)
	require.True(suite.T(), exists, "Events table should exist before starting test")

	// Test Start
	err = suite.eventBus.Start()
	require.NoError(suite.T(), err)
	assert.True(suite.T(), suite.eventBus.workerStarted)

	// Give some time for the worker to start
	time.Sleep(100 * time.Millisecond)

	// Insert an event to ensure worker is processing
	rz := benchmark.NewRandomizer(time.Now().UnixNano(), 0)
	ctx := suite.conn.Context(context.Background(), false)
	session := suite.conn.Session(ctx)
	err = suite.eventBus.InsertEvent(rz, session, "test-subject")
	require.NoError(suite.T(), err)

	// Wait for event to be processed
	time.Sleep(200 * time.Millisecond)

	// Stop the worker before checking the event count
	suite.eventBus.Stop()
	time.Sleep(100 * time.Millisecond)

	// Verify event was processed
	var count int
	// Re-establish session context for this query to ensure freshness
	freshCtx := suite.conn.Context(context.Background(), false)
	freshSession := suite.conn.Session(freshCtx)
	err = freshSession.QueryRow("SELECT COUNT(*) FROM acronis_db_bench_eventbus_events").Scan(&count)
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), 0, count, "Event should have been processed")

	// Verify worker is stopped
	assert.False(suite.T(), suite.eventBus.workerStarted, "Worker should be marked as stopped")

	// Insert another event to verify worker is stopped
	err = suite.eventBus.InsertEvent(rz, session, "test-subject-2")
	require.NoError(suite.T(), err)

	// Verify event remains in events table since worker is stopped
	err = session.QueryRow("SELECT COUNT(*) FROM acronis_db_bench_eventbus_events").Scan(&count)
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), 1, count, "Event should remain in events table when worker is stopped")

	// Test double stop - should not panic or block
	suite.eventBus.Stop()
}

func (suite *EventBusTestSuite) TestQueueIsEmpty() {
	require.NoError(suite.T(), suite.eventBus.CreateTables())
	defer func() {
		_ = suite.eventBus.DropTables()
	}()

	// Initially queue should be empty
	empty, err := suite.eventBus.QueueIsEmpty()
	require.NoError(suite.T(), err)
	assert.True(suite.T(), empty)

	// Insert an event
	rz := benchmark.NewRandomizer(time.Now().UnixNano(), 0)
	ctx := suite.conn.Context(context.Background(), false)
	session := suite.conn.Session(ctx)
	err = suite.eventBus.InsertEvent(rz, session, "test-subject")
	require.NoError(suite.T(), err)

	// Queue should not be empty
	empty, err = suite.eventBus.QueueIsEmpty()
	require.NoError(suite.T(), err)
	assert.False(suite.T(), empty)
}
