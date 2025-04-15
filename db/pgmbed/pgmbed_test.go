package pgmbed

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/lib/pq"

	"github.com/acronis/perfkit/logger"
)

type LogLevel = logger.LogLevel
type LogMessage = logger.LogMessage

const (
	LevelError = logger.LevelError
	LevelWarn  = logger.LevelWarn
	LevelInfo  = logger.LevelInfo
	LevelDebug = logger.LevelDebug
	LevelTrace = logger.LevelTrace
)

type testLogger struct {
	level       LogLevel
	lastMessage *LogMessage
}

func (l *testLogger) Log(level LogLevel, message string, args ...interface{}) {
	l.lastMessage = &LogMessage{Level: level, Message: fmt.Sprintf(message, args...)}
	fmt.Printf(message, args...)
}

func (l *testLogger) Error(format string, args ...interface{}) {
	l.Log(LevelError, format, args...)
}

func (l *testLogger) Warn(format string, args ...interface{}) {
	l.Log(LevelWarn, format, args...)
}

func (l *testLogger) Info(format string, args ...interface{}) {
	l.Log(LevelInfo, format, args...)
}

func (l *testLogger) Debug(format string, args ...interface{}) {
	l.Log(LevelDebug, format, args...)
}

func (l *testLogger) Trace(format string, args ...interface{}) {
	l.Log(LevelTrace, format, args...)
}

func (l *testLogger) GetLevel() LogLevel {
	return l.level
}

func (l *testLogger) SetLevel(level LogLevel) {
	l.level = level
}

func (l *testLogger) GetLastMessage() *LogMessage {
	return l.lastMessage
}

func (l *testLogger) Clone() logger.Logger {
	return &testLogger{level: l.level}
}

func TestLaunch(t *testing.T) {
	// Initialize the embedded Postgres (first connection)
	cs := "postgres://localhost:5433/postgres?sslmode=disable&embedded-postgres=true&ep-port=5433&ep-max-connections=512"
	cleanedConnString, opts, err := ParseOptions(cs)
	if err != nil {
		t.Errorf("Error parsing: %v\n", err)
		return
	}
	t.Logf("connection string after cleaning: %s", cleanedConnString)

	// Simulate launching the embedded Postgres
	cs, err = Launch(cleanedConnString, opts, &testLogger{})
	if err != nil {
		t.Errorf("failed initializing: %v\n", err)
		return
	}

	t.Logf("connection string for launched embedded postgres: %s", cs)

	// Simulate launching the embedded Postgres again
	cs, err = Launch(cleanedConnString, opts, nil)
	if err != nil {
		t.Errorf("failed initializing: %v\n", err)
		return
	}

	t.Logf("connection string for second attempt of launching embedded postgres: %s", cs)

	// Test database connection by executing SELECT 1
	db, err := sql.Open("postgres", cs)
	if err != nil {
		t.Errorf("failed to open database connection: %v", err)
		return
	}
	defer db.Close()

	var result int
	err = db.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		t.Errorf("failed to execute query: %v", err)
		return
	}

	if result != 1 {
		t.Errorf("unexpected result: got %d, want 1", result)
		return
	}

	// Simulate closing connections
	if err = Terminate(); err != nil {
		t.Errorf("failed terminating: %v\n", err)
		return
	}

	if err = Terminate(); err != nil {
		t.Errorf("failed terminating: %v\n", err)
		return
	}
}
