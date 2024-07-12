package pgmbed

import (
	"fmt"
	"testing"
)

type testLogger struct{}

func (l *testLogger) Log(format string, args ...interface{}) {
	fmt.Printf(format, args...)
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
