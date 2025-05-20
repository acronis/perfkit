package es

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/acronis/perfkit/db"
)

const (
	esConnString         = "es://localhost:9200"
	openSearchConnString = "opensearch://admin:bgnYFGR2RhN3SCX@localhost:9201" // example value of a secret compliant with OpenSearch password requirements
)

type TestingSuite struct {
	suite.Suite
	ConnString string
}

func TestDatabaseSuiteElasticSearch(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: esConnString})
}

func TestDatabaseSuiteOpenSearch(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: openSearchConnString})
}

type testLogger struct {
	t *testing.T
}

func newTestLogger(t *testing.T) db.Logger {
	return &testLogger{t: t}
}

func (l *testLogger) Log(format string, args ...interface{}) {
	l.t.Logf(format, args...)
}

func testTableDefinition() *db.TableDefinition {
	return &db.TableDefinition{
		TableRows: []db.TableRow{
			db.TableRowItem{Name: "@timestamp", Type: db.DataTypeDateTime, Indexed: true},
			db.TableRowItem{Name: "id", Type: db.DataTypeId, Indexed: true},
			db.TableRowItem{Name: "uuid", Type: db.DataTypeUUID, Indexed: true},
			db.TableRowItem{Name: "type", Type: db.DataTypeVarChar, Indexed: true},
			db.TableRowItem{Name: "policy_name", Type: db.DataTypeVarChar, Indexed: true},
			db.TableRowItem{Name: "resource_name", Type: db.DataTypeVarChar, Indexed: true},
			db.TableRowItem{Name: "accessors", Type: db.DataTypeVarChar, Indexed: true},
			db.TableRowItem{Name: "start_time", Type: db.DataTypeDateTime, Indexed: true},
		},
	}
}

func (suite *TestingSuite) makeTestSession() (db.Database, db.Session, *db.Context) {
	var logger = newTestLogger(suite.T())

	dbo, err := db.Open(db.Config{
		ConnString:        suite.ConnString,
		MaxOpenConns:      16,
		MaxConnLifetime:   1000 * time.Millisecond,
		QueryLogger:       logger,
		ReadRowsLogger:    logger,
		LogOperationsTime: true,
	})

	require.NoError(suite.T(), err, "making test esSession")

	var tableSpec = testTableDefinition()

	time.Sleep(1 * time.Second)

	if err = dbo.CreateTable("perf_table", tableSpec, ""); err != nil {
		require.NoError(suite.T(), err, "init scheme")
	}

	var c = dbo.Context(context.Background(), false)

	s := dbo.Session(c)

	return dbo, s, c
}

func logDbTime(t *testing.T, c *db.Context) {
	t.Helper()
	db.DumpExecutionTime(newTestLogger(t), c)
}

func cleanup(t *testing.T, dbo db.Database) {
	t.Helper()

	exists, err := dbo.TableExists("perf_table")
	if err != nil {
		t.Error("check table exists", err)
		return
	}

	if !exists {
		return
	}

	if err := dbo.DropTable("perf_table"); err != nil {
		t.Error("drop table", err)
		return
	}
}

func dbDialect(connString string) (dialect, error) {
	scheme, _, err := db.ParseScheme(connString)
	if err != nil {
		return nil, fmt.Errorf("cannot parse connection string scheme '%v', error: %v", connString, err)
	}

	switch scheme {
	case "es", "elastic", "elasticsearch":
		return &elasticSearchDialect{}, nil
	case "os", "opensearch":
		return &openSearchDialect{}, nil
	default:
		return nil, fmt.Errorf("db: unsupported backend '%v'", scheme)
	}
}
