package es

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/acronis/perfkit/db"
)

const (
	esConnString         = "es://0.0.0.0:9200"
	openSearchConnString = "opensearch://admin:%22ScoRpi0n$%22@0.0.0.0:9200" // example value of a secret compliant with OpenSearch password requirements
)

type TestingSuite struct {
	suite.Suite
	ConnString string
}

func TestDatabaseSuiteElasticSearch(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: esConnString})
}

/*
func TestDatabaseSuiteOpenSearch(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: openSearchConnString})
}

*/

type testLogger struct {
	t *testing.T
}

func (l *testLogger) Log(format string, args ...interface{}) {
	l.t.Logf(format, args...)
}

func testTableDefinition() *db.TableDefinition {
	return &db.TableDefinition{
		TableRows: []db.TableRow{
			{Name: "@timestamp", Type: db.DataTypeDateTime, Indexed: true},
			{Name: "id", Type: db.DataTypeId, Indexed: true},
			{Name: "uuid", Type: db.DataTypeUUID, Indexed: true},
			{Name: "type", Type: db.DataTypeVarChar, Indexed: true},
			{Name: "policy_name", Type: db.DataTypeVarChar, Indexed: true},
			{Name: "resource_name", Type: db.DataTypeVarChar, Indexed: true},
			{Name: "accessors", Type: db.DataTypeVarChar, Indexed: true},
			{Name: "start_time", Type: db.DataTypeDateTime, Indexed: true},
		},
	}
}

func (suite *TestingSuite) makeTestSession() (db.Database, db.Session, *db.Context) {
	var logger = &testLogger{t: suite.T()}

	dbo, err := db.Open(db.Config{
		ConnString:      suite.ConnString,
		MaxOpenConns:    16,
		MaxConnLifetime: 100 * time.Millisecond,
		QueryLogger:     logger,
	})

	require.NoError(suite.T(), err, "making test esSession")

	var tableSpec = testTableDefinition()

	if err = dbo.CreateTable("perf_table", tableSpec, ""); err != nil {
		require.NoError(suite.T(), err, "init scheme")
	}

	var c = dbo.Context(context.Background())

	s := dbo.Session(c)

	return dbo, s, c
}

func logDbTime(t *testing.T, c *db.Context) {
	t.Helper()

	t.Log("dbtime", c.DBtime)
}

func cleanup(t *testing.T, dbo db.Database) {
	t.Helper()

	if err := dbo.DropTable("perf_table"); err != nil {
		t.Error("drop table", err)
		return
	}
}
