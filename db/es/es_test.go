package es

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/acronis/perfkit/db"
	"github.com/acronis/perfkit/logger"
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
	var logger = logger.NewPlaneLogger(logger.LevelDebug, true)

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

	var c = dbo.Context(context.Background(), false)

	s := dbo.Session(c)

	return dbo, s, c
}

func logDbTime(t *testing.T, c *db.Context) {
	t.Helper()

	t.Log("beginTime", time.Duration(c.BeginTime.Load()))
	t.Log("prepareTime", time.Duration(c.PrepareTime.Load()))
	t.Log("execTime", time.Duration(c.ExecTime.Load()))
	t.Log("queryTime", time.Duration(c.QueryTime.Load()))
	t.Log("deallocTime", time.Duration(c.DeallocTime.Load()))
	t.Log("commitTime", time.Duration(c.CommitTime.Load()))
}

func cleanup(t *testing.T, dbo db.Database) {
	t.Helper()

	if err := dbo.DropTable("perf_table"); err != nil {
		t.Error("drop table", err)
		return
	}
}
