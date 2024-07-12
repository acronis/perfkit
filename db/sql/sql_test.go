package sql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/acronis/perfkit/db"
)

const (
	sqliteConnString = "sqlite://:memory:"

	mariaDBConnString    = "mysql://user:password@tcp(localhost:3306)/perfkit_db_ci"
	sqlServerConnString  = "sqlserver://perfkit_db_runner:qwe123%21%40%23@localhost:1433?database=perfkit_db_ci"
	postgresqlConnString = "postgresql://root:root@localhost:5432/perfkit_db_ci?sslmode=disable"
	clickHouseConnString = "clickhouse://username:password@localhost:9000/perfkit_db_ci"
	cassandraConnString  = "cql://admin:admin@localhost:9042?keyspace=perfkit_db_ci"
)

type TestingSuite struct {
	suite.Suite
	ConnString string
}

func TestDatabaseSuiteSQLite(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: sqliteConnString})
}

func TestDatabaseSuiteMySQL(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: mariaDBConnString})
}

func TestDatabaseSuiteSQLServer(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: sqlServerConnString})
}

func TestDatabaseSuitePG(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: postgresqlConnString})
}

func TestDatabaseSuiteClickHouse(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: clickHouseConnString})
}

func TestDatabaseSuiteCassandra(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: cassandraConnString})
}

type testLogger struct {
	t *testing.T
}

func (l *testLogger) Log(format string, args ...interface{}) {
	l.t.Logf(format, args...)
}

func testTableDefinition(dia db.DialectName) *db.TableDefinition {
	var tableSpec *db.TableDefinition

	if dia == db.CASSANDRA {
		tableSpec = &db.TableDefinition{
			TableRows: []db.TableRow{
				{Name: "origin", Type: "INT"},
				{Name: "type", Type: "INT"},
				{Name: "name", Type: "TEXT"},
			},
			PrimaryKey: []string{"origin", "type", "name"},
		}
	} else {
		tableSpec = &db.TableDefinition{
			TableRows: []db.TableRow{
				{Name: "origin", Type: "INT", NotNull: true},
				{Name: "type", Type: "INTEGER", NotNull: true},
				{Name: "name", Type: "VARCHAR(256)", NotNull: false},
			},
			PrimaryKey: []string{"origin", "type", "name"},
		}
	}

	if dia == db.CLICKHOUSE {
		tableSpec.Engine = "MergeTree() ORDER BY (origin, type, name)"
	}

	return tableSpec
}

func (suite *TestingSuite) makeTestSession() (db.Database, db.Session, *db.Context) {
	var logger = &testLogger{t: suite.T()}

	dbo, err := db.Open(db.Config{
		ConnString:      suite.ConnString,
		MaxOpenConns:    16,
		MaxConnLifetime: 100 * time.Millisecond,
		QueryLogger:     logger,
	})

	require.NoError(suite.T(), err, "making test session")

	var tableSpec = testTableDefinition(dbo.DialectName())

	if err = dbo.CreateTable("perf_table", tableSpec, ""); err != nil {
		require.NoError(suite.T(), err, "init scheme")
	}

	if err = dbo.CreateIndex("perf_index", "perf_table", []string{"type", "name"}, db.IndexTypeBtree); err != nil {
		require.NoError(suite.T(), err, "create_index")
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

func TestSanitizeConn(t *testing.T) {
	require.Equal(t, "", sanitizeConn(""))
	require.Equal(t, "mysql://tcp:3306/perfkit_ci", sanitizeConn("mysql://root:root@tcp:3306/perfkit_ci"))
	require.Equal(t, "root:root@tcp:3306/perfkit_ci", sanitizeConn("root:root@tcp:3306/perfkit_ci"))
	require.Equal(t, "postgresql://postgres:5432/perfkit_ci?sslmode=disable", sanitizeConn("postgresql://root:root@postgres:5432/perfkit_ci?sslmode=disable"))
	require.Equal(t, "sqlite://:memory:", sanitizeConn("sqlite://:memory:"))
	require.Equal(t, "some_random@string", sanitizeConn("some_random@string"))
}
