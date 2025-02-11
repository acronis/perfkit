package sql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/MichaelS11/go-cql-driver"
	"github.com/gocql/gocql"

	"github.com/acronis/perfkit/db"
)

const (
	sqliteConnString = "sqlite://:memory:"

	mariaDBConnString    = "mysql://user:password@tcp(localhost:3306)/perfkit_db_ci"                             // example value of a secret
	sqlServerConnString  = "sqlserver://perfkit_db_runner:MyP%40ssw0rd123@localhost:1433?database=perfkit_db_ci" // example value of a secret
	postgresqlConnString = "postgresql://root:password@localhost:5432/perfkit_db_ci?sslmode=disable"             // example value of a secret
	pgVectorConnString   = "postgresql://root:password@localhost:5432/perfkit_pg_vector_db_ci?sslmode=disable"   // example value of a secret
	clickHouseConnString = "clickhouse://username:password@localhost:9000/perfkit_db_ci"                         // example value of a secret
	cassandraConnString  = "cql://admin:password@localhost:9042?keyspace=perfkit_db_ci"                          // example value of a secret
)

type TestingSuite struct {
	suite.Suite
	ConnString string
}

/*
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
*/

func TestDatabaseSuitePGVector(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: pgVectorConnString})
}

/*
func TestDatabaseSuiteClickHouse(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: clickHouseConnString})
}

func TestDatabaseSuiteCassandra(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: cassandraConnString})
}

*/

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
				{Name: "origin", Type: db.DataTypeInt},
				{Name: "type", Type: db.DataTypeInt},
				{Name: "name", Type: db.DataTypeLongText},
			},
			PrimaryKey: []string{"origin", "type", "name"},
		}
	} else {
		tableSpec = &db.TableDefinition{
			TableRows: []db.TableRow{
				{Name: "origin", Type: db.DataTypeInt, NotNull: true},
				{Name: "type", Type: db.DataTypeInt, NotNull: true},
				{Name: "name", Type: db.DataTypeVarChar256, NotNull: false},
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
		ReadRowsLogger:  logger,
	})

	require.NoError(suite.T(), err, "making test sqlSession")

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

func dbDialect(connString string) (dialect, error) {
	scheme, _, err := db.ParseScheme(connString)
	if err != nil {
		return nil, fmt.Errorf("cannot parse connection string scheme '%v', error: %v", connString, err)
	}

	switch scheme {
	case "sqlite":
		return &sqliteDialect{}, nil
	case "mysql":
		return &mysqlDialect{}, nil
	case "pg", "postgres", "postgresql":
		if _, schemaName, err := postgresSchemaAndConnString(connString); err != nil {
			return nil, fmt.Errorf("db: postgres: %v", err)
		} else {
			return &pgDialect{schemaName: schemaName}, nil
		}
	case "mssql", "sqlserver":
		return &msDialect{}, nil
	case "cql":
		var cassandraConfig *gocql.ClusterConfig
		if cassandraConfig, err = cql.ConfigStringToClusterConfig(connString); err != nil {
			return nil, fmt.Errorf("db: cannot convert cassandra dsn: %s: err: %v", sanitizeConn(connString), err)
		}

		return &cassandraDialect{keySpace: cassandraConfig.Keyspace}, nil
	case "clickhouse":
		return &clickHouseDialect{}, nil
	default:
		return nil, fmt.Errorf("db: unsupported backend '%v'", scheme)
	}
}

func TestSanitizeConn(t *testing.T) {
	require.Equal(t, "", sanitizeConn(""))
	require.Equal(t, "mysql://tcp:3306/perfkit_ci", sanitizeConn("mysql://root:password@tcp:3306/perfkit_ci"))                                                     // example value of a secret
	require.Equal(t, "root:password@tcp:3306/perfkit_ci", sanitizeConn("root:password@tcp:3306/perfkit_ci"))                                                       // example value of a secret
	require.Equal(t, "postgresql://postgres:5432/perfkit_ci?sslmode=disable", sanitizeConn("postgresql://root:password@postgres:5432/perfkit_ci?sslmode=disable")) // example value of a secret
	require.Equal(t, "sqlite://:memory:", sanitizeConn("sqlite://:memory:"))
	require.Equal(t, "some_random@string", sanitizeConn("some_random@string"))
}
