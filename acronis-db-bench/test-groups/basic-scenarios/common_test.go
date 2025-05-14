package basic_scenarios

import (
	"testing"

	"github.com/stretchr/testify/suite"

	// List of required database drivers
	_ "github.com/acronis/perfkit/db/sql" // sql drivers

	// List of required test groups
	_ "github.com/acronis/perfkit/acronis-db-bench/test-groups/tenant-and-cti" // tenant-and-cti
)

const (
	sqliteConnString        = "sqlite://:memory:"
	mariaDBConnString       = "mysql://user:password@tcp(localhost:3306)/perfkit_db_ci"                             // example value of a secret
	postgresqlConnString    = "postgresql://root:password@localhost:5432/perfkit_pg_vector_db_ci?sslmode=disable"   // example value of a secret
	dbrMariaDBConnString    = "mysql+dbr://user:password@tcp(localhost:3306)/perfkit_db_ci"                         // example value of a secret
	dbrPGVectorDBConnString = "postgres+dbr://root:password@localhost:5432/perfkit_pg_vector_db_ci?sslmode=disable" // example value of a secret
)

type TestingSuite struct {
	suite.Suite
	ConnString string
}

/*
func TestDatabaseSuiteSQLite(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: sqliteConnString})
}
*/

func TestDatabaseSuiteMariaDB(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: mariaDBConnString})
}

func TestDatabaseSuitePostgreSQL(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: postgresqlConnString})
}

func TestDatabaseSuiteDBRMariaDB(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: dbrMariaDBConnString})
}

func TestDatabaseSuiteDBRPostgreSQL(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: dbrPGVectorDBConnString})
}
