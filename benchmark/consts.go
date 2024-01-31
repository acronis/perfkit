package benchmark

import "strings"

const (
	LogError  = 0    // LogError is the log level for errors
	LogWarn   = 1    // LogWarn is the log level for warnings
	LogInfo   = 2    // LogInfo is the log level for informational messages
	LogDebug  = 3    // LogDebug is the log level for debug messages
	LogTrace  = 4    // LogTrace is the log level for trace messages
	PollLimit = 4096 // PollLimit is the maximum number of events to poll for at a time
)

const (
	SQLITE     = "sqlite"     // SQLITE is the SQLite driver name
	SQLITE3    = "sqlite3"    // SQLITE3 is the SQLite driver name
	POSTGRES   = "postgres"   // POSTGRES is the PostgreSQL driver name
	MYSQL      = "mysql"      // MYSQL is the MySQL driver name
	MSSQL      = "mssql"      // MSSQL is the Microsoft SQL Server driver name
	CLICKHOUSE = "clickhouse" // CLICKHOUSE is the ClickHouse driver name
	CASSANDRA  = "cassandra"  // CASSANDRA is the Cassandra driver name

	SequenceName = "acronis_db_bench_sequence" // SequenceName is the name of the sequence used for generating IDs
)

var (
	// SupportedDrivers is a string containing all supported drivers
	SupportedDrivers = strings.Join([]string{SQLITE, POSTGRES, MYSQL, MSSQL}, "|")
	// CassandraKeySpace is the name of the DB keyspace used for Cassandra
	CassandraKeySpace = "acronis_db_bench"
)
