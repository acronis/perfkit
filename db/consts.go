package db

import "strings"

type DialectName string

const (
	SQLITE     DialectName = "sqlite"     // SQLITE is the SQLite driver name
	SQLITE3    DialectName = "sqlite3"    // SQLITE3 is the SQLite driver name
	POSTGRES   DialectName = "postgres"   // POSTGRES is the PostgreSQL driver name
	MYSQL      DialectName = "mysql"      // MYSQL is the MySQL driver name
	MSSQL      DialectName = "mssql"      // MSSQL is the Microsoft SQL Server driver name
	CLICKHOUSE DialectName = "clickhouse" // CLICKHOUSE is the ClickHouse driver name
	CASSANDRA  DialectName = "cassandra"  // CASSANDRA is the Cassandra driver name
)

var (
	// SupportedDrivers is a string containing all supported drivers
	SupportedDrivers = strings.Join([]string{string(SQLITE), string(POSTGRES), string(MYSQL), string(MSSQL)}, "|")
)
