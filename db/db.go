package db

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Connector is an interface for registering database connectors without knowing the specific connector implementations
type Connector interface {
	ConnectionPool(cfg Config) (Database, error)
	DialectName(scheme string) (DialectName, error)
}

var (
	dbRegistry   = make(map[string]Connector)
	registryLock = sync.Mutex{}
)

// Register registers a database connector
func Register(schema string, conn Connector) error {
	registryLock.Lock()
	defer registryLock.Unlock()

	if _, ok := dbRegistry[schema]; ok {
		return fmt.Errorf("schema %s already exists", schema)
	}

	dbRegistry[schema] = conn

	return nil
}

// Config - database configuration
type Config struct {
	ConnString      string
	MaxOpenConns    int
	MaxConnLifetime time.Duration
	MaxPacketSize   int
	DryRun          bool
	UseTruncate     bool

	QueryLogger      Logger
	ReadedRowsLogger Logger
	QueryTimeLogger  Logger
}

// Open opens a database connection
func Open(cfg Config) (Database, error) {
	var scheme, _, err = ParseScheme(cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s to scheme: %v", cfg.ConnString, err)
	}

	registryLock.Lock()
	var conn, ok = dbRegistry[scheme]
	registryLock.Unlock()

	if !ok {
		return nil, fmt.Errorf("scheme %s doesn't exist in registry", scheme)
	}

	return conn.ConnectionPool(cfg)
}

// GetDialectName - database dialect name
func GetDialectName(cs string) (DialectName, error) {
	var scheme, _, err = ParseScheme(cs)
	if err != nil {
		return "", fmt.Errorf("failed to parse %s to scheme: %v", cs, err)
	}

	registryLock.Lock()
	var conn, ok = dbRegistry[scheme]
	registryLock.Unlock()

	if !ok {
		return "", fmt.Errorf("scheme %s doesn't exist in registry", scheme)
	}

	return conn.DialectName(scheme)
}

// Logger is an interface for logging
type Logger interface {
	Log(format string, args ...interface{})
}

// databaseQueryRegistrator is an interface for registering database queries
type databaseQueryRegistrator interface {
	StatementEnter(query string, args ...interface{}) time.Time
	StatementExit(statement string, startTime time.Time, err error, showRowsAffected bool, result Result, format string, args []interface{}, rows Rows, dest []interface{})
}

// databaseSearcher is an interface for searching the database
type databaseSearcher interface {
	Search(from string, what string, where string, orderBy string, limit int, explain bool, args ...interface{}) (Rows, error)
	Aggregate(from string, what string, where string, groupBy string, orderBy string, limit int, explain bool, args ...interface{}) (Rows, error)
}

// databaseInserter is an interface for inserting data into the database
type databaseInserter interface {
	InsertInto(tableName string, data interface{}, columnNames []string) error
}

// databaseQuerier is an interface for low-level querying the database
type databaseQuerier interface {
	Exec(format string, args ...interface{}) (Result, error)
	QueryRow(format string, args ...interface{}) Row
	Query(format string, args ...interface{}) (Rows, error)
}

// Result is an interface for database query results
type Result interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
}

// Stmt is an interface for database statements
type Stmt interface {
	Exec(args ...any) (Result, error)
	Close() error
}

// databaseQueryPreparer is an interface for preparing database queries
type databaseQueryPreparer interface {
	Prepare(query string) (Stmt, error)
}

// DatabaseAccessor is an interface for accessing the database
type DatabaseAccessor interface {
	databaseQueryRegistrator
	databaseSearcher
	databaseInserter
	databaseQuerier
	databaseQueryPreparer
}

// Session is an interface for database sessions
type Session interface {
	DatabaseAccessor

	Transact(func(tx DatabaseAccessor) error) error

	// GetNextVal is presented in Session interface to restrict using it inside transaction
	GetNextVal(sequenceName string) (uint64, error)
}

type TableRow struct {
	Name    string
	Type    string
	NotNull bool
}

type TableDefinition struct {
	TableRows  []TableRow
	PrimaryKey []string
	Engine     string
}

type IndexType string

const (
	IndexTypeBtree IndexType = "btree"
)

// Constraint represents a database constraint
type Constraint struct {
	Name       string `json:"name"`
	TableName  string `json:"table_name"`
	Definition string `json:"definition"`
}

// databaseMigrator is an interface for migrating the database
type databaseMigrator interface {
	ApplyMigrations(tableName, tableMigrationDDL string) error

	TableExists(tableName string) (bool, error)
	CreateTable(tableName string, tableDefinition *TableDefinition, tableMigrationDDL string) error
	DropTable(tableName string) error

	IndexExists(indexName string, tableName string) (bool, error)
	CreateIndex(indexName string, tableName string, columns []string, indexType IndexType) error
	DropIndex(indexName string, tableName string) error

	ReadConstraints() ([]Constraint, error)
	AddConstraints(constraints []Constraint) error
	DropConstraints(constraints []Constraint) error

	CreateSequence(sequenceName string) error
	DropSequence(sequenceName string) error
}

// databaseDescriber is an interface for describing the database
type databaseDescriber interface {
	GetVersion() (DialectName, string, error)
	GetInfo(version string) (ret []string, info *Info, err error)
	GetTablesSchemaInfo(tableNames []string) ([]string, error)
	GetTablesVolumeInfo(tableNames []string) ([]string, error)
}

// Stats is a struct for storing database statistics
type Stats struct {
	OpenConnections int // The number of established connections both in use and idle.
	InUse           int // The number of connections currently in use.
	Idle            int // The number of idle connections.
}

// Context is a struct for storing database context
type Context struct {
	Ctx        context.Context
	BeginTime  time.Duration
	DBtime     time.Duration
	CommitTime time.Duration
	TxRetries  int
}

// Database is an interface for database operations
type Database interface {
	Ping(ctx context.Context) error

	DialectName() DialectName
	UseTruncate() bool

	databaseMigrator
	databaseDescriber

	Context(ctx context.Context) *Context
	Session(ctx *Context) Session
	RawSession() interface{}

	Stats() *Stats

	Close() error
}

// Dialect is an interface for database dialects
type Dialect interface {
	GetType(id string) string
}

// Recommendation is a struct for storing DB recommendation
type Recommendation struct {
	Setting string
	Meaning string

	ExpectedValue  string
	MinVal         int64
	RecommendedVal int64
}

// DBType - database type
type DBType struct {
	Driver DialectName // driver name (used in the code)
	Symbol string      // short symbol for the driver (used in the command line)
	Name   string      // full name of the driver (used in the command line)
}

// GetDatabases returns a list of supported databases
func GetDatabases() []DBType {
	var ret []DBType
	ret = append(ret, DBType{Driver: POSTGRES, Symbol: "P", Name: "PostgreSQL"})
	ret = append(ret, DBType{Driver: MYSQL, Symbol: "M", Name: "MySQL/MariaDB"})
	ret = append(ret, DBType{Driver: MSSQL, Symbol: "W", Name: "MSSQL"})
	ret = append(ret, DBType{Driver: SQLITE, Symbol: "S", Name: "SQLite"})
	ret = append(ret, DBType{Driver: CLICKHOUSE, Symbol: "C", Name: "ClickHouse"})
	// "A" is used as the latest symbol of the "Cassandra" due to duplicate with ClickHouse "C"
	ret = append(ret, DBType{Driver: CASSANDRA, Symbol: "A", Name: "Cassandra"})

	return ret
}