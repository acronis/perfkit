package sql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"

	"github.com/MichaelS11/go-cql-driver"
	"github.com/gocql/gocql"
	"github.com/google/uuid"

	"github.com/acronis/perfkit/db"
)

func init() {
	if err := db.Register("cql", &cassandraConnector{}); err != nil {
		panic(err)
	}
}

type cassandraDialect struct {
	keySpace string
}

func (d *cassandraDialect) name() db.DialectName {
	return db.CASSANDRA
}

func (d *cassandraDialect) encodeString(s string) string {
	// borrowed from dbr
	// http://www.postgresql.org/docs/9.2/static/sql-syntax-lexical.html
	return `'` + strings.Replace(s, `'`, `''`, -1) + `'`
}

func (d *cassandraDialect) encodeUUID(s uuid.UUID) string {
	return s.String()
}

func (d *cassandraDialect) encodeVector(vs []float32) string {
	return ""
}

func (d *cassandraDialect) encodeBool(b bool) string {
	// borrowed from dbr
	if b {
		return "TRUE"
	}
	return "FALSE"
}

func (d *cassandraDialect) encodeBytes(bs []byte) string {
	// borrowed from dbr, using string for json fields
	return d.encodeString(string(bs))
}

func (d *cassandraDialect) encodeTime(timestamp time.Time) string {
	return `'` + timestamp.UTC().Format(time.RFC3339Nano) + `'`
}

// GetType returns Cassandra-specific types
func (d *cassandraDialect) getType(dataType db.DataType) string {
	switch dataType {
	// Primary Keys and IDs
	case db.DataTypeId:
		return "int"
	case db.DataTypeTenantUUIDBoundID:
		return "varchar" // Composite key with tenant UUID

	// Integer Types
	case db.DataTypeInt:
		return "int" // Standard integer
	case db.DataTypeBigInt:
		return "bigint" // Large integer
	case db.DataTypeBigIntAutoIncPK:
		return "bigint primary key" // Auto-incrementing big integer primary key
	case db.DataTypeBigIntAutoInc:
		return "bigint" // Auto-incrementing big integer
	case db.DataTypeSmallInt:
		return "{$smallint}" // Small integer
	case db.DataTypeTinyInt:
		return "tinyint" // Tiny integer

	// String Types
	case db.DataTypeVarChar:
		return "varchar" // Variable-length string
	case db.DataTypeVarChar32:
		return "varchar" // VARCHAR(32)
	case db.DataTypeVarChar36:
		return "varchar" // VARCHAR(36)
	case db.DataTypeVarChar64:
		return "varchar" // VARCHAR(64)
	case db.DataTypeVarChar128:
		return "varchar" // VARCHAR(128)
	case db.DataTypeVarChar256:
		return "varchar" // VARCHAR(256)
	case db.DataTypeText:
		return "varchar" // Unlimited length text
	case db.DataTypeLongText:
		return "varchar" // Long text
	case db.DataTypeAscii:
		return "" // Charset specification is not needed in Cassandra

	// UUID Types
	case db.DataTypeUUID:
		return "uuid" // Cassandra supports UUID type
	case db.DataTypeVarCharUUID:
		return "varchar" // Cassandra supports UUID type

	// Binary Types
	case db.DataTypeLongBlob:
		return "blob" // Use blob for binary data
	case db.DataTypeHugeBlob:
		return "blob" // Use blob for binary data
	case db.DataTypeBinary20:
		return "blob" // varchar for fixed-length binary data
	case db.DataTypeBinaryBlobType:
		return "blob" // Use blob for binary data

	// Date and Time Types
	case db.DataTypeDateTime:
		return "timestamp" // DateTime type for date and time
	case db.DataTypeDateTime6:
		return "timestamp with time zone" // Timestamp with time zone
	case db.DataTypeTimestamp:
		return "timestamp" // Timestamp
	case db.DataTypeTimestamp6:
		return "timestamp with time zone" // Timestamp with time zone
	case db.DataTypeCurrentTimeStamp6:
		return "{$current_timestamp6}" // Current timestamp with microseconds

	// Boolean Types
	case db.DataTypeBoolean:
		return "boolean"
	case db.DataTypeBooleanFalse:
		return "false"
	case db.DataTypeBooleanTrue:
		return "true"

	// Constraints and Modifiers
	case db.DataTypeUnique:
		return "" // Unique values are not supported
	case db.DataTypeEngine:
		return ""
	case db.DataTypeNotNull:
		return ""
	case db.DataTypeNull:
		return ""
	default:
		return ""
	}
}

func (d *cassandraDialect) randFunc() string {
	return ""
}

func (d *cassandraDialect) supportTransactions() bool {
	return false
}

func (d *cassandraDialect) isRetriable(err error) bool {
	return false
}

func (d *cassandraDialect) canRollback(err error) bool {
	return true
}

func (d *cassandraDialect) table(table string) string {
	if d.keySpace != "" {
		return d.keySpace + "." + table
	}

	return table
}

func (d *cassandraDialect) schema() string {
	return d.keySpace
}

// Recommendations returns Cassandra recommendations for DB settings
func (d *cassandraDialect) recommendations() []db.Recommendation {
	return nil
}

func (d *cassandraDialect) close() error {
	return nil
}

type cassandraConnector struct{}

func (c *cassandraConnector) ConnectionPool(cfg db.Config) (db.Database, error) {
	var parsedURL, err = url.Parse(cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("db: cannot parse cassandra dsn: %s: err: %v", sanitizeConn(cfg.ConnString), err)
	}

	var user = parsedURL.User.Username()
	var password, _ = parsedURL.User.Password()

	parsedURL.User = nil

	var cs = parsedURL.String()
	if _, cs, err = db.ParseScheme(cs); err != nil {
		return nil, fmt.Errorf("db: cannot parse cassandra db path, err: %v", err)
	}

	var cassandraConfig *gocql.ClusterConfig
	if cassandraConfig, err = cql.ConfigStringToClusterConfig(cs); err != nil {
		return nil, fmt.Errorf("db: cannot convert cassandra dsn: %s: err: %v", sanitizeConn(cfg.ConnString), err)
	}

	var keySpace = cassandraConfig.Keyspace

	cassandraConfig.Timeout = time.Minute
	cassandraConfig.ConnectTimeout = time.Minute

	cassandraConfig.Authenticator = gocql.PasswordAuthenticator{
		Username: user,
		Password: password,
	}

	var dsn = cql.ClusterConfigToConfigString(cassandraConfig)

	dbo := &sqlDatabase{}
	var rwc *sql.DB

	if rwc, err = sql.Open("cql", dsn); err != nil {
		return nil, fmt.Errorf("db: cannot connect to cassandra db at %v, err: %v", sanitizeConn(cfg.ConnString), err)
	}

	if err = rwc.Ping(); err != nil {
		return nil, fmt.Errorf("db: failed ping cassandra db at %v, err: %v", sanitizeConn(cfg.ConnString), err)
	}

	dbo.rw = &cassandraQuerier{rwc}
	dbo.t = &cassandraQuerier{rwc}

	maxConn := int(math.Max(1, float64(cfg.MaxOpenConns)))
	rwc.SetMaxOpenConns(maxConn)
	rwc.SetMaxIdleConns(maxConn)

	dbo.dialect = &cassandraDialect{keySpace: keySpace}
	dbo.useTruncate = cfg.UseTruncate
	dbo.queryStringInterpolation = cfg.QueryStringInterpolation
	dbo.dryRun = cfg.DryRun
	dbo.queryLogger = cfg.QueryLogger
	dbo.readRowsLogger = cfg.ReadRowsLogger
	dbo.explainLogger = cfg.ExplainLogger

	return dbo, nil
}

func (c *cassandraConnector) DialectName(scheme string) (db.DialectName, error) {
	return db.CASSANDRA, nil
}

// cassandraQuerier implements the querier and accessor interfaces for Cassandra
// This structure is created to maintain compatibility with the sql package interfaces
// even though Cassandra doesn't support transactions. It wraps the standard sql.DB
// to provide query functionality while maintaining the expected interface structure.
type cassandraQuerier struct {
	be *sql.DB
}

// cassandraTransaction implements the transaction interface for Cassandra
// Since Cassandra doesn't support true transactions, this is a no-op implementation
// that allows the code to maintain compatibility with the sql package interfaces
// while actually executing queries directly against the database.
type cassandraTransaction struct {
	be *sql.DB
}

// ping verifies the database connection is still alive
func (d *cassandraQuerier) ping(ctx context.Context) error {
	return d.be.PingContext(ctx)
}

// stats returns database statistics
func (d *cassandraQuerier) stats() sql.DBStats {
	return d.be.Stats()
}

// rawSession returns the underlying database session
func (d *cassandraQuerier) rawSession() interface{} {
	return d.be
}

// close closes the database connection
func (d *cassandraQuerier) close() error {
	return d.be.Close()
}

// execContext executes a query without returning any rows
func (d *cassandraQuerier) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return d.be.ExecContext(ctx, query, args...)
}

// queryRowContext executes a query that returns a single row
func (d *cassandraQuerier) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return d.be.QueryRowContext(ctx, query, args...)
}

// queryContext executes a query that returns rows
func (d *cassandraQuerier) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return d.be.QueryContext(ctx, query, args...)
}

// prepareContext creates a prepared statement for later queries or executions
func (d *cassandraQuerier) prepareContext(ctx context.Context, query string) (sqlStatement, error) {
	var stmt, err = d.be.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &sqlStmt{stmt}, nil
}

// begin starts a new "transaction" (no-op for Cassandra)
func (d *cassandraQuerier) begin(ctx context.Context) (transaction, error) {
	return &cassandraTransaction{d.be}, nil
}

// Transaction interface implementation methods below
// Note: These methods execute directly since Cassandra doesn't support transactions

// execContext executes a query without returning any rows
func (t *cassandraTransaction) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.be.ExecContext(ctx, query, args...)
}

// queryRowContext executes a query that returns a single row
func (t *cassandraTransaction) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.be.QueryRowContext(ctx, query, args...)
}

// queryContext executes a query that returns rows
func (t *cassandraTransaction) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.be.QueryContext(ctx, query, args...)
}

// prepareContext creates a prepared statement for later queries or executions
func (t *cassandraTransaction) prepareContext(ctx context.Context, query string) (sqlStatement, error) {
	var stmt, err = t.be.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &sqlStmt{stmt}, nil
}

// commit is a no-op since Cassandra doesn't support transactions
func (t *cassandraTransaction) commit() error {
	return nil
}

// rollback is a no-op since Cassandra doesn't support transactions
func (t *cassandraTransaction) rollback() error {
	return nil
}
