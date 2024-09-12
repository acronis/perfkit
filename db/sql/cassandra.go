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

// GetType returns Cassandra-specific types
func (d *cassandraDialect) getType(dataType db.DataType) string {
	switch dataType {
	case db.DataTypeInt:
		return "INT"
	case db.DataTypeString:
		return "VARCHAR"
	case db.DataTypeString256:
		return "VARCHAR(256)"
	case db.DataTypeBigIntAutoIncPK:
		return "bigint PRIMARY KEY" // Cassandra does not support auto-increment, bigint is closest
	case db.DataTypeBigIntAutoInc:
		return "bigint" // Use bigint for large integers
	case db.DataTypeAscii:
		return "" // Charset specification is not needed in Cassandra
	case db.DataTypeUUID:
		return "UUID" // Cassandra supports UUID type
	case db.DataTypeVarCharUUID:
		return "varchar" // Cassandra supports UUID type
	case db.DataTypeLongBlob:
		return "blob" // Use blob for binary data
	case db.DataTypeHugeBlob:
		return "blob" // Use blob for binary data
	case db.DataTypeDateTime:
		return "timestamp" // DateTime type for date and time
	case db.DataTypeDateTime6:
		return "timestamp with time zone" // Timestamp with time zone
	case db.DataTypeTimestamp6:
		return "timestamp with time zone" // Timestamp with time zone
	case db.DataTypeCurrentTimeStamp6:
		return "now()" // Function for current timestamp
	case db.DataTypeBinary20:
		return "blob" // varchar for fixed-length binary data
	case db.DataTypeBinaryBlobType:
		return "blob" // Use blob for binary data
	case db.DataTypeBoolean:
		return "boolean"
	case db.DataTypeBooleanFalse:
		return "false"
	case db.DataTypeBooleanTrue:
		return "true"
	case db.DataTypeTinyInt:
		return "tinyint"
	case db.DataTypeLongText:
		return "text" // Use text for long text
	case db.DataTypeUnique:
		return "" // Unique values are not supported
	case db.DataTypeEngine:
		return ""
	case db.DataTypeNotNull:
		return ""
	case db.DataTypeNull:
		return ""
	case db.DataTypeTenantUUIDBoundID:
		return "varchar"
	default:
		return ""
	}
}

func (d *cassandraDialect) randFunc() string {
	return ""
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
	dbo.queryLogger = cfg.QueryLogger

	return dbo, nil
}

func (c *cassandraConnector) DialectName(scheme string) (db.DialectName, error) {
	return db.CASSANDRA, nil
}

type cassandraQuerier struct {
	be *sql.DB
}
type cassandraTransaction struct {
	be *sql.DB
}

func (d *cassandraQuerier) ping(ctx context.Context) error {
	return d.be.PingContext(ctx)
}
func (d *cassandraQuerier) stats() sql.DBStats {
	return d.be.Stats()
}
func (d *cassandraQuerier) rawSession() interface{} {
	return d.be
}
func (d *cassandraQuerier) close() error {
	return d.be.Close()
}
func (d *cassandraQuerier) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return d.be.ExecContext(ctx, query, args...)
}
func (d *cassandraQuerier) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return d.be.QueryRowContext(ctx, query, args...)
}
func (d *cassandraQuerier) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return d.be.QueryContext(ctx, query, args...)
}
func (d *cassandraQuerier) prepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return d.be.PrepareContext(ctx, query)
}
func (d *cassandraQuerier) begin(ctx context.Context) (transaction, error) {
	return &cassandraTransaction{d.be}, nil
}

func (t *cassandraTransaction) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.be.ExecContext(ctx, query, args...)
}
func (t *cassandraTransaction) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.be.QueryRowContext(ctx, query, args...)
}
func (t *cassandraTransaction) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.be.QueryContext(ctx, query, args...)
}
func (t *cassandraTransaction) prepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return t.be.PrepareContext(ctx, query)
}
func (t *cassandraTransaction) commit() error {
	return nil
}
func (t *cassandraTransaction) rollback() error {
	return nil
}
