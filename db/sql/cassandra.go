package sql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net/url"
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

// GetType returns Cassandra-specific types
func (d *cassandraDialect) getType(id string) string {
	switch id {
	case "{$bigint_autoinc_pk}":
		return "bigint PRIMARY KEY" // Cassandra does not support auto-increment, bigint is closest
	case "{$bigint_autoinc}":
		return "bigint" // Use bigint for large integers
	case "{$ascii}":
		return "" // Charset specification is not needed in Cassandra
	case "{$uuid}":
		return "UUID" // Cassandra supports UUID type
	case "{$varchar_uuid}":
		return "varchar" // Cassandra supports UUID type
	case "{$longblob}":
		return "blob" // Use blob for binary data
	case "{$hugeblob}":
		return "blob" // Use blob for binary data
	case "{$datetime}":
		return "timestamp" // DateTime type for date and time
	case "{$datetime6}":
		return "timestamp with time zone" // Timestamp with time zone
	case "{$timestamp6}":
		return "timestamp with time zone" // Timestamp with time zone
	case "{$current_timestamp6}":
		return "now()" // Function for current timestamp
	case "{$binary20}":
		return "blob" // varchar for fixed-length binary data
	case "{$binaryblobtype}":
		return "blob" // Use blob for binary data
	case "{$boolean}":
		return "boolean"
	case "{$boolean_false}":
		return "false"
	case "{$boolean_true}":
		return "true"
	case "{$tinyint}":
		return "tinyint"
	case "{$longtext}":
		return "text" // Use text for long text
	case "{$unique}":
		return "" // Unique values are not supported
	case "{$engine}":
		return ""
	case "{$notnull}":
		return ""
	case "{$null}":
		return ""
	case "{$tenant_uuid_bound_id}":
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

func (d *cassandraQuerier) Ping(ctx context.Context) error {
	return d.be.PingContext(ctx)
}
func (d *cassandraQuerier) Stats() sql.DBStats {
	return d.be.Stats()
}
func (d *cassandraQuerier) RawSession() interface{} {
	return d.be
}
func (d *cassandraQuerier) Close() error {
	return d.be.Close()
}
func (d *cassandraQuerier) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return d.be.ExecContext(ctx, query, args...)
}
func (d *cassandraQuerier) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return d.be.QueryRowContext(ctx, query, args...)
}
func (d *cassandraQuerier) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return d.be.QueryContext(ctx, query, args...)
}
func (d *cassandraQuerier) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return d.be.PrepareContext(ctx, query)
}
func (d *cassandraQuerier) Begin(ctx context.Context) (transaction, error) {
	return &cassandraTransaction{d.be}, nil
}

func (t *cassandraTransaction) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.be.ExecContext(ctx, query, args...)
}
func (t *cassandraTransaction) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.be.QueryRowContext(ctx, query, args...)
}
func (t *cassandraTransaction) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.be.QueryContext(ctx, query, args...)
}
func (t *cassandraTransaction) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return t.be.PrepareContext(ctx, query)
}
func (t *cassandraTransaction) Commit() error {
	return nil
}
func (t *cassandraTransaction) Rollback() error {
	return nil
}
