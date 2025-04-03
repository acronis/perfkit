package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/lib/pq"

	"github.com/acronis/perfkit/db"
	"github.com/acronis/perfkit/db/pgmbed"
	"github.com/acronis/perfkit/logger"
)

func init() {
	for _, pgNameStyle := range []string{"postgres", "postgresql"} {
		if err := db.Register(pgNameStyle, &pgConnector{}); err != nil {
			panic(err)
		}
	}
}

type pgDialect struct {
	schemaName string
	embedded   bool
}

func (d *pgDialect) name() db.DialectName {
	return db.POSTGRES
}

func (d *pgDialect) encodeString(s string) string {
	// borrowed from dbr
	// http://www.postgresql.org/docs/9.2/static/sql-syntax-lexical.html
	return `'` + strings.Replace(s, `'`, `''`, -1) + `'`
}

func (d *pgDialect) encodeUUID(s uuid.UUID) string {
	return d.encodeString(s.String())
}

func (d *pgDialect) encodeVector(vs []float32) string {
	var sb strings.Builder
	for _, f := range vs {
		if sb.Len() != 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.FormatFloat(float64(f), 'f', -1, 64))
	}

	return fmt.Sprintf("'[%s]'", sb.String())
}

func (d *pgDialect) encodeOrderByVector(field, operator, vector string) string {
	return fmt.Sprintf("%s <-> '%s'", field, vector)
}

func (d *pgDialect) encodeBool(b bool) string {
	// borrowed from dbr
	if b {
		return "TRUE"
	}
	return "FALSE"
}

func (d *pgDialect) encodeBytes(bs []byte) string {
	// borrowed from dbr, using string for json fields
	return d.encodeString(string(bs))
}

func (d *pgDialect) encodeTime(timestamp time.Time) string {
	return `'` + timestamp.UTC().Format(time.RFC3339Nano) + `'`
}

// GetType returns PostgreSQL-specific types
func (d *pgDialect) getType(id db.DataType) string {
	switch id {
	// Primary Keys and IDs
	case db.DataTypeId:
		return "BIGSERIAL PRIMARY KEY"
	case db.DataTypeTenantUUIDBoundID:
		return "VARCHAR(64)"

	// Integer Types
	case db.DataTypeInt:
		return "INT"
	case db.DataTypeBigInt:
		return "BIGINT"
	case db.DataTypeBigIntAutoIncPK:
		return "BIGSERIAL PRIMARY KEY"
	case db.DataTypeBigIntAutoInc:
		return "BIGSERIAL"
	case db.DataTypeSmallInt:
		return "SMALLINT"
	case db.DataTypeTinyInt:
		return "SMALLINT"

	// String Types
	case db.DataTypeVarChar:
		return "VARCHAR"
	case db.DataTypeVarChar32:
		return "VARCHAR(32)"
	case db.DataTypeVarChar36:
		return "VARCHAR(36)"
	case db.DataTypeVarChar64:
		return "VARCHAR(64)"
	case db.DataTypeVarChar128:
		return "VARCHAR(128)"
	case db.DataTypeVarChar256:
		return "VARCHAR(256)"
	case db.DataTypeText:
		return "VARCHAR"
	case db.DataTypeLongText:
		return "TEXT"
	case db.DataTypeAscii:
		return ""

	// UUID Types
	case db.DataTypeUUID:
		return "UUID"
	case db.DataTypeVarCharUUID:
		return "VARCHAR(36)"

	// Binary Types
	case db.DataTypeLongBlob:
		return "BYTEA"
	case db.DataTypeHugeBlob:
		return "BYTEA"
	case db.DataTypeBinary20:
		return "BYTEA"
	case db.DataTypeBinaryBlobType:
		return "BYTEA"

	// Date and Time Types
	case db.DataTypeDateTime:
		return "TIMESTAMP"
	case db.DataTypeDateTime6:
		return "TIMESTAMP(6)"
	case db.DataTypeTimestamp:
		return "TIMESTAMP"
	case db.DataTypeTimestamp6:
		return "TIMESTAMP(6)"
	case db.DataTypeCurrentTimeStamp6:
		return "CURRENT_TIMESTAMP(6)"

	// Boolean Types
	case db.DataTypeBoolean:
		return "BOOLEAN"
	case db.DataTypeBooleanFalse:
		return "false"
	case db.DataTypeBooleanTrue:
		return "true"

	// Special Types
	case db.DataTypeJSON:
		return "JSONB"
	case db.DataTypeVector3Float32:
		return "vector(3)"
	case db.DataTypeVector768Float32:
		return "vector(768)"

	// Constraints and Modifiers
	case db.DataTypeUnique:
		return "unique"
	case db.DataTypeEngine:
		return ""
	case db.DataTypeNotNull:
		return "not null"
	case db.DataTypeNull:
		return "null"

	default:
		return ""
	}
}

func (d *pgDialect) randFunc() string {
	return "RANDOM()"
}

func (d *pgDialect) supportTransactions() bool {
	return true
}

func (d *pgDialect) isRetriable(err error) bool {
	if pqErr, ok := err.(*pq.Error); ok {
		if pqErr.Code == "40P01" { // deadlock error
			return true
		}
	}
	return false
}

func (d *pgDialect) canRollback(err error) bool {
	// current pq lib will mark connection as "bad" after timeout and will return driver.ErrBadConn
	return !errors.Is(err, context.Canceled)
}

func (d *pgDialect) table(table string) string {
	if d.schemaName != "" {
		return d.schemaName + "." + table
	}

	return table
}

func (d *pgDialect) schema() string {
	return d.schemaName
}

// Recommendations returns PostgreSQL recommendations for DB settings
func (d *pgDialect) recommendations() []db.Recommendation {
	return []db.Recommendation{
		{Setting: "shared_buffers", Meaning: "primary DB cache", MinVal: int64(1 * db.GByte), RecommendedVal: int64(4 * db.GByte)},
		{Setting: "effective_cache_size", Meaning: "OS cache", MinVal: int64(2 * db.GByte), RecommendedVal: int64(8 * db.GByte)},
		{Setting: "work_mem", Meaning: "Mem for internal sorting & hash tables", MinVal: int64(8 * db.MByte), RecommendedVal: int64(16 * db.MByte)},
		{Setting: "maintenance_work_mem", Meaning: "Mem for VACUUM, CREATE INDEX, etc", MinVal: int64(128 * db.MByte), RecommendedVal: int64(256 * db.MByte)},
		{Setting: "wal_buffers", Meaning: "Mem used in shared memory for WAL data", MinVal: int64(8 * db.MByte), RecommendedVal: int64(16 * db.MByte)},
		{Setting: "max_wal_size", Meaning: "max WAL size", MinVal: int64(512 * db.MByte), RecommendedVal: int64(1 * db.GByte)},
		{Setting: "min_wal_size", Meaning: "min WAL size", MinVal: int64(32 * db.MByte), RecommendedVal: int64(64 * db.MByte)},
		{Setting: "max_connections", Meaning: "max allowed number of DB connections", MinVal: int64(512), RecommendedVal: int64(2048)},
		{Setting: "random_page_cost", Meaning: "it should be 1.1 as it is typical for SSD", ExpectedValue: "1.1"},
		{Setting: "track_activities", Meaning: "collects session activities info", ExpectedValue: "on"},
		{Setting: "track_counts", Meaning: "track tables/indexes access count", ExpectedValue: "on"},
		{Setting: "log_checkpoints", Meaning: "logs information about each checkpoint", ExpectedValue: "on"},
		{Setting: "jit", Meaning: "JIT compilation feature", ExpectedValue: "off"},
		// effective_io_concurrency = 2 # For SSDs, this might be set to the number of separate SSDs or channels.
	}
}

func (d *pgDialect) close() error {
	if d.embedded {
		return pgmbed.Terminate()
	}

	return nil
}

type pgConnector struct{}

func postgresSchemaAndConnString(cs string) (string, string, error) {
	const schemaParamName = "schema"
	const sslModeParamName = "sslmode"
	var schemaName string

	var u, err = url.Parse(cs)
	if err != nil {
		return "", "", fmt.Errorf("cannot parse connection url %v, err: %v", cs, err)
	} else {
		m, _ := url.ParseQuery(u.RawQuery)
		if s, ok := m[schemaParamName]; ok {
			schemaName = s[0]
			delete(m, schemaParamName)
			u.RawQuery = m.Encode()
			cs = u.String()
		}
		// adding disable sslmode by default
		if _, ok := m[sslModeParamName]; !ok {
			m[sslModeParamName] = []string{"disable"}
			u.RawQuery = m.Encode()
			cs = u.String()
		}
	}

	return schemaName, cs, nil
}

func initializePostgresDB(cs string, logger logger.Logger) (string, dialect, error) {
	var schemaName, cleanedConnectionString, err = postgresSchemaAndConnString(cs)
	if err != nil {
		return "", nil, fmt.Errorf("db: postgres: %v", err)
	}

	var embeddedPostgresOpts *pgmbed.Opts
	if cs, embeddedPostgresOpts, err = pgmbed.ParseOptions(cs); err != nil {
		return "", nil, fmt.Errorf("db: postgres: %v", err)
	}

	var embeddedPostgresEnabled bool
	if embeddedPostgresOpts != nil && embeddedPostgresOpts.Enabled {
		cs, err = pgmbed.Launch(cs, embeddedPostgresOpts, logger)
		if err != nil {
			return "", nil, fmt.Errorf("db: cannot initialize embedded postgres: %v", err)
		}
		embeddedPostgresEnabled = true
		cleanedConnectionString = cs
	}

	return cleanedConnectionString, &pgDialect{schemaName: schemaName, embedded: embeddedPostgresEnabled}, err
}

func (c *pgConnector) ConnectionPool(cfg db.Config) (db.Database, error) {
	dbo := &sqlDatabase{}
	var rwc *sql.DB

	var cs, dia, err = initializePostgresDB(cfg.ConnString, cfg.SystemLogger)
	if err != nil {
		return nil, err
	}

	if rwc, err = sql.Open("postgres", cs); err != nil {
		return nil, fmt.Errorf("db: cannot connect to postgresql db at %v, err: %v", sanitizeConn(cfg.ConnString), err)
	}

	if err = rwc.Ping(); err != nil {
		return nil, fmt.Errorf("db: failed ping postgresql db at %v, err: %v\n", sanitizeConn(cfg.ConnString), err)
	}

	dbo.rw = &sqlQuerier{rwc}
	dbo.t = &sqlQuerier{rwc}

	maxConn := int(math.Max(1, float64(cfg.MaxOpenConns)))
	rwc.SetMaxOpenConns(maxConn)
	rwc.SetMaxIdleConns(maxConn)

	rwc.SetConnMaxLifetime(cfg.MaxConnLifetime)

	dbo.dialect = dia
	dbo.useTruncate = cfg.UseTruncate
	dbo.queryStringInterpolation = cfg.QueryStringInterpolation
	dbo.dryRun = cfg.DryRun
	dbo.queryLogger = cfg.QueryLogger
	dbo.readRowsLogger = cfg.ReadRowsLogger
	dbo.explainLogger = cfg.ExplainLogger

	return dbo, nil
}

func (c *pgConnector) DialectName(scheme string) (db.DialectName, error) {
	return db.POSTGRES, nil
}
