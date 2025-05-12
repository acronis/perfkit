package sql

import (
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	_ "github.com/ClickHouse/clickhouse-go/v2" // clickhouse driver

	"github.com/acronis/perfkit/db"
)

func init() {
	if err := db.Register("clickhouse", &clickhouseConnector{}); err != nil {
		panic(err)
	}
}

type clickHouseDialect struct{}

func (d *clickHouseDialect) name() db.DialectName {
	return db.CLICKHOUSE
}

func (d *clickHouseDialect) argumentPlaceholder(index int) string {
	return "?"
}

func (d *clickHouseDialect) encodeString(s string) string {
	// borrowed from dbr
	// http://www.postgresql.org/docs/9.2/static/sql-syntax-lexical.html
	return `'` + strings.Replace(s, `'`, `''`, -1) + `'`
}

func (d *clickHouseDialect) encodeUUID(s uuid.UUID) string {
	return d.encodeString(s.String())
}

func (d *clickHouseDialect) encodeVector(vs []float32) string {
	return ""
}

func (d *clickHouseDialect) encodeOrderByVector(field, operator, vector string) string {
	return "" // ClickHouse doesn't support vector search
}

func (d *clickHouseDialect) encodeBool(b bool) string {
	// borrowed from dbr
	if b {
		return "TRUE"
	}
	return "FALSE"
}

func (d *clickHouseDialect) encodeBytes(bs []byte) string {
	// borrowed from dbr, using string for json fields
	return d.encodeString(string(bs))
}

func (d *clickHouseDialect) encodeTime(timestamp time.Time) string {
	return `'` + timestamp.UTC().Format(time.RFC3339Nano) + `'`
}

// GetType returns ClickHouse-specific types
func (d *clickHouseDialect) getType(id db.DataType) string {
	switch id {
	// Primary Keys and IDs
	case db.DataTypeId:
		return "UInt64" // Auto-incrementing primary key
	case db.DataTypeTenantUUIDBoundID:
		return "String"

	// Integer Types
	case db.DataTypeInt:
		return "UInt64"
	case db.DataTypeBigInt:
		return "UInt64"
	case db.DataTypeBigIntAutoIncPK:
		return "UInt64" // ClickHouse does not support auto-increment
	case db.DataTypeBigIntAutoInc:
		return "UInt64"
	case db.DataTypeSmallInt:
		return "Int16"
	case db.DataTypeTinyInt:
		return "Int8"

	// String Types
	case db.DataTypeVarChar:
		return "String"
	case db.DataTypeVarChar32:
		return "String"
	case db.DataTypeVarChar36:
		return "String"
	case db.DataTypeVarChar64:
		return "String"
	case db.DataTypeVarChar128:
		return "String"
	case db.DataTypeVarChar256:
		return "String"
	case db.DataTypeText:
		return "String"
	case db.DataTypeLongText:
		return "String"
	case db.DataTypeAscii:
		return "" // Charset specification is not needed in ClickHouse

	// UUID Types
	case db.DataTypeUUID:
		return "UUID"
	case db.DataTypeVarCharUUID:
		return "FixedString(36)"

	// Binary Types
	case db.DataTypeLongBlob:
		return "String"
	case db.DataTypeHugeBlob:
		return "String"
	case db.DataTypeBinary20:
		return "FixedString(20)"
	case db.DataTypeBinaryBlobType:
		return "String"

	// Date and Time Types
	case db.DataTypeDateTime:
		return "DateTime"
	case db.DataTypeDateTime6:
		return "DateTime64(6)"
	case db.DataTypeTimestamp:
		return "DateTime"
	case db.DataTypeTimestamp6:
		return "DateTime64(6)"
	case db.DataTypeCurrentTimeStamp6:
		return "now64(6)"

	// Boolean Types
	case db.DataTypeBoolean:
		return "UInt8"
	case db.DataTypeBooleanFalse:
		return "0"
	case db.DataTypeBooleanTrue:
		return "1"

	// Special Types
	case db.DataTypeJSON:
		return "String" // Store JSON as string in ClickHouse
	case db.DataTypeVector3Float32:
		return "Array(Float32)"
	case db.DataTypeVector768Float32:
		return "Array(Float32)"

	// Constraints and Modifiers
	case db.DataTypeUnique:
		return ""
	case db.DataTypeEngine:
		return "ENGINE = MergeTree() ORDER BY id;"
	case db.DataTypeNotNull:
		return "not null"
	case db.DataTypeNull:
		return "null"

	default:
		return ""
	}
}

func (d *clickHouseDialect) randFunc() string {
	return ""
}

func (d *clickHouseDialect) supportTransactions() bool {
	return true
}

func (d *clickHouseDialect) isRetriable(err error) bool {
	return false
}

func (d *clickHouseDialect) canRollback(err error) bool {
	return true
}

func (d *clickHouseDialect) table(table string) string {
	return table
}

func (d *clickHouseDialect) schema() string {
	return ""
}

// Recommendations returns ClickHouse recommendations for DB settings
func (d *clickHouseDialect) recommendations() []db.Recommendation {
	return nil
}

func (d *clickHouseDialect) close() error {
	return nil
}

type clickhouseConnector struct{}

func (c *clickhouseConnector) ConnectionPool(cfg db.Config) (db.Database, error) {
	var _, _, err = db.ParseScheme(cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("db: cannot parse clickhouse db path, err: %v", err)
	}

	dbo := &sqlDatabase{}
	var rwc *sql.DB

	if rwc, err = sql.Open("clickhouse", cfg.ConnString); err != nil {
		return nil, fmt.Errorf("db: cannot connect to clickhouse db at %v, err: %v", sanitizeConn(cfg.ConnString), err)
	}

	if err = rwc.Ping(); err != nil {
		return nil, fmt.Errorf("db: failed ping clickhouse db at %v, err: %v", sanitizeConn(cfg.ConnString), err)
	}

	dbo.rw = &sqlQuerier{rwc}
	dbo.t = &sqlQuerier{rwc}

	maxConn := int(math.Max(1, float64(cfg.MaxConnLifetime)))
	maxConnLifetime := cfg.MaxConnLifetime

	rwc.SetMaxOpenConns(maxConn)
	rwc.SetMaxIdleConns(maxConn)

	if maxConnLifetime > 0 {
		rwc.SetConnMaxLifetime(maxConnLifetime)
	}

	dbo.dialect = &clickHouseDialect{}
	dbo.useTruncate = cfg.UseTruncate
	dbo.queryStringInterpolation = cfg.QueryStringInterpolation
	dbo.dryRun = cfg.DryRun
	dbo.logTime = cfg.LogOperationsTime
	dbo.queryLogger = cfg.QueryLogger
	dbo.readRowsLogger = cfg.ReadRowsLogger
	dbo.explainLogger = cfg.ExplainLogger

	return dbo, nil
}

func (c *clickhouseConnector) DialectName(scheme string) (db.DialectName, error) {
	return db.CLICKHOUSE, nil
}
