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
	case db.DataTypeInt:
		return "UInt64" // Int for integers
	case db.DataTypeVarChar:
		return "String"
	case db.DataTypeVarChar256:
		return "String"
	case db.DataTypeText:
		return "String"
	case db.DataTypeBigInt:
		return "UInt64"
	case db.DataTypeBigIntAutoIncPK:
		return "UInt64" // ClickHouse does not support auto-increment
	case db.DataTypeBigIntAutoInc:
		return "UInt64" // Use UInt64 for large integers
	case db.DataTypeAscii:
		return "" // Charset specification is not needed in ClickHouse
	case db.DataTypeUUID:
		return "UUID" // ClickHouse supports UUID type
	case db.DataTypeVarCharUUID:
		return "FixedString(36)" // ClickHouse supports UUID type
	case db.DataTypeLongBlob:
		return "String" // Use String for binary data
	case db.DataTypeHugeBlob:
		return "String" // Use String for binary data
	case db.DataTypeDateTime:
		return "DateTime" // DateTime type for date and time
	case db.DataTypeDateTime6:
		return "DateTime64(6)" // DateTime64 with precision for fractional seconds
	case db.DataTypeTimestamp6:
		return "DateTime64(6)" // DateTime64 for timestamp with fractional seconds
	case db.DataTypeCurrentTimeStamp6:
		return "now64(6)" // Function for current timestamp
	case db.DataTypeBinary20:
		return "FixedString(20)" // FixedString for fixed-length binary data
	case db.DataTypeBinaryBlobType:
		return "String" // Use String for binary data
	case db.DataTypeBoolean:
		return "UInt8" // ClickHouse uses UInt8 for boolean values
	case db.DataTypeBooleanFalse:
		return "0"
	case db.DataTypeBooleanTrue:
		return "1"
	case db.DataTypeTinyInt:
		return "Int8" // Int8 for small integers
	case db.DataTypeLongText:
		return "String" // Use String for long text
	case db.DataTypeUnique:
		return "" // Unique values are not supported
	case db.DataTypeEngine:
		return "ENGINE = MergeTree() ORDER BY id;"
	case db.DataTypeNotNull:
		return "not null"
	case db.DataTypeNull:
		return "null"
	case db.DataTypeTenantUUIDBoundID:
		return "String"
	default:
		return ""
	}
}

func (d *clickHouseDialect) randFunc() string {
	return ""
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
	dbo.queryLogger = cfg.QueryLogger

	return dbo, nil
}

func (c *clickhouseConnector) DialectName(scheme string) (db.DialectName, error) {
	return db.CLICKHOUSE, nil
}
