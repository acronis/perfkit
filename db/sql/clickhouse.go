package sql

import (
	"database/sql"
	"fmt"
	"math"

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

// GetType returns ClickHouse-specific types
func (d *clickHouseDialect) getType(id string) string {
	switch id {
	case "{$bigint_autoinc_pk}":
		return "UInt64" // ClickHouse does not support auto-increment
	case "{$bigint_autoinc}":
		return "UInt64" // Use UInt64 for large integers
	case "{$ascii}":
		return "" // Charset specification is not needed in ClickHouse
	case "{$uuid}":
		return "UUID" // ClickHouse supports UUID type
	case "{$varchar_uuid}":
		return "FixedString(36)" // ClickHouse supports UUID type
	case "{$longblob}":
		return "String" // Use String for binary data
	case "{$hugeblob}":
		return "String" // Use String for binary data
	case "{$datetime}":
		return "DateTime" // DateTime type for date and time
	case "{$datetime6}":
		return "DateTime64(6)" // DateTime64 with precision for fractional seconds
	case "{$timestamp6}":
		return "DateTime64(6)" // DateTime64 for timestamp with fractional seconds
	case "{$current_timestamp6}":
		return "now64(6)" // Function for current timestamp
	case "{$binary20}":
		return "FixedString(20)" // FixedString for fixed-length binary data
	case "{$binaryblobtype}":
		return "String" // Use String for binary data
	case "{$boolean}":
		return "UInt8" // ClickHouse uses UInt8 for boolean values
	case "{$boolean_false}":
		return "0"
	case "{$boolean_true}":
		return "1"
	case "{$tinyint}":
		return "Int8" // Int8 for small integers
	case "{$longtext}":
		return "String" // Use String for long text
	case "{$unique}":
		return "" // Unique values are not supported
	case "{$engine}":
		return "ENGINE = MergeTree() ORDER BY id;"
	case "{$notnull}":
		return "not null"
	case "{$null}":
		return "null"
	case "{$tenant_uuid_bound_id}":
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
