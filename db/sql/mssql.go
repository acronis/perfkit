package sql

import (
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	mssql "github.com/denisenkom/go-mssqldb" // mssql driver

	"github.com/acronis/perfkit/db"
)

// nolint: gochecknoinits // required by go-mssqldb
func init() {
	for _, msNameStyle := range []string{"mssql", "sqlserver"} {
		if err := db.Register(msNameStyle, &msConnector{}); err != nil {
			panic(err)
		}
	}
}

type msDialect struct{}

func (d *msDialect) name() db.DialectName {
	return db.MSSQL
}

func (d *msDialect) encodeString(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}

func (d *msDialect) encodeUUID(s uuid.UUID) string {
	return d.encodeString(s.String())
}

func (d *msDialect) encodeVector(vs []float32) string {
	return ""
}

func (d *msDialect) encodeBool(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func (d *msDialect) encodeBytes(bs []byte) string {
	return fmt.Sprintf("0x%x", bs)
}

func (d *msDialect) encodeTime(timestamp time.Time) string {
	return `'` + timestamp.UTC().Format(time.RFC3339Nano) + `'`
}

// GetType returns SQL Server-specific types
func (d *msDialect) getType(id db.DataType) string {
	switch id {
	case db.DataTypeInt:
		return "BIGINT"
	case db.DataTypeVarChar:
		return "VARCHAR"
	case db.DataTypeVarChar256:
		return "VARCHAR(256)"
	case db.DataTypeText:
		return "VARCHAR"
	case db.DataTypeBigInt:
		return "BIGINT"
	case db.DataTypeBigIntAutoIncPK:
		return "BIGINT IDENTITY(1,1) PRIMARY KEY"
	case db.DataTypeBigIntAutoInc:
		return "BIGINT IDENTITY(1,1)"
	case db.DataTypeAscii:
		return ""
	case db.DataTypeUUID:
		return "UNIQUEIDENTIFIER"
	case db.DataTypeVarCharUUID:
		return "VARCHAR(36)"
	case db.DataTypeLongBlob:
		return "VARCHAR(MAX)"
	case db.DataTypeHugeBlob:
		return "VARBINARY(MAX)"
	case db.DataTypeDateTime:
		return "DATETIME"
	case db.DataTypeDateTime6:
		return "DATETIME2(6)"
	case db.DataTypeTimestamp6:
		return "DATETIME2(6)"
	case db.DataTypeCurrentTimeStamp6:
		return "SYSDATETIME()"
	case db.DataTypeBinary20:
		return "BINARY(20)"
	case db.DataTypeBinaryBlobType:
		return "varbinary(max)"
	case db.DataTypeBoolean:
		return "BIT"
	case db.DataTypeBooleanFalse:
		return "0"
	case db.DataTypeBooleanTrue:
		return "1"
	case db.DataTypeTinyInt:
		return "TINYINT"
	case db.DataTypeLongText:
		return "NVARCHAR(MAX)"
	case db.DataTypeUnique:
		return "unique"
	case db.DataTypeEngine:
		return ""
	case db.DataTypeNotNull:
		return "not null"
	case db.DataTypeNull:
		return "null"
	case db.DataTypeTenantUUIDBoundID:
		return "VARCHAR(64)"
	default:
		return ""
	}
}

func (d *msDialect) randFunc() string {
	return "NEWID()"
}

func (d *msDialect) isDeadlock(err error) bool {
	if msErr, ok := err.(mssql.Error); ok {
		if msErr.Number == 1205 { // deadlock error
			return true
		}
	}
	return false
}

func (d *msDialect) supportTransactions() bool {
	return true
}

func (d *msDialect) isRetriable(err error) bool {
	return d.isDeadlock(err)
}

func (d *msDialect) canRollback(err error) bool {
	return !d.isDeadlock(err) // mssql destroys deadlocked transaction by itself, rollback from application results in error
}

func (d *msDialect) table(table string) string {
	return table
}

func (d *msDialect) schema() string {
	return ""
}

// Recommendations returns SQL Server recommendations for DB settings
func (d *msDialect) recommendations() []db.Recommendation {
	return nil
}

func (d *msDialect) close() error {
	return nil
}

type msConnector struct{}

func (c *msConnector) ConnectionPool(cfg db.Config) (db.Database, error) {
	dbo := &sqlDatabase{}
	var err error
	var rwc *sql.DB

	if rwc, err = sql.Open("sqlserver", cfg.ConnString); err != nil {
		return nil, fmt.Errorf("sql: cannot connect to sql server db at %v, err: %v", sanitizeConn(cfg.ConnString), err)
	}

	if err = rwc.Ping(); err != nil {
		return nil, fmt.Errorf("sql: failed ping sql server db at %v, err: %v", sanitizeConn(cfg.ConnString), err)
	}

	dbo.rw = &sqlQuerier{rwc}
	dbo.t = &sqlQuerier{rwc}

	rwc.SetMaxOpenConns(int(math.Max(1, float64(cfg.MaxOpenConns))))

	if cfg.MaxConnLifetime > 0 {
		rwc.SetConnMaxLifetime(cfg.MaxConnLifetime)
	}

	dbo.dialect = &msDialect{}
	dbo.queryStringInterpolation = cfg.QueryStringInterpolation
	dbo.dryRun = cfg.DryRun
	dbo.queryLogger = cfg.QueryLogger

	return dbo, nil
}

func (c *msConnector) DialectName(scheme string) (db.DialectName, error) {
	return db.MSSQL, nil
}
