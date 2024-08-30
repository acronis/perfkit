package sql

import (
	"database/sql"
	"fmt"
	"math"

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

// GetType returns SQL Server-specific types
func (d *msDialect) getType(id string) string {
	switch id {
	case "{$bigint_autoinc_pk}":
		return "BIGINT IDENTITY(1,1) PRIMARY KEY"
	case "{$bigint_autoinc}":
		return "BIGINT IDENTITY(1,1)"
	case "{$ascii}":
		return ""
	case "{$uuid}":
		return "UNIQUEIDENTIFIER"
	case "{$varchar_uuid}":
		return "VARCHAR(36)"
	case "{$longblob}":
		return "VARCHAR(MAX)"
	case "{$hugeblob}":
		return "VARBINARY(MAX)"
	case "{$datetime}":
		return "DATETIME"
	case "{$datetime6}":
		return "DATETIME2(6)"
	case "{$timestamp6}":
		return "DATETIME2(6)"
	case "{$current_timestamp6}":
		return "SYSDATETIME()"
	case "{$binary20}":
		return "BINARY(20)"
	case "{$binaryblobtype}":
		return "varbinary(max)"
	case "{$boolean}":
		return "BIT"
	case "{$boolean_false}":
		return "0"
	case "{$boolean_true}":
		return "1"
	case "{$tinyint}":
		return "TINYINT"
	case "{$longtext}":
		return "NVARCHAR(MAX)"
	case "{$unique}":
		return "unique"
	case "{$engine}":
		return ""
	case "{$notnull}":
		return "not null"
	case "{$null}":
		return "null"
	case "{$tenant_uuid_bound_id}":
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
	dbo.queryLogger = cfg.QueryLogger

	return dbo, nil
}

func (c *msConnector) DialectName(scheme string) (db.DialectName, error) {
	return db.MSSQL, nil
}
