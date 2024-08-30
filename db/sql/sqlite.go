package sql

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3" // sqlite3 driver

	"github.com/acronis/perfkit/db"
)

func init() {
	if err := db.Register("sqlite", &sqliteConnector{}); err != nil {
		panic(err)
	}
}

type sqliteDialect struct {
	memmode bool
}

func (d *sqliteDialect) name() db.DialectName {
	return db.SQLITE
}

// GetType returns SQLite-specific types
func (d *sqliteDialect) getType(id string) string {
	switch id {
	case "{$bigint_autoinc_pk}":
		return "INTEGER PRIMARY KEY AUTOINCREMENT"
	case "{$bigint_autoinc}":
		return "INTEGER AUTOINCREMENT"
	case "{$ascii}":
		return ""
	case "{$uuid}":
		return "TEXT"
	case "{$varchar_uuid}":
		return "TEXT"
	case "{$longblob}":
		return "BLOB"
	case "{$hugeblob}":
		return "BLOB"
	case "{$datetime}":
		return "TEXT"
	case "{$datetime6}":
		return "TEXT"
	case "{$timestamp6}":
		return "TEXT"
	case "{$current_timestamp6}":
		return "CURRENT_TIMESTAMP"
	case "{$binary20}":
		return "BLOB"
	case "{$binaryblobtype}":
		return "MEDIUMBLOB"
	case "{$boolean}":
		return "BOOLEAN"
	case "{$boolean_false}":
		return "0"
	case "{$boolean_true}":
		return "1"
	case "{$tinyint}":
		return "SMALLINT"
	case "{$longtext}":
		return "TEXT"
	case "{$unique}":
		return "unique"
	case "{$engine}":
		return ""
	case "{$notnull}":
		return "not null"
	case "{$null}":
		return "null"
	case "{$tenant_uuid_bound_id}":
		return "TEXT"
	default:
		return ""
	}
}

func (d *sqliteDialect) randFunc() string {
	return "RANDOM()"
}

func (d *sqliteDialect) isRetriable(err error) bool {
	return false
}

func (d *sqliteDialect) canRollback(err error) bool {
	return true
}

func (d *sqliteDialect) table(table string) string {
	return table
}

func (d *sqliteDialect) schema() string {
	return ""
}

// Recommendations returns SQLite recommendations for DB settings
func (d *sqliteDialect) recommendations() []db.Recommendation {
	return nil
}

func (d *sqliteDialect) close() error {
	return nil
}

type sqliteConnector struct{}

func (c *sqliteConnector) ConnectionPool(cfg db.Config) (db.Database, error) {
	_, path, err := db.ParseScheme(cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("db: cannot parse sqlite db path, err: %v", err)
	}

	if path == "" {
		return nil, fmt.Errorf("db: empty sqlite file path")
	}

	var dia = sqliteDialect{}
	if strings.Contains(path, ":memory:") || strings.Contains(path, "mode=memory") {
		dia.memmode = true
	} else if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("db: filepath '%v' is not absolute", sanitizeConn(cfg.ConnString))
	}

	dbo := &sqlDatabase{}
	var rwc *sql.DB

	if rwc, err = sql.Open("sqlite3", path); err != nil {
		return nil, fmt.Errorf("db: cannot open sqlite db at %v, err: %v", sanitizeConn(cfg.ConnString), err)
	}

	if err = rwc.Ping(); err != nil {
		return nil, fmt.Errorf("db: failed ping sqlite db at %v, err: %v", sanitizeConn(cfg.ConnString), err)
	}

	dbo.rw = &sqlQuerier{rwc}
	dbo.t = &sqlQuerier{rwc}

	options := `PRAGMA page_size = 4096;
		PRAGMA cache_size = -20000;
		PRAGMA journal_mode=WAL;
		PRAGMA wal_autocheckpoint = 5000;
		PRAGMA wal_checkpoint(RESTART);
		PRAGMA synchronous = NORMAL;`

	if _, err = rwc.Exec(options); err != nil {
		return nil, fmt.Errorf("db: failed to set sqlite options, err: %v", err)
	}

	rwc.SetMaxOpenConns(cfg.MaxOpenConns)
	rwc.SetMaxIdleConns(cfg.MaxOpenConns)

	dbo.dialect = &dia
	dbo.queryLogger = cfg.QueryLogger

	return dbo, nil
}

func (c *sqliteConnector) DialectName(scheme string) (db.DialectName, error) {
	return db.SQLITE, nil
}
