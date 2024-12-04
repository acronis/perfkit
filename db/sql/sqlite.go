package sql

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

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

func (d *sqliteDialect) encodeString(s string) string {
	// borrowed from dbr
	// https://www.sqlite.org/faq.html
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}

func (d *sqliteDialect) encodeUUID(s uuid.UUID) string {
	return d.encodeString(s.String())
}

func (d *sqliteDialect) encodeBool(b bool) string {
	// borrowed from dbr
	// https://www.sqlite.org/lang_expr.html
	if b {
		return "1"
	}
	return "0"
}

func (d *sqliteDialect) encodeBytes(bs []byte) string {
	// borrowed from dbr
	// https://www.sqlite.org/lang_expr.html
	return fmt.Sprintf(`X'%x'`, bs)
}

func (d *sqliteDialect) encodeTime(timestamp time.Time) string {
	// borrowed from dbr
	// https://www.sqlite.org/lang_datefunc.html
	return `'` + timestamp.UTC().Format(time.RFC3339Nano) + `'`
}

// GetType returns SQLite-specific types
func (d *sqliteDialect) getType(id db.DataType) string {
	switch id {
	case db.DataTypeInt:
		return "INT"
	case db.DataTypeString:
		return "VARCHAR"
	case db.DataTypeString256:
		return "VARCHAR(256)"
	case db.DataTypeBigInt:
		return "INTEGER"
	case db.DataTypeBigIntAutoIncPK:
		return "INTEGER PRIMARY KEY AUTOINCREMENT"
	case db.DataTypeBigIntAutoInc:
		return "INTEGER AUTOINCREMENT"
	case db.DataTypeAscii:
		return ""
	case db.DataTypeUUID:
		return "TEXT"
	case db.DataTypeVarCharUUID:
		return "TEXT"
	case db.DataTypeLongBlob:
		return "BLOB"
	case db.DataTypeHugeBlob:
		return "BLOB"
	case db.DataTypeDateTime:
		return "TEXT"
	case db.DataTypeDateTime6:
		return "TEXT"
	case db.DataTypeTimestamp6:
		return "TEXT"
	case db.DataTypeCurrentTimeStamp6:
		return "CURRENT_TIMESTAMP"
	case db.DataTypeBinary20:
		return "BLOB"
	case db.DataTypeBinaryBlobType:
		return "MEDIUMBLOB"
	case db.DataTypeBoolean:
		return "BOOLEAN"
	case db.DataTypeBooleanFalse:
		return "0"
	case db.DataTypeBooleanTrue:
		return "1"
	case db.DataTypeTinyInt:
		return "SMALLINT"
	case db.DataTypeLongText:
		return "TEXT"
	case db.DataTypeUnique:
		return "unique"
	case db.DataTypeEngine:
		return ""
	case db.DataTypeNotNull:
		return "not null"
	case db.DataTypeNull:
		return "null"
	case db.DataTypeTenantUUIDBoundID:
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
