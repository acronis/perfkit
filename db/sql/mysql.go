package sql

import (
	"bytes"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/go-sql-driver/mysql" // mysql driver

	"github.com/acronis/perfkit/db"
)

func init() {
	if err := db.Register("mysql", &mysqlConnector{}); err != nil {
		panic(err)
	}
}

type mysqlDialect struct {
	version   string
	sqlEngine string
}

func (d *mysqlDialect) name() db.DialectName {
	return db.MYSQL
}

func (d *mysqlDialect) encodeString(s string) string {
	// borrowed from dbr
	buf := new(bytes.Buffer)

	buf.WriteByte('\'')
	// https://dev.mysql.com/doc/refman/5.7/en/string-literals.html
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case 0:
			buf.WriteString(`\0`)
		case '\'':
			buf.WriteString(`\'`)
		case '"':
			buf.WriteString(`\"`)
		case '\b':
			buf.WriteString(`\b`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		case 26:
			buf.WriteString(`\Z`)
		case '\\':
			buf.WriteString(`\\`)
		default:
			buf.WriteByte(s[i])
		}
	}

	buf.WriteByte('\'')
	return buf.String()
}

func (d *mysqlDialect) encodeUUID(s uuid.UUID) string {
	return d.encodeString(s.String())
}

func (d *mysqlDialect) encodeVector(vs []float32) string {
	return ""
}

func (d *mysqlDialect) encodeBool(b bool) string {
	// borrowed from dbr
	if b {
		return "1"
	}
	return "0"
}

func (d *mysqlDialect) encodeBytes(bs []byte) string {
	// borrowed from dbr
	return fmt.Sprintf(`0x%x`, bs)
}

func (d *mysqlDialect) encodeTime(timestamp time.Time) string {
	return fmt.Sprintf("'%s'", timestamp.UTC().Format(time.RFC3339Nano))
}

// GetType returns MySQL-specific types
func (d *mysqlDialect) getType(id db.DataType) string {
	switch id {
	case db.DataTypeInt:
		return "INT"
	case db.DataTypeVarChar:
		return "VARCHAR"
	case db.DataTypeVarChar32:
		return "VARCHAR(32)"
	case db.DataTypeVarChar64:
		return "VARCHAR(64)"
	case db.DataTypeVarChar128:
		return "VARCHAR(128)"
	case db.DataTypeVarChar256:
		return "VARCHAR(256)"
	case db.DataTypeText:
		return "TEXT"
	case db.DataTypeJSON:
		return "JSON"
	case db.DataTypeBigInt:
		return "BIGINT"
	case db.DataTypeBigIntAutoIncPK:
		return "BIGINT AUTO_INCREMENT PRIMARY KEY"
	case db.DataTypeBigIntAutoInc:
		return "BIGINT AUTO_INCREMENT"
	case db.DataTypeSmallInt:
		return "SMALLINT"
	case db.DataTypeAscii:
		return "CHARACTER SET ascii"
	case db.DataTypeUUID:
		return "VARCHAR(36)"
	case db.DataTypeVarCharUUID:
		return "VARCHAR(36)"
	case db.DataTypeTenantUUIDBoundID:
		return "VARCHAR(64)"
	case db.DataTypeLongBlob:
		return "LONGBLOB"
	case db.DataTypeHugeBlob:
		return "LONGBLOB"
	case db.DataTypeDateTime:
		return "DATETIME"
	case db.DataTypeDateTime6:
		return "DATETIME(6)"
	case db.DataTypeTimestamp:
		return "TIMESTAMP"
	case db.DataTypeTimestamp6:
		return "TIMESTAMP(6)"
	case db.DataTypeCurrentTimeStamp6:
		return "CURRENT_TIMESTAMP(6)"
	case db.DataTypeBinary20:
		return "BINARY(20)"
	case db.DataTypeBinaryBlobType:
		return "MEDIUMBLOB"
	case db.DataTypeBoolean:
		return "BOOLEAN"
	case db.DataTypeBooleanFalse:
		return "0"
	case db.DataTypeBooleanTrue:
		return "1"
	case db.DataTypeTinyInt:
		return "TINYINT"
	case db.DataTypeLongText:
		return "LONGTEXT"
	case db.DataTypeUnique:
		return "unique"
	case db.DataTypeEngine:
		if d.sqlEngine == "xpand-allnodes" {
			return "engine = xpand"
		} else if d.sqlEngine != "" {
			return "engine = " + d.sqlEngine
		} else {
			return "engine = innodb"
		}
	case db.DataTypeNotNull:
		return "not null"
	case db.DataTypeNull:
		return "null"
	default:
		return ""
	}
}

func (d *mysqlDialect) randFunc() string {
	return "RAND()"
}

func (d *mysqlDialect) isRetriable(err error) bool {
	if mysqlErr, ok := err.(*mysql.MySQLError); ok {
		switch mysqlErr.Number {
		case 0x4bd, /*deadlock*/
			0x4b5 /*lock timedout*/ :
			return true
		}
	}
	if err == mysql.ErrInvalidConn {
		return true
	}
	return false
}

func (d *mysqlDialect) canRollback(err error) bool {
	return err != mysql.ErrInvalidConn
}

func (d *mysqlDialect) table(table string) string {
	return table
}

func (d *mysqlDialect) schema() string {
	return ""
}

// isMySQL56 checks if DB is MySQL 5.6
func (d *mysqlDialect) isMySQL56() bool {
	if strings.HasPrefix(d.version, "5.6") {
		return true
	}

	return false
}

// isGaleraCluster checks if DB is Galera Cluster
func (d *mysqlDialect) isGaleraCluster() bool {
	if strings.Contains(d.version, "Percona") && strings.Contains(d.version, "Cluster") {
		return true
	}

	return false
}

// Recommendations returns MySQL recommendations for DB settings
func (d *mysqlDialect) recommendations() []db.Recommendation {
	var r []db.Recommendation

	if d.isMySQL56() {
		r = []db.Recommendation{
			{Setting: "innodb_buffer_pool_size", Meaning: "primary DB cache per pool", MinVal: int64(2 * db.GByte), RecommendedVal: int64(3 * db.GByte)},
			{Setting: "innodb_buffer_pool_instances", Meaning: "number of DB cache pools", MinVal: int64(4), RecommendedVal: int64(8)},
			{Setting: "innodb_log_file_size", Meaning: "InnoDB redo log size", MinVal: int64(256 * db.MByte), RecommendedVal: int64(512 * db.MByte)},
			{Setting: "innodb_log_files_in_group", Meaning: "number of redo log files per group", MinVal: int64(2), RecommendedVal: int64(2)},
		}

		if d.isGaleraCluster() {
			r = append(r, db.Recommendation{Setting: "wsrep_slave_threads", Meaning: "number of threads replicating write sets", MinVal: int64(4), RecommendedVal: int64(8)})
		}
	} else {
		r = []db.Recommendation{
			{Setting: "innodb_buffer_pool_size", Meaning: "primary DB cache", MinVal: int64(8 * db.GByte), RecommendedVal: int64(12 * db.GByte)},
			{Setting: "innodb_log_file_size", Meaning: "InnoDB redo log size", MinVal: int64(512 * db.MByte), RecommendedVal: int64(2 * db.GByte)},
		}
	}

	r = append(r, db.Recommendation{Setting: "max_connections", Meaning: "max allowed number of DB connections", MinVal: int64(512), RecommendedVal: int64(2048)})
	r = append(r, db.Recommendation{Setting: "query_cache_type", Meaning: "query cache policy", ExpectedValue: "OFF"})
	r = append(r, db.Recommendation{Setting: "performance_schema", Meaning: "performance-related server metrics", ExpectedValue: "ON"})

	return r
}

func (d *mysqlDialect) close() error {
	return nil
}

type mysqlConnector struct{}

func (c *mysqlConnector) ConnectionPool(cfg db.Config) (db.Database, error) {
	var _, cs, err = db.ParseScheme(cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("db: cannot parse mysql db path, err: %v", err)
	}

	dbo := &sqlDatabase{}
	var rwc *sql.DB

	dsn := cs + "?" + "maxAllowedPacket=" + strconv.Itoa(cfg.MaxPacketSize) + "&parseTime=true"
	if rwc, err = sql.Open("mysql", dsn); err != nil {
		return nil, fmt.Errorf("db: cannot connect to mysql db at %v, err: %v", sanitizeConn(cfg.ConnString), err)
	}

	if err = rwc.Ping(); err != nil {
		return nil, fmt.Errorf("db: failed ping mysql db at %v, err: %v", sanitizeConn(cfg.ConnString), err)
	}

	dbo.rw = &sqlQuerier{rwc}
	dbo.t = &sqlQuerier{rwc}

	maxConn := int(math.Max(1, float64(cfg.MaxOpenConns)))
	maxConnLifetime := cfg.MaxConnLifetime

	rwc.SetMaxOpenConns(maxConn)
	rwc.SetMaxIdleConns(maxConn)

	if maxConnLifetime > 0 {
		rwc.SetConnMaxLifetime(maxConnLifetime)
	}

	dbo.dialect = &mysqlDialect{}
	dbo.queryLogger = cfg.QueryLogger

	return dbo, nil
}

func (c *mysqlConnector) DialectName(scheme string) (db.DialectName, error) {
	return db.MYSQL, nil
}
