package sql

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"

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

// GetType returns MySQL-specific types
func (d *mysqlDialect) getType(id string) string {
	switch id {
	case "{$bigint_autoinc_pk}":
		return "BIGINT AUTO_INCREMENT PRIMARY KEY"
	case "{$bigint_autoinc}":
		return "BIGINT AUTO_INCREMENT"
	case "{$ascii}":
		return "character set ascii"
	case "{$uuid}":
		return "VARCHAR(36)"
	case "{$varchar_uuid}":
		return "VARCHAR(36)"
	case "{$tenant_uuid_bound_id}":
		return "VARCHAR(64)"
	case "{$longblob}":
		return "LONGBLOB"
	case "{$hugeblob}":
		return "LONGBLOB"
	case "{$datetime}":
		return "DATETIME"
	case "{$datetime6}":
		return "DATETIME(6)"
	case "{$timestamp6}":
		return "TIMESTAMP(6)"
	case "{$current_timestamp6}":
		return "CURRENT_TIMESTAMP(6)"
	case "{$binary20}":
		return "BINARY(20)"
	case "{$binaryblobtype}":
		return "MEDIUMBLOB"
	case "{$boolean}":
		return "BOOLEAN"
	case "{$boolean_false}":
		return "0"
	case "{$boolean_true}":
		return "1"
	case "{$tinyint}":
		return "TINYINT"
	case "{$longtext}":
		return "LONGTEXT"
	case "{$unique}":
		return "unique"
	case "{$notnull}":
		return "not null"
	case "{$null}":
		return "null"
	}

	if d.sqlEngine == "xpand-allnodes" {
		return "engine = xpand"
	} else {
		return "engine = " + d.sqlEngine
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
