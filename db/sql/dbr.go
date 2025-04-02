package sql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/gocraft/dbr/v2"

	"github.com/acronis/perfkit/db"
	"github.com/acronis/perfkit/logger"
)

func init() {
	for _, supportedDialect := range []string{"sqlite", "postgres", "mysql", "mssql"} {
		if err := db.Register(fmt.Sprintf("%s+dbr", supportedDialect), &dbrConnector{}); err != nil {
			panic(err)
		}
	}
}

/*
 * SQL queries logging
 */

// DBRQuery is a struct for storing query and its duration
type dbrQuery struct {
	query    string
	duration float64
}

// DBREventReceiver is a wrapper for dbr.EventReceiver interface
type dbrEventReceiver struct {
	queryLogger logger.Logger
	exitOnError bool
	queries     []dbrQuery
}

// Event logs query into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *dbrEventReceiver) Event(eventName string) {
	if eventName == "dbr.begin" || eventName == "dbr.commit" {
		return
	}
	r.queryLogger.Trace("DBREventReceiver::Event occured: %s", eventName)
}

// EventKv logs query and its key-value pairs into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *dbrEventReceiver) EventKv(eventName string, kvs map[string]string) {
	r.queryLogger.Trace("DBREventReceiver::EventKv occured: %s: kvs: %v", eventName, kvs)
}

// EventErr logs query and its error into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *dbrEventReceiver) EventErr(eventName string, err error) error { //nolint:revive
	r.queryLogger.Error("DBREventReceiver::EventErr occured: %s", eventName)

	return nil
}

// Timing logs query and its duration into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *dbrEventReceiver) Timing(eventName string, nanoseconds int64) {
	r.queryLogger.Trace("DBREventReceiver::Timing occured: %s: ns: %d", eventName, nanoseconds)
}

// EventErrKv logs query and its error into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *dbrEventReceiver) EventErrKv(eventName string, err error, kvs map[string]string) error {
	if err != nil {
		r.queryLogger.Trace("eventName: %s: %s # %s", eventName, kvs["sql"], err)
	}

	return nil
}

// TimingKv adds query and its duration into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *dbrEventReceiver) TimingKv(eventName string, nanoseconds int64, kvs map[string]string) { //nolint:revive
	r.queries = append(r.queries, dbrQuery{query: kvs["sql"], duration: float64(nanoseconds) / 1000000000.0})
}

func dialectFromDbrScheme(scheme string) (string, dialect, error) {
	const schemeSeparator = "+"
	parts := strings.Split(scheme, schemeSeparator)
	if len(parts) != 2 {
		return "", nil, fmt.Errorf("'%s' is invalid scheme separator", schemeSeparator)
	}

	switch parts[0] {
	case "sqlite":
		return "sqlite", &sqliteDialect{}, nil
	case "mysql":
		return "mysql", &mysqlDialect{}, nil
	case "postgres":
		return "postgres", &pgDialect{}, nil
	case "mssql":
		return "mssql", &msDialect{}, nil
	default:
		return "", nil, fmt.Errorf("'%s' is unsupported dialect", parts[0])
	}
}

type dbrConnector struct{}

func (c *dbrConnector) ConnectionPool(cfg db.Config) (db.Database, error) {
	var scheme, cs, err = db.ParseScheme(cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("db: cannot parse dbr db path, err: %v", err)
	}

	dbo := &sqlDatabase{}
	var rwc *dbr.Connection

	var driver string
	var dia dialect
	if driver, dia, err = dialectFromDbrScheme(scheme); err != nil {
		return nil, fmt.Errorf("db: cannot parse dbr db path, err: %v", err)
	}

	if dia.name() == db.POSTGRES {
		cs, dia, err = initializePostgresDB(cfg.ConnString, cfg.SystemLogger)
		if err != nil {
			return nil, err
		}
	}

	if rwc, err = dbr.Open(driver, cs, &dbrEventReceiver{queryLogger: cfg.QueryLogger, exitOnError: true, queries: []dbrQuery{}}); err != nil {
		return nil, fmt.Errorf("db: cannot connect to mysql db at %v, err: %v", sanitizeConn(cfg.ConnString), err)
	}

	if err = rwc.Ping(); err != nil {
		return nil, fmt.Errorf("db: failed ping mysql db at %v, err: %v", sanitizeConn(cfg.ConnString), err)
	}

	var sess = rwc.NewSession(nil)

	dbo.rw = &dbrQuerier{sess}
	dbo.t = &dbrQuerier{sess}

	maxConn := int(math.Max(1, float64(cfg.MaxOpenConns)))
	maxConnLifetime := cfg.MaxConnLifetime

	rwc.SetMaxOpenConns(maxConn)
	rwc.SetMaxIdleConns(maxConn)

	if maxConnLifetime > 0 {
		rwc.SetConnMaxLifetime(maxConnLifetime)
	}

	dbo.dialect = dia
	dbo.queryLogger = cfg.QueryLogger

	return dbo, nil
}

func (c *dbrConnector) DialectName(scheme string) (db.DialectName, error) {
	var driver, _, err = dialectFromDbrScheme(scheme)
	if err != nil {
		return "", nil
	}

	switch driver {
	case "sqlite":
		return db.SQLITE, nil
	case "mysql":
		return db.MYSQL, nil
	case "postgres":
		return db.POSTGRES, nil
	case "mssql":
		return db.MSSQL, nil
	default:
		return "", nil
	}
}

/*
// DBRLogQuery logs all queries from DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (c *dbrConnector) FlushLogs(result interface{}) {
	er := &dbrEventReceiver{}

	for _, q := range er.queries {
		fmt.Sprintf("%s # dur: %.6f", q.query, q.duration)

			if result != nil && c.Logger.LogLevel >= LogTrace {
				ret += " # = "
				ret += db.DumpRecursive(result, "  ")
			}
			c.Log(c.logLevel, ret)
	}

	er.queries = []dbrQuery{}
}
*/

type dbrQuerier struct {
	be *dbr.Session
}
type dbrTransaction struct {
	be *dbr.Tx
}

func (d *dbrQuerier) ping(ctx context.Context) error {
	return d.be.PingContext(ctx)
}
func (d *dbrQuerier) stats() sql.DBStats {
	return d.be.Stats()
}
func (d *dbrQuerier) rawSession() interface{} {
	return d.be
}
func (d *dbrQuerier) close() error {
	return d.be.Close()
}
func (d *dbrQuerier) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return d.be.ExecContext(ctx, query, args...)
}
func (d *dbrQuerier) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return d.be.QueryRowContext(ctx, query, args...)
}
func (d *dbrQuerier) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return d.be.QueryContext(ctx, query, args...)
}
func (d *dbrQuerier) prepareContext(ctx context.Context, query string) (sqlStatement, error) {
	var stmt, err = d.be.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return &sqlStmt{stmt}, nil
}
func (d *dbrQuerier) begin(ctx context.Context) (transaction, error) {
	be, err := d.be.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &dbrTransaction{be}, nil
}

func (t *dbrTransaction) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.be.ExecContext(ctx, query, args...)
}
func (t *dbrTransaction) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.be.QueryRowContext(ctx, query, args...)
}
func (t *dbrTransaction) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.be.QueryContext(ctx, query, args...)
}
func (t *dbrTransaction) prepareContext(ctx context.Context, query string) (sqlStatement, error) {
	var stmt, err = t.be.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return &sqlStmt{stmt}, nil
}
func (t *dbrTransaction) commit() error {
	return t.be.Commit()
}
func (t *dbrTransaction) rollback() error {
	return t.be.Rollback()
}
