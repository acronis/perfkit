package sql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/gocraft/dbr/v2"
	dbrdialect "github.com/gocraft/dbr/v2/dialect"

	"github.com/acronis/perfkit/db"
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
	queryLogger db.Logger
	exitOnError bool
	queries     []dbrQuery
}

// Event logs query into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *dbrEventReceiver) Event(eventName string) {
	if eventName == "dbr.begin" || eventName == "dbr.commit" {
		return
	}
	r.queryLogger.Log("DBREventReceiver::Event occured: %s", eventName)
}

// EventKv logs query and its key-value pairs into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *dbrEventReceiver) EventKv(eventName string, kvs map[string]string) {
	r.queryLogger.Log("DBREventReceiver::EventKv occured: %s: kvs: %v", eventName, kvs)
}

// EventErr logs query and its error into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *dbrEventReceiver) EventErr(eventName string, err error) error { //nolint:revive
	r.queryLogger.Log("DBREventReceiver::EventErr occured: %s", eventName)

	return nil
}

// Timing logs query and its duration into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *dbrEventReceiver) Timing(eventName string, nanoseconds int64) {
	r.queryLogger.Log("DBREventReceiver::Timing occured: %s: ns: %d", eventName, nanoseconds)
}

// EventErrKv logs query and its error into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *dbrEventReceiver) EventErrKv(eventName string, err error, kvs map[string]string) error {
	if err != nil {
		r.queryLogger.Log("eventName: %s: %s # %s", eventName, kvs["sql"], err)
	}

	return nil
}

// TimingKv adds query and its duration into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *dbrEventReceiver) TimingKv(eventName string, nanoseconds int64, kvs map[string]string) { //nolint:revive
	r.queries = append(r.queries, dbrQuery{query: kvs["sql"], duration: float64(nanoseconds) / 1000000000.0})
}

func dialectFromDbrScheme(scheme string, path string) (string, string, dialect, error) {
	const schemeSeparator = "+"
	parts := strings.Split(scheme, schemeSeparator)
	if len(parts) != 2 {
		return "", "", nil, fmt.Errorf("'%s' is invalid scheme separator", schemeSeparator)
	}

	switch parts[0] {
	case "sqlite":
		return "sqlite3", path, &sqliteDialect{}, nil
	case "mysql":
		return "mysql", path, &mysqlDialect{}, nil
	case "postgres":
		return "postgres", fmt.Sprintf("%s://%s", "postgres", path), &pgDialect{standardArgumentPlaceholder: true}, nil
	case "mssql":
		return "mssql", fmt.Sprintf("%s://%s", "sqlserver", path), &msDialect{standardArgumentPlaceholder: true}, nil
	default:
		return "", "", nil, fmt.Errorf("'%s' is unsupported dialect", parts[0])
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
	if driver, cs, dia, err = dialectFromDbrScheme(scheme, cs); err != nil {
		return nil, fmt.Errorf("db: cannot parse dbr db path, err: %v", err)
	}

	if dia.name() == db.POSTGRES {
		cs, dia, err = initializePostgresDB(cs, cfg.SystemLogger)
		if err != nil {
			return nil, err
		}
	}

	if rwc, err = dbr.Open(driver, cs, &dbrEventReceiver{queryLogger: cfg.QueryLogger, exitOnError: true, queries: []dbrQuery{}}); err != nil {
		return nil, fmt.Errorf("db: cannot connect to dbr sql db at %v, err: %v", sanitizeConn(cfg.ConnString), err)
	}

	if err = rwc.Ping(); err != nil {
		return nil, fmt.Errorf("db: failed ping dbr sql db at %v, err: %v", sanitizeConn(cfg.ConnString), err)
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
	dbo.qbs = newDBRQueryBuildersFactory(sess)
	dbo.useTruncate = cfg.UseTruncate
	dbo.queryStringInterpolation = cfg.QueryStringInterpolation
	dbo.dryRun = cfg.DryRun
	dbo.logTime = cfg.LogOperationsTime
	dbo.queryLogger = cfg.QueryLogger
	dbo.readRowsLogger = cfg.ReadRowsLogger
	dbo.explainLogger = cfg.ExplainLogger

	return dbo, nil
}

func (c *dbrConnector) DialectName(scheme string) (db.DialectName, error) {
	var driver, _, _, err = dialectFromDbrScheme(scheme, "")
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

type dbrQueryBuildersFactory struct {
	sess *dbr.Session // only for building queries
}

func newDBRQueryBuildersFactory(sess *dbr.Session) queryBuilderFactory {
	return &dbrQueryBuildersFactory{sess: sess}
}

func (queryBuildersFactory *dbrQueryBuildersFactory) newSelectQueryBuilder(tableName string, queryable map[string]filterFunction) selectQueryBuilder {
	return &dbrSelectBuilder{
		sess:      queryBuildersFactory.sess,
		tableName: tableName,
		queryable: queryable,
	}
}

func (queryBuildersFactory *dbrQueryBuildersFactory) newInsertQueryBuilder(tableName string) insertQueryBuilder {
	return &insertBuilder{
		tableName: tableName,
	}
}

type dbrSelectBuilder struct {
	sess      *dbr.Session              // only for building queries
	tableName string                    // Name of the table being queried
	queryable map[string]filterFunction // Maps column names to their filter functions
}

func dbrSqlConditions(sb *dbrSelectBuilder, stmt *dbr.SelectStmt, d dialect, c *db.SelectCtrl) (*dbr.SelectStmt, bool, error) {
	if len(c.Where) == 0 {
		return stmt, false, nil
	}

	for _, field := range db.SortFields(c.Where) {
		if field.Col == "" {
			return nil, false, fmt.Errorf("empty condition field")
		}

		condgen, ok := sb.queryable[field.Col]
		if !ok {
			return nil, false, fmt.Errorf("bad condition field '%v'", field.Col)
		}

		if len(field.Vals) == 1 {
			// Handle special cases
			if field.Vals[0] == db.SpecialConditionIsNull {
				stmt = stmt.Where(fmt.Sprintf("%v.%v IS NULL", sb.tableName, field.Col))
				continue
			}
			if field.Vals[0] == db.SpecialConditionIsNotNull {
				stmt = stmt.Where(fmt.Sprintf("%v.%v IS NOT NULL", sb.tableName, field.Col))
				continue
			}
		}

		var fieldName string
		fieldName = fmt.Sprintf("%v.%v", sb.tableName, field.Col)

		fmts, args, err := condgen(d, c.OptimizeConditions, fieldName, field.Vals)
		if err != nil {
			return nil, false, err
		}

		if fmts == nil {
			continue
		}

		if len(fmts) != len(args) {
			return nil, false, fmt.Errorf("number of args %d doesn't match number of conditions %d", len(args), len(fmts))
		}

		for i := range fmts {
			stmt = stmt.Where(sqlf(d, fmts[i], args[i]))
		}
	}

	return stmt, false, nil
}

func dbrSqlOrder(sb *dbrSelectBuilder, stmt *dbr.SelectStmt, d dialect, c *db.SelectCtrl) (*dbr.SelectStmt, error) {
	if len(c.Order) == 0 {
		return stmt, nil
	}

	for _, v := range c.Order {
		fnc, args, err := db.ParseFuncMultipleArgs(v, ";")
		if err != nil {
			return nil, err
		}

		if len(args) == 0 {
			return nil, fmt.Errorf("empty order field")
		}

		var dir string
		switch fnc {
		case "asc":
			dir = "ASC"
		case "desc":
			dir = "DESC"
		case "nearest":
			dir = "NEAREST"
		case "":
			return nil, fmt.Errorf("empty order function")
		default:
			return nil, fmt.Errorf("bad order function '%v'", fnc)
		}

		if dir == "ASC" || dir == "DESC" {
			if len(args) != 1 {
				return nil, fmt.Errorf("number of args %d doesn't match number of conditions 1", len(args))
			}
			stmt = stmt.OrderBy(fmt.Sprintf("%v.%v %v", sb.tableName, args[0], dir))
		} else if dir == "NEAREST" {
			if len(args) != 3 {
				return nil, fmt.Errorf("number of args %d doesn't match number of conditions for nearest function, should be 3", len(args))
			}
			orderStatement := d.encodeOrderByVector(args[0], args[1], args[2])
			stmt = stmt.OrderBy(orderStatement)
		}
	}

	return stmt, nil
}

func (sb *dbrSelectBuilder) sql(d dialect, c *db.SelectCtrl) (string, bool, error) {
	var stmt = sb.sess.Select(c.Fields...)

	if sb.tableName == "" {
		var buf = dbr.NewBuffer()

		// If no table name is provided, build a simple SELECT statement
		// Taken PostgreSQL dialect as an default
		if err := stmt.Build(dbrdialect.PostgreSQL, buf); err != nil {
			return "", false, fmt.Errorf("failed to build query: %w", err)
		}

		return buf.String(), false, nil
	}

	// Add FROM clause
	stmt = stmt.From(d.table(sb.tableName))

	// Add WHERE conditions
	var empty bool
	var err error
	stmt, empty, err = dbrSqlConditions(sb, stmt, d, c)
	if err != nil {
		return "", false, err
	}

	if empty {
		return "", true, nil
	}

	// Add ORDER BY
	stmt, err = dbrSqlOrder(sb, stmt, d, c)
	if err != nil {
		return "", false, err
	}

	// Add LIMIT and OFFSET
	if c.Page.Limit > 0 {
		stmt = stmt.Offset(uint64(c.Page.Offset)).Limit(uint64(c.Page.Limit))
	}

	// Convert dialect to dbr dialect
	var dbrDialect dbr.Dialect
	switch d.name() {
	case db.SQLITE:
		dbrDialect = dbrdialect.SQLite3
	case db.POSTGRES:
		dbrDialect = dbrdialect.PostgreSQL
	case db.MYSQL:
		dbrDialect = dbrdialect.MySQL
	case db.MSSQL:
		dbrDialect = dbrdialect.MSSQL
	default:
		dbrDialect = dbrdialect.PostgreSQL // Default to PostgreSQL if unknown
	}

	var buf = dbr.NewBuffer()

	if err := stmt.Build(dbrDialect, buf); err != nil {
		return "", false, fmt.Errorf("failed to build query: %w", err)
	}

	return buf.String(), false, nil
}

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
