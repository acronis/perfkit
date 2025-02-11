package sql

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"database/sql"
	"database/sql/driver"

	"github.com/google/uuid"
	"go.uber.org/atomic"

	"github.com/acronis/perfkit/db"
)

/*
 * DB connection management
 */

type querier interface {
	execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	prepareContext(ctx context.Context, query string) (sqlStatement, error)
}

type accessor interface {
	querier

	ping(ctx context.Context) error
	stats() sql.DBStats
	rawSession() interface{}
	close() error
}

type transaction interface {
	querier

	commit() error
	rollback() error
}

type transactor interface {
	begin(ctx context.Context) (transaction, error)
}

func inTx(ctx context.Context, t transactor, d dialect, fn func(q querier, d dialect) error) error {
	tx, err := t.begin(ctx)
	if err != nil {
		return err
	}

	if err = fn(tx, d); err != nil {
		if err != driver.ErrBadConn && d.canRollback(err) {
			if rErr := tx.rollback(); rErr != nil {
				if err == context.Canceled && (rErr == sql.ErrTxDone || rErr == context.Canceled) {
					return err
				} else {
					return fmt.Errorf("during rollback tx with error %v, error occurred %v", err, rErr)
				}
			}
		}
		return err
	}

	if err = tx.commit(); err == sql.ErrTxDone {
		select {
		case <-ctx.Done():
			// Context has been closed after end of executing and before commit.
			// After that, go db runtime call tx rollback in watcher goroutine.
			err = context.Canceled
		default:
		}
	}

	return err
}

type sqlGateway struct {
	ctx     context.Context
	rw      querier
	dialect dialect

	InsideTX                 bool
	MaxRetries               int
	QueryStringInterpolation bool

	dryRun bool

	queryLogger    db.Logger
	readRowsLogger db.Logger
}

type sqlSession struct {
	sqlGateway
	t transactor
}

func (s *sqlSession) Transact(fn func(tx db.DatabaseAccessor) error) error {
	var err error
	var maxRetries = s.MaxRetries
	if maxRetries == 0 {
		maxRetries = 10
	}

	for i := 0; i < maxRetries; i++ {
		err = inTx(s.ctx, s.t, s.dialect, func(q querier, dl dialect) error {
			gw := sqlGateway{
				s.ctx,
				q,
				dl,
				true,
				s.MaxRetries,
				s.QueryStringInterpolation,
				s.dryRun,
				s.queryLogger,
				s.readRowsLogger,
			}
			return fn(&gw) // bad but will work for now?
		})

		if !s.dialect.isRetriable(err) {
			break
		}
	}
	return err
}

// database is a wrapper for DB connection
type sqlDatabase struct {
	rw      accessor
	t       transactor
	dialect dialect

	useTruncate              bool
	queryStringInterpolation bool
	dryRun                   bool

	queryLogger     db.Logger
	readRowsLogger  db.Logger
	queryTimeLogger db.Logger

	lastQuery string
}

// Ping pings the DB
func (d *sqlDatabase) Ping(ctx context.Context) error {
	var err = d.rw.ping(ctx)
	if err != nil && d.queryLogger != nil {
		d.queryLogger.Log("ping failed: %v", err)
	}

	return err
}

func (d *sqlDatabase) DialectName() db.DialectName {
	return d.dialect.name()
}

func (d *sqlDatabase) UseTruncate() bool {
	return d.useTruncate
}

func (d *sqlDatabase) GetVersion() (db.DialectName, string, error) {
	return getVersion(d.rw, d.dialect)
}

func (d *sqlDatabase) GetInfo(version string) (ret []string, dbInfo *db.Info, err error) {
	return getInfo(d.rw, d.dialect, version)
}

func (d *sqlDatabase) ApplyMigrations(tableName, tableMigrationSQL string) error {
	return inTx(context.Background(), d.t, d.dialect, func(q querier, dia dialect) error {
		return applyMigrations(q, dia, tableName, tableMigrationSQL)
	})
}

func (d *sqlDatabase) TableExists(tableName string) (bool, error) {
	return tableExists(d.rw, d.dialect, tableName)
}

func (d *sqlDatabase) CreateTable(tableName string, tableDefinition *db.TableDefinition, tableMigrationDDL string) error {
	return inTx(context.Background(), d.t, d.dialect, func(q querier, dia dialect) error {
		return createTable(q, dia, tableName, tableDefinition, tableMigrationDDL)
	})
}

func (d *sqlDatabase) DropTable(name string) error {
	return inTx(context.Background(), d.t, d.dialect, func(q querier, dia dialect) error {
		return dropTable(q, dia, name, d.useTruncate)
	})
}

func (d *sqlDatabase) IndexExists(indexName string, tableName string) (bool, error) {
	return indexExists(d.rw, d.dialect, indexName, tableName)
}

func (d *sqlDatabase) CreateIndex(indexName string, tableName string, columns []string, indexType db.IndexType) error {
	return inTx(context.Background(), d.t, d.dialect, func(q querier, dia dialect) error {
		return createIndex(q, dia, indexName, tableName, columns, indexType)
	})
}

func (d *sqlDatabase) DropIndex(indexName string, tableName string) error {
	return inTx(context.Background(), d.t, d.dialect, func(q querier, dia dialect) error {
		return dropIndex(q, dia, indexName, tableName)
	})
}

func (d *sqlDatabase) ReadConstraints() ([]db.Constraint, error) {
	return readConstraints(d.rw, d.dialect)
}

func (d *sqlDatabase) AddConstraints(constraints []db.Constraint) error {
	return inTx(context.Background(), d.t, d.dialect, func(q querier, dia dialect) error {
		return addConstraints(q, dia, constraints)
	})
}

func (d *sqlDatabase) DropConstraints(constraints []db.Constraint) error {
	return inTx(context.Background(), d.t, d.dialect, func(q querier, dia dialect) error {
		return dropConstraints(q, dia, constraints)
	})
}

func (d *sqlDatabase) CreateSequence(sequenceName string) error {
	return inTx(context.Background(), d.t, d.dialect, func(q querier, dia dialect) error {
		return createSequence(q, dia, sequenceName)
	})
}

func (d *sqlDatabase) DropSequence(sequenceName string) error {
	return inTx(context.Background(), d.t, d.dialect, func(q querier, dia dialect) error {
		return dropSequence(q, dia, sequenceName)
	})
}

func (d *sqlDatabase) GetTablesSchemaInfo(tableNames []string) ([]string, error) {
	return getTablesSchemaInfo(d.rw, d.dialect, tableNames)
}

func (d *sqlDatabase) GetTablesVolumeInfo(tableNames []string) ([]string, error) {
	return getTablesVolumeInfo(d.rw, d.dialect, tableNames)
}

func accountTime(t *atomic.Int64, since time.Time) {
	t.Add(time.Since(since).Nanoseconds())
}

// wrappedQuerier is a wrapper for querier that implements following functionality:
// - measuring time of queries
// - logging of queries
// - dry-run mode
type wrappedQuerier struct {
	q querier

	prepareTime *atomic.Int64 // *time.Duration
	execTime    *atomic.Int64 // *time.Duration
	queryTime   *atomic.Int64 // *time.Duration
	deallocTime *atomic.Int64 // *time.Duration

	dryRun      bool
	queryLogger db.Logger
}

func (wq wrappedQuerier) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	defer accountTime(wq.execTime, time.Now())

	if wq.queryLogger != nil {
		if wq.dryRun {
			if !strings.Contains(query, "\n") {
				wq.queryLogger.Log(fmt.Sprintf("-- %s -- skip because of 'dry-run' mode", query))
			} else {
				wq.queryLogger.Log("-- skip because of 'dry-run' mode")
				formattedQuery := fmt.Sprintf("/*\n%s\n*/", query)
				wq.queryLogger.Log(formattedQuery)
			}
		} else {
			wq.queryLogger.Log(query)
		}
	}

	if wq.dryRun {
		return &sqlSurrogateResult{}, nil
	}

	return wq.q.execContext(ctx, query, args...)
}

func (wq wrappedQuerier) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	defer accountTime(wq.queryTime, time.Now())

	if wq.queryLogger != nil {
		wq.queryLogger.Log(query)
	}

	return wq.q.queryRowContext(ctx, query, args...)
}

func (wq wrappedQuerier) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	defer accountTime(wq.queryTime, time.Now())

	if wq.queryLogger != nil {
		wq.queryLogger.Log(query, args...)
	}

	return wq.q.queryContext(ctx, query, args...)
}

func (wq wrappedQuerier) prepareContext(ctx context.Context, query string) (sqlStatement, error) {
	defer accountTime(wq.prepareTime, time.Now())

	if wq.queryLogger != nil {
		wq.queryLogger.Log(fmt.Sprintf("PREPARE stmt FROM '%s';", query))
	}

	var stmt, err = wq.prepareContext(ctx, query)
	if err != nil {
		return stmt, err
	}

	return &wrappedStatement{
		stmt:        stmt,
		execTime:    wq.execTime,
		deallocTime: wq.deallocTime,
		dryRun:      wq.dryRun,
		queryLogger: wq.queryLogger,
	}, nil
}

// wrappedStatement is a wrapper for sqlStmt that adds additional features:
// - measuring time of queries
// - logging of queries
// - dry-run mode
type wrappedStatement struct {
	stmt sqlStatement

	execTime    *atomic.Int64 // *time.Duration
	deallocTime *atomic.Int64 // *time.Duration

	dryRun      bool
	queryLogger db.Logger
}

func (ws *wrappedStatement) Exec(args ...any) (db.Result, error) {
	defer accountTime(ws.execTime, time.Now())

	if ws.queryLogger != nil {
		if ws.dryRun {
			ws.queryLogger.Log("-- EXECUTE stmt -- skip because of 'dry-run' mode")
		} else {
			ws.queryLogger.Log("EXECUTE stmt;")
		}
	}

	if ws.dryRun {
		return &sqlSurrogateResult{}, nil
	}

	return ws.stmt.Exec(args...)
}

func (ws *wrappedStatement) Close() error {
	defer accountTime(ws.deallocTime, time.Now())

	if ws.queryLogger != nil {
		ws.queryLogger.Log("DEALLOCATE PREPARE stmt;")
	}

	return ws.stmt.Close()
}

// wrappedTransaction is a wrapper for transaction that implements following functionality:
// - measuring time of queries
// - logging of queries
// - dry-run mode
type wrappedTransaction struct {
	tx transaction

	prepareTime *atomic.Int64 // *time.Duration
	execTime    *atomic.Int64 // *time.Duration
	queryTime   *atomic.Int64 // *time.Duration
	deallocTime *atomic.Int64 // *time.Duration
	commitTime  *atomic.Int64 // *time.Duration

	dryRun         bool
	queryLogger    db.Logger
	txNotSupported bool
}

func (wtx wrappedTransaction) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	defer accountTime(wtx.execTime, time.Now())

	if wtx.queryLogger != nil {
		if wtx.dryRun {
			if !strings.Contains(query, "\n") {
				wtx.queryLogger.Log(fmt.Sprintf("-- %s -- skip because of 'dry-run' mode", query))
			} else {
				wtx.queryLogger.Log("-- skip because of 'dry-run' mode")
				formattedQuery := fmt.Sprintf("/*\n%s\n*/", query)
				wtx.queryLogger.Log(formattedQuery)
			}
		} else {
			wtx.queryLogger.Log(query)
		}
	}

	if wtx.dryRun {
		return &sqlSurrogateResult{}, nil
	}

	return wtx.tx.execContext(ctx, query, args...)
}

func (wtx wrappedTransaction) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	defer accountTime(wtx.queryTime, time.Now())

	if wtx.queryLogger != nil {
		wtx.queryLogger.Log(query)
	}

	return wtx.tx.queryRowContext(ctx, query, args...)
}

func (wtx wrappedTransaction) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	defer accountTime(wtx.queryTime, time.Now())

	if wtx.queryLogger != nil {
		wtx.queryLogger.Log(query)
	}

	return wtx.tx.queryContext(ctx, query, args...)
}

func (wtx wrappedTransaction) prepareContext(ctx context.Context, query string) (sqlStatement, error) {
	defer accountTime(wtx.prepareTime, time.Now())

	if wtx.queryLogger != nil {
		wtx.queryLogger.Log(fmt.Sprintf("PREPARE stmt FROM '%s';", query))
	}

	var stmt, err = wtx.tx.prepareContext(ctx, query)
	if err != nil {
		return stmt, err
	}

	return &wrappedStatement{
		stmt:        stmt,
		execTime:    wtx.execTime,
		deallocTime: wtx.deallocTime,
		dryRun:      wtx.dryRun,
		queryLogger: wtx.queryLogger,
	}, nil
}

func (wtx wrappedTransaction) commit() error {
	defer accountTime(wtx.commitTime, time.Now())

	if wtx.queryLogger != nil {
		if wtx.txNotSupported {
			wtx.queryLogger.Log("-- COMMIT -- skip because dialect does not support transactions")
		} else {
			wtx.queryLogger.Log("COMMIT")
		}
	}

	return wtx.tx.commit()
}

func (wtx wrappedTransaction) rollback() error {
	defer accountTime(wtx.commitTime, time.Now())

	if wtx.queryLogger != nil {
		if wtx.txNotSupported {
			wtx.queryLogger.Log("-- ROLLBACK -- skip because dialect does not support transactions")
		} else {
			wtx.queryLogger.Log("ROLLBACK")
		}
	}

	return wtx.tx.rollback()
}

// wrappedTransactor is a wrapper for transactor that implements following functionality:
// - measuring time of queries
// - logging of queries
// - dry-run mode
type wrappedTransactor struct {
	t transactor

	beginTime   *atomic.Int64 // *time.Duration
	prepareTime *atomic.Int64 // *time.Duration
	execTime    *atomic.Int64 // *time.Duration
	queryTime   *atomic.Int64 // *time.Duration
	deallocTime *atomic.Int64 // *time.Duration
	commitTime  *atomic.Int64 // *time.Duration

	dryRun bool

	queryLogger    db.Logger
	readRowsLogger db.Logger

	txNotSupported bool
}

func (wt wrappedTransactor) begin(ctx context.Context) (transaction, error) {
	defer accountTime(wt.beginTime, time.Now())

	if wt.queryLogger != nil {
		if wt.txNotSupported {
			wt.queryLogger.Log("-- BEGIN -- skip because dialect does not support transactions")
		} else {
			wt.queryLogger.Log("BEGIN")
		}
	}

	var t, err = wt.t.begin(ctx)

	if err != nil {
		return t, err
	}

	return wrappedTransaction{
		tx:             t,
		prepareTime:    wt.prepareTime,
		execTime:       wt.execTime,
		queryTime:      wt.queryTime,
		deallocTime:    wt.deallocTime,
		commitTime:     wt.commitTime,
		dryRun:         wt.dryRun,
		queryLogger:    wt.queryLogger,
		txNotSupported: wt.txNotSupported,
	}, nil
}

func (d *sqlDatabase) Context(ctx context.Context) *db.Context {
	return &db.Context{
		Ctx:         ctx,
		BeginTime:   atomic.NewInt64(0),
		PrepareTime: atomic.NewInt64(0),
		ExecTime:    atomic.NewInt64(0),
		QueryTime:   atomic.NewInt64(0),
		DeallocTime: atomic.NewInt64(0),
		CommitTime:  atomic.NewInt64(0),
	}
}

func (d *sqlDatabase) Session(c *db.Context) db.Session {
	return &sqlSession{
		sqlGateway: sqlGateway{
			ctx: c.Ctx,
			rw: wrappedQuerier{
				q:           d.rw,
				prepareTime: c.PrepareTime,
				execTime:    c.ExecTime,
				queryTime:   c.QueryTime,
				deallocTime: c.DeallocTime,
				dryRun:      d.dryRun,
				queryLogger: d.queryLogger,
			},
			dialect:                  d.dialect,
			InsideTX:                 false,
			QueryStringInterpolation: d.queryStringInterpolation,
			dryRun:                   d.dryRun,
			queryLogger:              d.queryLogger,
			readRowsLogger:           d.readRowsLogger,
		},
		t: wrappedTransactor{
			t:              d.t,
			beginTime:      c.BeginTime,
			prepareTime:    c.PrepareTime,
			execTime:       c.ExecTime,
			queryTime:      c.QueryTime,
			deallocTime:    c.DeallocTime,
			commitTime:     c.CommitTime,
			dryRun:         d.dryRun,
			queryLogger:    d.queryLogger,
			readRowsLogger: d.readRowsLogger,
			txNotSupported: !d.dialect.supportTransactions(),
		},
	}
}

func (d *sqlDatabase) RawSession() interface{} {
	if d.queryLogger != nil && d.rw != nil {
		stats := d.rw.stats()
		if stats.OpenConnections > 1 {
			d.queryLogger.Log("Potential connections leak detected, ensure the previous DB query closed the connection: %s", d.lastQuery)
		}
	}

	return d.rw.rawSession()
}

func (d *sqlDatabase) Stats() *db.Stats {
	sqlStats := d.rw.stats()
	return &db.Stats{OpenConnections: sqlStats.OpenConnections, Idle: sqlStats.Idle, InUse: sqlStats.InUse}
}

func (d *sqlDatabase) Close() error {
	var err = d.rw.close()
	if err != nil {
		return fmt.Errorf("close failed: %w", err)
	}

	return d.dialect.close()
}

type dialect interface {
	name() db.DialectName
	encodeString(s string) string
	encodeUUID(s uuid.UUID) string
	encodeVector(vs []float32) string
	encodeBool(b bool) string
	encodeBytes(bs []byte) string
	encodeTime(timestamp time.Time) string
	getType(dataType db.DataType) string
	randFunc() string
	supportTransactions() bool
	isRetriable(err error) bool
	canRollback(err error) bool
	table(table string) string
	schema() string
	recommendations() []db.Recommendation
	close() error
}

func sanitizeConn(cs string) string {
	sanitized := cs
	u, _ := url.Parse(cs)
	if u != nil && u.User != nil {
		u.User = nil
		sanitized = u.String()
	}
	return sanitized
}
