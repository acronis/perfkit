package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"go.uber.org/atomic"
	"net/url"
	"time"

	"github.com/acronis/perfkit/db"
)

/*
 * DB connection management
 */

type querier interface {
	execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	prepareContext(ctx context.Context, query string) (*sql.Stmt, error)
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
	ctx        context.Context
	rw         querier
	dialect    dialect
	InsideTX   bool
	MaxRetries int

	queryLogger db.Logger
}

type esSession struct {
	sqlGateway
	t transactor
}

func (s *esSession) Transact(fn func(tx db.DatabaseAccessor) error) error {
	var err error
	var maxRetries = s.MaxRetries
	if maxRetries == 0 {
		maxRetries = 10
	}

	for i := 0; i < maxRetries; i++ {
		err = inTx(s.ctx, s.t, s.dialect, func(q querier, dl dialect) error {
			gw := sqlGateway{s.ctx, q, dl, true, s.MaxRetries, s.queryLogger}
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
	rw          accessor
	t           transactor
	dialect     dialect
	useTruncate bool

	queryLogger      db.Logger
	readedRowsLogger db.Logger
	queryTimeLogger  db.Logger

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

type timedQuerier struct {
	dbtime *atomic.Int64 // Do not move
	q      querier

	queryLogger db.Logger
}

func (tq timedQuerier) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	defer accountTime(tq.dbtime, time.Now())

	if tq.queryLogger != nil {
		tq.queryLogger.Log(query)
	}

	return tq.q.execContext(ctx, query, args...)
}

func (tq timedQuerier) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	defer accountTime(tq.dbtime, time.Now())

	if tq.queryLogger != nil {
		tq.queryLogger.Log(query)
	}

	return tq.q.queryRowContext(ctx, query, args...)
}

func (tq timedQuerier) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	defer accountTime(tq.dbtime, time.Now())

	if tq.queryLogger != nil {
		tq.queryLogger.Log(query, args...)
	}

	return tq.q.queryContext(ctx, query, args...)
}

func (tq timedQuerier) prepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	defer accountTime(tq.dbtime, time.Now())

	if tq.queryLogger != nil {
		tq.queryLogger.Log(query)
	}

	return tq.q.prepareContext(ctx, query)
}

type timedTransaction struct {
	dbtime     *atomic.Int64 // *time.Duration
	committime *atomic.Int64 // *time.Duration
	tx         transaction

	queryLogger db.Logger
}

func (ttx timedTransaction) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	defer accountTime(ttx.dbtime, time.Now())

	if ttx.queryLogger != nil {
		ttx.queryLogger.Log(query)
	}

	return ttx.tx.execContext(ctx, query, args...)
}

func (ttx timedTransaction) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	defer accountTime(ttx.dbtime, time.Now())

	if ttx.queryLogger != nil {
		ttx.queryLogger.Log(query)
	}

	return ttx.tx.queryRowContext(ctx, query, args...)
}

func (ttx timedTransaction) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	defer accountTime(ttx.dbtime, time.Now())

	if ttx.queryLogger != nil {
		ttx.queryLogger.Log(query)
	}

	return ttx.tx.queryContext(ctx, query, args...)
}

func (ttx timedTransaction) prepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	defer accountTime(ttx.dbtime, time.Now())

	if ttx.queryLogger != nil {
		ttx.queryLogger.Log(query)
	}

	return ttx.tx.prepareContext(ctx, query)
}

func (ttx timedTransaction) commit() error {
	defer accountTime(ttx.committime, time.Now())

	if ttx.queryLogger != nil {
		ttx.queryLogger.Log("COMMIT")
	}

	return ttx.tx.commit()
}

func (ttx timedTransaction) rollback() error {
	defer accountTime(ttx.committime, time.Now())

	if ttx.queryLogger != nil {
		ttx.queryLogger.Log("ROLLBACK")
	}

	return ttx.tx.rollback()
}

type timedTransactor struct {
	dbtime     *atomic.Int64
	begintime  *atomic.Int64
	committime *atomic.Int64
	t          transactor

	queryLogger db.Logger
}

func (tt timedTransactor) begin(ctx context.Context) (transaction, error) {
	defer accountTime(tt.begintime, time.Now())

	if tt.queryLogger != nil {
		tt.queryLogger.Log("BEGIN")
	}

	var t, err = tt.t.begin(ctx)

	if err != nil {
		return t, err
	}

	return timedTransaction{
		tx:          t,
		dbtime:      atomic.NewInt64(tt.dbtime.Load()),
		committime:  atomic.NewInt64(tt.committime.Load()),
		queryLogger: tt.queryLogger,
	}, nil
}

func (d *sqlDatabase) Context(ctx context.Context) *db.Context {
	return &db.Context{Ctx: ctx}
}

func (d *sqlDatabase) Session(c *db.Context) db.Session {
	return &esSession{
		sqlGateway: sqlGateway{
			ctx:         c.Ctx,
			rw:          timedQuerier{q: d.rw, dbtime: atomic.NewInt64(c.DBtime.Nanoseconds()), queryLogger: d.queryLogger},
			dialect:     d.dialect,
			InsideTX:    false,
			queryLogger: d.queryLogger,
		},
		t: timedTransactor{
			t:           d.t,
			begintime:   atomic.NewInt64(c.BeginTime.Nanoseconds()),
			dbtime:      atomic.NewInt64(c.DBtime.Nanoseconds()),
			committime:  atomic.NewInt64(c.CommitTime.Nanoseconds()),
			queryLogger: d.queryLogger,
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
	encodeBool(b bool) string
	encodeBytes(bs []byte) string
	getType(dataType db.DataType) string
	randFunc() string
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
