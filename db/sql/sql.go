package sql

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"database/sql"
	"database/sql/driver"

	"github.com/google/uuid"
	"go.uber.org/atomic"

	"github.com/acronis/perfkit/db"
	"github.com/acronis/perfkit/logger"
)

/*
 * DB connection management
 */

// querier defines the interface for database query operations
type querier interface {
	// execContext executes a query without returning any rows
	execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	// queryRowContext executes a query that returns a single row
	queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	// queryContext executes a query that returns rows
	queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	// prepareContext creates a prepared statement for later queries or executions
	prepareContext(ctx context.Context, query string) (sqlStatement, error)
}

// accessor extends querier with database connection management operations
type accessor interface {
	querier

	// ping verifies the database connection is still alive
	ping(ctx context.Context) error
	// stats returns database statistics
	stats() sql.DBStats
	// rawSession returns the underlying database session
	rawSession() interface{}
	// close closes the database connection
	close() error
}

// transaction represents a database transaction
type transaction interface {
	querier

	// commit commits the transaction
	commit() error
	// rollback aborts the transaction
	rollback() error
}

// transactor provides the ability to begin new transactions
type transactor interface {
	// begin starts a new transaction
	begin(ctx context.Context) (transaction, error)
}

// inTx executes a function within a transaction
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

// sqlGateway provides core database functionality
type sqlGateway struct {
	ctx     context.Context // Current context
	rw      querier         // Query executor
	dialect dialect         // SQL dialect being used

	InsideTX                 bool // Indicates if running within transaction
	MaxRetries               int  // Maximum number of retry attempts
	QueryStringInterpolation bool // Whether to interpolate query strings

	explain bool // Whether to explain queries

	readRowsLogger logger.Logger // Logger for read operations
	explainLogger  logger.Logger // Logger for query explanations
}

// sqlSession represents a database session
type sqlSession struct {
	sqlGateway
	t transactor // Transaction manager
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
				s.explain,
				s.readRowsLogger,
				s.explainLogger,
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

	queryLogger    logger.Logger
	readRowsLogger logger.Logger
	explainLogger  logger.Logger
}

// Ping pings the DB
func (d *sqlDatabase) Ping(ctx context.Context) error {
	var err = d.rw.ping(ctx)
	if err != nil && d.queryLogger != nil {
		d.queryLogger.Error("ping failed: %v", err)
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

func (d *sqlDatabase) Context(ctx context.Context, explain bool) *db.Context {
	return &db.Context{
		Ctx:         ctx,
		Explain:     explain,
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
			explain:                  c.Explain,
			readRowsLogger:           d.readRowsLogger,
			explainLogger:            d.explainLogger,
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
			txNotSupported: !d.dialect.supportTransactions(),
		},
	}
}

func (d *sqlDatabase) RawSession() interface{} {
	if d.queryLogger != nil && d.rw != nil {
		stats := d.rw.stats()
		if stats.OpenConnections > 1 {
			d.queryLogger.Warn("potential connections leak detected, ensure the previous DB query closed the connection")
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

// dialect defines the interface for SQL dialect-specific operations
type dialect interface {
	// name returns the dialect name
	name() db.DialectName
	// encodeString converts a string to dialect-specific format
	encodeString(s string) string
	// encodeUUID converts a UUID to dialect-specific format
	encodeUUID(s uuid.UUID) string
	// encodeVector converts a float32 slice to dialect-specific format
	encodeVector(vs []float32) string
	// encodeBool converts a boolean to dialect-specific format
	encodeBool(b bool) string
	// encodeBytes converts a byte slice to dialect-specific format
	encodeBytes(bs []byte) string
	// encodeTime converts a timestamp to dialect-specific format
	encodeTime(timestamp time.Time) string
	// getType returns the dialect-specific type for a given data type
	getType(dataType db.DataType) string
	// randFunc returns the dialect-specific random function
	randFunc() string
	// supportTransactions indicates if dialect supports transactions
	supportTransactions() bool
	// isRetriable determines if an error can be retried
	isRetriable(err error) bool
	// canRollback determines if transaction can be rolled back
	canRollback(err error) bool
	// table returns the dialect-specific table name
	table(table string) string
	// schema returns the dialect-specific schema
	schema() string
	// recommendations returns dialect-specific recommendations
	recommendations() []db.Recommendation
	// close cleans up any dialect-specific resources
	close() error
}

// sanitizeConn removes sensitive information from connection string
func sanitizeConn(cs string) string {
	sanitized := cs
	u, _ := url.Parse(cs)
	if u != nil && u.User != nil {
		u.User = nil
		sanitized = u.String()
	}
	return sanitized
}
