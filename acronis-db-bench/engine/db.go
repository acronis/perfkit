package engine

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/acronis/perfkit/db"
	"github.com/acronis/perfkit/logger"
)

// DatabaseOpts represents common flags for every test
type DatabaseOpts struct {
	// Driver and Dsn are deprecated, keep for backward compatibility, use ConnString instead
	Driver string `long:"driver" description:"(deprecated) db driver (postgres|mysql|sqlite3)" required:"false"`
	Dsn    string `long:"dsn" description:"(deprecated) dsn connection string" required:"false"`

	ConnString   string `long:"connection-string" description:"connection string (can also be set via ACRONIS_DB_BENCH_CONNECTION_STRING environment variable)" required:"false"`
	MaxOpenConns int    `long:"max-open-cons" description:"max open connections per worker" default:"2" required:"false"`
	Reconnect    bool   `long:"reconnect" description:"reconnect to DB before every test iteration" required:"false"`

	EnableQueryStringInterpolation bool `long:"enable-query-string-interpolation" description:"enable query string interpolation during insert queries construction" required:"false"`

	DryRun  bool `long:"dry-run" description:"do not execute any INSERT/UPDATE/DELETE queries on DB-side" required:"false"`
	Explain bool `long:"explain" description:"prepend the test queries by EXPLAIN ANALYZE" required:"false"`

	DontCleanup bool `long:"dont-cleanup" description:"do not cleanup DB content before/after the test in '-t all' mode" required:"false"`
	UseTruncate bool `long:"use-truncate" description:"use TRUNCATE instead of DROP TABLE in cleanup procedure" required:"false"`
}

// dbConnectorsPool is a simple connection pool, required not to saturate DB connection pool
type dbConnectorsPool struct {
	lock      sync.Mutex
	pool      map[string]*DBConnector
	isClosing bool // New field to track shutdown state
}

// key returns a unique key for the connection pool
func (p *dbConnectorsPool) key(connectionString string, workerID int) string {
	return fmt.Sprintf("%s-%d", connectionString, workerID)
}

// take returns a connection from the pool or nil if the pool is empty
func (p *dbConnectorsPool) take(dbOpts *DatabaseOpts, workerID int) *DBConnector {
	k := p.key(dbOpts.ConnString, workerID)

	p.lock.Lock()
	defer p.lock.Unlock()

	// Don't give out new connections if we're shutting down
	if p.isClosing {
		return nil
	}

	conn, exists := p.pool[k]
	if exists {
		delete(p.pool, k)
		conn.Logger.Trace("taking connection from the connection pool")
		return conn
	}

	return nil
}

// put puts a connection to the pool
func (p *dbConnectorsPool) put(conn *DBConnector) {
	if conn == nil {
		return
	}

	k := p.key(conn.DbOpts.ConnString, conn.WorkerID)

	p.lock.Lock()
	defer p.lock.Unlock()

	// If we're shutting down, close the connection instead of returning it to the pool
	if p.isClosing {
		if conn.Database != nil {
			conn.Database.Close()
			conn.Database = nil
		}
		return
	}

	// Check if there's already a connection in the pool
	_, exists := p.pool[k]
	if exists {
		// If there is, close the new connection instead of panicking
		if conn.Database != nil {
			conn.Database.Close()
			conn.Database = nil
		}
		conn.Logger.Warn("connection already exists in pool, closing new connection")
		return
	}

	p.pool[k] = conn
	conn.Logger.Trace("releasing connection to the connection pool")
}

// shutdown gracefully closes all connections in the pool
func (p *dbConnectorsPool) shutdown() {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.isClosing = true

	// Close all connections in the pool
	for k, conn := range p.pool {
		if conn.Database != nil {
			conn.Database.Close()
			conn.Database = nil
		}
		delete(p.pool, k)
	}
}

// newDBConnectorsPool creates a new connection pool
func newDBConnectorsPool() *dbConnectorsPool {
	p := dbConnectorsPool{}
	p.pool = make(map[string]*DBConnector)

	return &p
}

// connPool is a global connection pool
var connPool = newDBConnectorsPool()

// connectionsChecker checks for potential connections leak
func connectionsChecker(conn *DBConnector) {
	for {
		if conn.Database != nil {
			openConnections := 0

			conn.lock.Lock()
			if conn.Database != nil {
				stats := conn.Database.Stats()
				openConnections = stats.OpenConnections
			}
			conn.lock.Unlock()

			if openConnections > 1 {
				conn.Logger.Error("internal error: potential connections leak detected")
			}
		}
		time.Sleep(3 * time.Second)
	}
}

// DBWorkerData is a structure to store all the worker data
type DBWorkerData struct {
	workingConn  *DBConnector
	tenantsCache *DBConnector
}

func (d *DBWorkerData) release() {
	if d.workingConn != nil {
		d.workingConn.Release()
	}
	if d.tenantsCache != nil {
		d.tenantsCache.Release()
	}
}

/*
 * DB connection management
 */

// DBConnector is a wrapper for DB connection
type DBConnector struct {
	Logger        logger.Logger
	DbOpts        *DatabaseOpts
	RetryAttempts int
	WorkerID      int

	lock     sync.Mutex
	Database db.Database
}

// NewDBConnector creates a new DBConnector
func NewDBConnector(dbOpts *DatabaseOpts, workerID int, systemConnect bool, l logger.Logger, retryAttempts int) (*DBConnector, error) {
	c := connPool.take(dbOpts, workerID)
	if c != nil {
		return c, nil
	}

	var logOperationTime bool
	var queryLogger, readRowsLogger, explainLogger, systemLogger db.Logger

	if l.GetLevel() >= logger.LevelInfo {
		queryLogger = newDBLogger(l.Clone(), logger.LevelInfo)
		systemLogger = newDBLogger(l.Clone(), logger.LevelInfo)
	}

	if l.GetLevel() >= logger.LevelDebug {
		logOperationTime = true
	}

	if l.GetLevel() >= logger.LevelTrace {
		readRowsLogger = newDBLogger(l.Clone(), logger.LevelTrace)
	}

	if dbOpts.Explain {
		explainLogger = newDBLogger(l.Clone(), l.GetLevel())
	}

	var dbConn, err = db.Open(db.Config{
		ConnString:               dbOpts.ConnString,
		MaxOpenConns:             dbOpts.MaxOpenConns,
		QueryStringInterpolation: dbOpts.EnableQueryStringInterpolation,
		DryRun:                   dbOpts.DryRun,
		LogOperationsTime:        logOperationTime,
		UseTruncate:              dbOpts.UseTruncate,

		QueryLogger:    queryLogger,
		ReadRowsLogger: readRowsLogger,
		ExplainLogger:  explainLogger,
		SystemLogger:   systemLogger,
	})
	if err != nil {
		return nil, err
	}

	c = &DBConnector{
		Logger:        l,
		DbOpts:        dbOpts,
		RetryAttempts: retryAttempts,
		WorkerID:      workerID,
		Database:      dbConn,
	}

	// go connectionsChecker(c)

	return c, nil
}

// Release safely releases the connection back to the pool
func (c *DBConnector) Release() {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.Database != nil {
		connPool.put(c)
	}
}

// Close forcefully closes the connection
func (c *DBConnector) Close() {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.Database != nil {
		c.Database.Close()
		c.Database = nil
	}
}

// SetLogLevel sets log level
func (c *DBConnector) SetLogLevel(logLevel int) {
	c.Logger.SetLevel(logger.LogLevel(logLevel))
}

// Log logs a message
func (c *DBConnector) Log(LogLevel logger.LogLevel, format string, args ...interface{}) {
	c.Logger.Log(LogLevel, format, args...)
}

// Exit exits with an error message
func (c *DBConnector) Exit(fmts string, args ...interface{}) {
	fmt.Printf(fmts, args...)
	fmt.Println()
	os.Exit(127)
}

type dbLogger struct {
	l     logger.Logger
	level logger.LogLevel
}

func (l *dbLogger) Log(format string, args ...interface{}) {
	l.l.Log(l.level, format, args...)
}

func newDBLogger(l logger.Logger, level logger.LogLevel) *dbLogger {
	return &dbLogger{
		l:     l,
		level: level,
	}
}
